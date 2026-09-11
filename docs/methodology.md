# Methodology

This document defines the benchmark's measurements, validity rules and cost model.

## Glossary

- **Batch** — one frozen corpus interval, with width `batch_interval_ms`. Its
  manifest record gives the row count, row-id range and checksum.
- **The offer** — the producer replaying the corpus on its original timeline.
  The **offered rate** is `offered_bytes_per_s` scaled by `producer.speed`.
- **Epoch** — the run's time origin, chosen when the producer starts. At normal
  speed, batch *k* is due at `epoch_ms + k * batch_interval_ms`, in milliseconds.
- **Contiguous prefix** — the largest *k* for which every batch `0..k` is covered.
  This is the watermark used to measure freshness.
- **Window vs full** — the freshness window excludes `scoring.warmup_s` after
  the epoch; the full series covers the whole run. The verdict uses the window.
- **PASS / UNDERSIZED / VOID** — the in-flight gate's verdicts. They let a sweep
  stop a failing fleet before the full run ends.

## The corpus is the ground truth

A corpus is generated once and reused unchanged. The scorer compares each batch
against its frozen row count and sum of row ids modulo a prime. The checksum
helps detect substitutions that a count alone would miss; it is not a proof of
row-by-row identity. The generator verifies the manifest against the stored bytes
before publishing the corpus.

Row ids encode their batch in their high bits, so the scorer can attribute rows
without a join. It reads only the id column from each data file, avoiding the
payload that makes up most of each row.

## Coverage and completeness

The tally uses two checks:

- **Coverage** — the observed row count is at least the expected count. Freshness
  uses this check to advance the contiguous prefix.
- **Completeness** — both the row count and checksum match. Exactness uses this
  check to identify faulty batches.

A late duplicate can therefore fail exactness without changing an earlier
freshness observation. Counts only grow, so the prefix advances incrementally.
A duplicate can mask loss in the coverage count; exactness checks the checksum
as well as the count before a result can pass.

## Freshness

Freshness measures the lag a reader would see over time. The scorer samples lag
on a one-second grid so every second has equal weight. Sampling only at commits
would underweight stalls: ten commits in one second followed by five idle minutes
would produce mostly low-lag samples.

At instant *t*, lag is `t` minus the emit time of the batch at the contiguous
prefix. A partially covered batch does not advance that prefix. Before any batch
is covered, lag is measured from the epoch.

The **window** starts `scoring.warmup_s` after the epoch to exclude startup from
the steady-state bound. The full series remains available to show startup lag.
If warmup exceeds the run's duration, the window uses the final grid point, so
all four window quantiles report the same sample.

Draining is also required: passing lag quantiles alone does not establish that
all offered batches arrived. The freshness verdict is true only when:

1. the prefix reaches the last offered batch;
2. the lag series has no coverage gap;
3. window p95 is at most `scoring.freshness_bound_s`;
4. window max is at most twice that bound.

A **coverage gap** is a sampled prefix whose batch has no emit time in the publish
logs. Any gap invalidates the affected series' quantiles and fails freshness.
Dropping the sample would hide missing evidence.

### Two clocks, and clock sanity

Lag is published on two time bases: the table's commit timestamps (the default)
and the scorer's wall time when it first observed each commit. The second avoids
dependence on the writer's clock. All snapshots discovered in one metadata read
share an observation timestamp, captured before scanning their data files. The
scorer-clock series includes polling delay and still assumes the producer and
scorer clocks are aligned.

`min_lag_s` is the smallest per-commit lag on the table's clock. A negative value
means a commit timestamp precedes the corresponding producer acknowledgement and
sets `clock_skew_suspected` to true. Treat measurements from those timestamps as
suspect and inspect the series on the scorer's clock.

## Exactness

Only batches recorded as sent in the publish logs are scored. A replay of a
corpus prefix must not report unsent batches as lost.

| Fault | Definition |
|---|---|
| `loss_rows` | sum of row-count deficits across offered batches |
| `duplicate_rows` | sum of row-count excesses across offered batches; also reported as `duplicate_ppm` |
| `corrupt_batches` | batches with the expected row count but a different checksum |

Loss and duplication are counted separately across batches. Within one batch,
equal loss and duplication can leave the count unchanged; a checksum mismatch is
reported as corruption. `exact` is true only when every scored batch's count and
checksum match. The violation list shows the first faults up to a cap; the totals
cover all scored batches.

## Keep-up

Keep-up measures whether the engine absorbs the offer as it arrives. A fleet
that falls behind can eventually drain and finish with little lag; its backlog
during the offer reveals that it failed to keep pace.

| Figure | Definition |
|---|---|
| `absorbed_at_offer_end` | last committed row count sampled at or before the final acknowledgement, divided by the final acknowledged row total |
| `drain_s` | seconds from the final producer acknowledgement until the scorer first observed the prefix covering the last batch |
| `backlog_rows_max`, `backlog_rows_p50` | backlog over the whole run |

The absorbed fraction uses the final publish logs as its denominator, so delayed
log uploads cannot inflate it by understating the offer. Its numerator comes
from a completed poll at or before offer end; polling and scan delays can make
this a conservative estimate. It is null if there is no sample at or before
offer end or no final offered rows. Later drain does not change the numerator.

Both figures use producer acknowledgement times and scorer observation times,
so they assume those clocks are aligned. Drain includes polling delay but is
independent of writer-clock skew. Use the same `score --poll-interval-s` across
compared runs (default `5` seconds). Negative drain values are retained; inspect
producer/scorer clock alignment if they occur. Live backlog and rate samples still
reflect the publish logs available at each poll.

## Geometry

Geometry describes the live files, rows and bytes independently of the verdict:
file-size p50/p90/p99, min and max; the share under 32 MiB and under 8 MiB; a log2
size histogram; and per-commit quantiles of file counts and sizes. It is measured
at each `scoring.geometry_offsets_s` offset and at the final snapshot. An offset
the run did not reach is `absent`.

Geometry is read from the table's metadata document and manifests after teardown.
This keeps manifest scans out of the poll loop used to time commits.

## The verdict

`run_valid` determines whether the scorer considers the run valid. It is true
only when:

1. the scoring loop finishes normally;
2. the table has every corpus column with the required type and nullability;
3. freshness passes: drained, no gap, p95 within the bound, max within twice it;
4. exactness finds no loss, duplication or corruption;
5. the producer meets its schedule.

Running and abandoned runs are invalid even if their current measurements pass.
Publication also requires the [results checks](../results/README.md).

`reason` explains an invalid result. It is null for a valid or still-running run.
A schema violation names each mismatched column; an abandoned run reports
`idle_stop_before_drain`; a scorer exception reports `scorer_failed:` followed by
the exception type. Other failures use the first applicable category, in order:
`producer_bound:`, `exactness:`, then `freshness:`. The reason includes the
relevant values, for example:

- `producer_bound: a batch was acknowledged 9250 ms after it was due, over behind_max_ms 5000`;
- `exactness:` followed by nonzero `loss_rows`, `duplicate_rows` or `corrupt_batches`;
- `freshness: window p95 79.40 s exceeds bound 60.00 s`, or a reason naming
  `window max`, `the table never drained` or `missing_emit_prefixes=`.

`state` records the scoring loop's status:

| `state` | Meaning |
|---|---|
| `running` | measurements are provisional |
| `drained` | the prefix reached the last offered batch; exactness may still fail |
| `idle_stop` | commits stopped while rows remained outstanding: the engine stopped, never consumed, or exceeded the scorer's idle timeout |
| `producer_bound` | the producer failed to deliver the scheduled offer |
| `void` | table columns differ from the corpus schema; `reason` names missing columns, wrong types or optional corpus columns |

`producer_bound` is true when a batch acknowledgement exceeds
`producer.behind_max_ms` after its due time, or a delivery fails. The run cannot
establish engine capacity at the intended rate. Check producer CPU and shard
count. A shorter corpus (`--set duration_s=…`) can reduce a local probe's resource
needs; `producer.speed` below 1 lowers the replay rate. Corpus overrides and
producer settings affect different parts of the workload. A schema violation
(`void`) takes precedence over a producer failure.

A drained, exact run that exceeds the freshness bound indicates insufficient
capacity for the offer, rather than a correctness failure.

## The gate

The gate reads the scorer's published artifacts and returns `PASS`, `UNDERSIZED`
or `VOID` with exit codes 0, 3 and 5. Reusing those artifacts avoids a second table
reader with different commit-observation times.

It checks three signals:

1. **Measurement age.** A keep-up sample older than the staleness bound is invalid,
   regardless of the summary's verdict.
2. **Current lag.** Lag is compared against twice the freshness bound.
3. **Backlog floor.** The minimum backlog in successive windows shows work the
   fleet failed to clear. Instantaneous backlog naturally rises between commits
   and falls after them, so a rising floor is the more useful capacity signal.

Capacity checks wait until the adaptation period ends. `UNDERSIZED` means the
fleet failed those checks; `VOID` means the measurements cannot support a capacity
judgment.

## Cost

Resource-based cost applies only to Kubernetes runs. Local runs (sites without
`kubernetes`) report hourly and total dollar costs as `null`, displayed as `n/a`
in the results table. Run duration remains available when timestamps are recorded.

Hourly cost is `Σ count × (vcpu × vcpu_hour_usd + gib × gib_hour_usd)` across the
fleet. Multiply by run hours for total cost. Managed fleets use admitted container
requests captured in `engine-pods.json` after staging verifies the running fleet;
external engines use their declared fleet. Missing pod evidence leaves managed
cost unavailable. This accounts for Spark memory overhead and configuration
overrides, independent of how the cluster packs pods onto nodes. The snapshot
assumes fixed replica counts and resource requests throughout the run. Capture
supports ordinary containers; init containers and pod-level resource settings
require additional accounting and are rejected. Run hours extend from the epoch
to the later of the producer's last acknowledgement and the table's
last commit, including drain time.

Use one pricing rule across compared runs. Split the instance's hourly price `P`
equally between CPU and memory: `vcpu_hour_usd = P / vcpu / 2` and
`gib_hour_usd = P / gib / 2`. For a 4-vCPU, 16-GiB instance, the rates are `P/8`
and `P/32`. A pod requesting the whole node costs `P` per hour; one requesting
half of each resource costs `P/2`. Use the account's actual price basis, on-demand
or spot, consistently across runs.
