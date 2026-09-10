# Methodology

What the benchmark asks of an engine, how each answer is defined, and why. Every
term the other documents use is defined here.

## Glossary

- **Batch** — one interval of the corpus, encoded once and frozen. A preset's
  `batch_interval_ms` is its width, and the corpus's manifest carries one record
  per batch: its row count, its row-id range and a checksum over those ids.
- **The offer** — the producer replaying the corpus on its original timeline.
  The **offered rate** is the corpus's `offered_bytes_per_s`, scaled by
  `producer.speed`.
- **Epoch** — the run's time origin, chosen when the producer starts. Batch *k*
  is due at `epoch + k * batch_interval_ms`, and every figure below is measured
  from it.
- **Contiguous prefix** — the largest *k* such that every batch `0..k` has all
  its rows in the table. This is the completeness watermark freshness is read
  off.
- **Window vs full** — the freshness window excludes `scoring.warmup_s` after
  the epoch; the full series covers the whole run. The bound is judged on the
  window.
- **PASS / UNDERSIZED / VOID** — the in-flight gate's three answers, so a sweep
  can abandon a fleet that will not pass without paying for its full duration.

## The corpus is the ground truth

A corpus is generated once and never regenerated during a run. Each batch is
judged on two figures the generator froze: its row count, and the sum of its row
ids modulo a prime. The count alone cannot tell a lost row from a row that
arrived twice; the two together cannot be satisfied by any wrong set of ids an
engine fault would produce. Every figure in the manifest is re-derived from the
stored bytes before the corpus is published, so the scorer trusts the manifest
without trusting the encoder that wrote it.

Row ids carry their batch in their high bits, so a commit's rows are attributed
to their batches with no join. The scorer reads only the id column out of each
data file: it is everything exactness needs, and a corpus row is mostly payload.

## Coverage and completeness

Two predicates come out of those two figures, and they answer different
questions.

- **Coverage** — have this batch's rows arrived, whatever else arrived with
  them? This is what freshness times.
- **Completeness** — did exactly this batch's rows arrive, and nothing else?
  This is what exactness judges.

Keeping them apart is what lets a late duplicate be a fault without also
rewriting when the rows before it became visible. Counts only grow, so the
prefix only advances, which is why it is carried forward from where the last
commit left it rather than rescanned.

## Freshness

Freshness is one number about a whole run, and the honest one is a quantile of
the lag a reader would have seen at an arbitrary instant. That is not a quantile
over commits: a fleet that commits ten times in one second and then stalls for
five minutes looks excellent per commit and terrible to a reader. So the lag is
sampled on a one-second grid, which weights every second of the run equally and
is what makes the p95 mean what the bound claims.

The lag at instant *t* is `t` minus the emit time of the newest batch whose rows
are **all** present — the contiguous prefix — so a batch that is half in the
table has not arrived. Before any batch is complete the lag is measured from the
epoch: an engine that has committed nothing has been late since the offer began.

The **window** starts `scoring.warmup_s` after the epoch, because a fleet that
has just been handed its first rows is provisioning rather than lagging, and the
bound is a claim about steady state. Both series are published: the full run says
how much lag the warmup hid, so a run whose window passes only because its warmup
swallowed a cold start is distinguishable from one that was fresh throughout. A
warmup longer than the run leaves the window empty, and the run's last grid point
stands in — which is why a corpus shorter than the warmup reports one sample four
times over.

**Draining is a separate condition, not a lag sample.** A run that ends with rows
still outside the table has no lag to measure for them, and quantiles over the
samples that do exist would score it as though those rows were never offered.

The freshness verdict is true when four things hold: the prefix reached the last
offered batch, the lag series has no gap, the window p95 is inside
`scoring.freshness_bound_s`, and the window max is inside twice it.

A gap is a **coverage failure**: a sampled prefix whose batch has no emit time in
the publish logs. One such sample voids the quantiles rather than being dropped,
because computing them over the rest would report a flattering number for a run
that cannot be scored.

### Two clocks, and clock sanity

Every lag is published on both time bases: the table's own commit timestamps —
what a reader of the table sees, and the default — and the wall time at which the
scorer first saw each commit, which is immune to a writer whose clock disagrees
with the producer's.

`min_lag_s` is the smallest per-commit lag on the table's clock. A batch cannot
be queryable before it was acknowledged in any single frame of reference, so a
negative minimum means those two clocks disagree and every figure drawn from the
table's timestamps is suspect. `clock_skew_suspected` is exactly that figure
being negative, so the two cannot contradict each other.

## Exactness

Only the batches the publish logs say were sent are judged. A replay over a
prefix of the corpus never offered the rest, and scoring them would report rows
nobody sent as rows the engine lost.

| Fault | What it is |
|---|---|
| `loss_rows` | rows of an offered batch that never arrived |
| `duplicate_rows` | rows that arrived more than once, also given as `duplicate_ppm` |
| `corrupt_batches` | the right row count with the wrong checksum |

Loss and duplication are counted separately because they are different faults
with different causes, and a run that loses a thousand rows and duplicates a
thousand others is not a run that got the answer right. `exact` is true only when
no batch disagrees at all. The violation list is capped: it exists to name the
first faults, and the counts above it are the complete figures.

## Keep-up

Freshness says how stale the table was; keep-up says whether the staleness was
bounded work or a growing debt. The two come apart at the end of a run: a fleet
that fell an hour behind and then drained still shows a small final lag, and only
the backlog it carried while the offer was running says it never kept pace.

| Figure | What it is |
|---|---|
| `absorbed_at_offer_end` | committed rows over offered rows at the instant the last batch was acknowledged. `1.0` is no standing debt |
| `drain_s` | seconds from that instant until the prefix reached the last batch |
| `backlog_rows_max`, `backlog_rows_p50` | the debt over the whole run |

The absorbed fraction is read at the instant the offer stopped and not at the end
of the run, because everything after that instant is drain: given long enough
every fleet absorbs the whole offer, and the fraction only distinguishes fleets
while rows are still arriving. Both sides are counted in rows rather than bytes
or offsets, so the backlog is the same quantity on either side of the
subtraction.

## Geometry

Geometry is what no verdict field covers: the live files, rows and bytes, the
file-size p50/p90/p99 with min and max, the share of files under 32 MiB and under
8 MiB, a log2 histogram of sizes, and per-commit quantiles of files added and of
their sizes. It is measured at each of `scoring.geometry_offsets_s` and at the
final snapshot; a rung the run never reached reads `absent`, so a run shorter
than the first offset has only a final point.

It is read from the table's own metadata document and its manifests, after the
fleet is gone — a read of metadata costs nothing to defer, and deferring it keeps
a manifest walk off the poll loop that is timing commits.

## The verdict

`run_valid` is the only field that decides whether a result may be published. It
is true when all of these hold:

1. the loop reached a verdict rather than abandoning the run;
2. the table held the corpus's columns, with their types and their
   required-ness;
3. the freshness verdict above — drained, no gap, p95 inside the bound, max
   inside twice it;
4. exactness found no loss, no duplication and no corruption;
5. the producer kept to its schedule.

It is false for a run still going: a partial run's lag is a lower bound and its
exactness an upper one. And false for one the loop abandoned even where the
figures beneath it are clean, since an idle stop with every batch landed reads
as exact and fresh.

`reason` names the clause that failed, and is null for a valid run and for one
still going. A void names every column the table got wrong, a run the loop
abandoned says `idle_stop_before_drain`, and a scorer that died says
`scorer_failed:` with its exception type. Any other invalid run takes the first
of three clauses to fail — `producer_bound:` before `exactness:` before
`freshness:`, since each makes the ones after it moot — and states its figures
after that prefix: `producer_bound: a batch was acknowledged 9250 ms after it
was due, over behind_max_ms 5000`; `exactness:` with whichever of `loss_rows`,
`duplicate_rows` and `corrupt_batches` are not zero; `freshness: window p95
79.40 s exceeds bound 60.00 s`, or its `window max`, `the table never drained`
and `missing_emit_prefixes=` variants.

`state` says how the run ended.

| `state` | What it means |
|---|---|
| `running` | the run is still going, and every figure beside it is provisional |
| `drained` | every offered batch arrived complete. The normal ending |
| `idle_stop` | the table stopped taking commits with rows still outstanding: the engine died, fell behind past the scorer's patience, or never consumed |
| `producer_bound` | the offer, not the engine, set the rate |
| `void` | the table's columns are not the ones the corpus published, so nothing measured against it describes the corpus. `reason` names each column that is missing, of the wrong type, or optional where the corpus is required |

`producer_bound` is true when a batch was acknowledged more than
`producer.behind_max_ms` after it was due, or a delivery errored. Such a run says
nothing about how fresh an engine kept the table. It usually means the producer
was starved of CPU by everything else on the machine, so offer less: a shorter
corpus with `--set duration_s=…`, a spec whose `producer.speed` is below 1, or
more `producer.shards` on a cluster. The first two are different dials — `--set`
reaches the corpus, and only the spec reaches the offer. A void outranks a bound
producer: the table is not the one the corpus describes, so no figure from either
side describes anything.

A breached freshness bound with clean exactness and a `drained` state is not a
malfunction. It is the fleet being too small for the offer, which is what the
benchmark exists to detect.

## The gate

The gate answers `PASS`, `UNDERSIZED` or `VOID` from the scorer's published
artifacts while a run is still going — as exit codes 0, 3 and 5. It reads those
artifacts rather than the table, because the scorer has already paid for that
read and two readers of one table would disagree about when a commit became
visible.

It reads three signals. The first is whether anything is still measuring: past
the staleness bound the newest keep-up sample is not a reading, whatever the
summary says. Then the lag right now, against twice the bound. The third is
whether the backlog's **floor** is climbing: backlog is sawtoothed,
filling between commits and emptying at each one, so its instantaneous value says
almost nothing, while the minimum over a window is the debt the fleet failed to
clear. A minimum that climbs window over window is a fleet falling behind however
busy each individual commit looked.

Nothing is judged undersized before the adaptation period is up, because a fleet
scaling out to meet its first rows is lagging for a reason that will pass.
UNDERSIZED and VOID are kept apart on purpose: the first is an answer about the
fleet, the second is the absence of an answer, and a sweep that conflated them
would report missing measurements as capacity limits.

## Cost

A result's cost is `Σ count × (vcpu × vcpu_hour_usd + gib × gib_hour_usd)` over
the fleet, times the run's hours. The fleet is what the run *asked for* —
container requests, not the nodes they landed on — so it is the same number
however the cluster packed it. The hours run from the epoch to the later of the
producer's last acknowledgement and the table's last commit: a fleet is not
released when the offer stops, and the drain is on the bill.

Both rates come from one rule, so that two sites' figures are comparable. Take
the hourly price of the instance type the fleet runs on and split it evenly
between the two terms: for an instance of `vcpu` cores and `gib` GiB at `P` an
hour, `vcpu_hour_usd = P / vcpu / 2` and `gib_hour_usd = P / gib / 2`. A 4 vCPU
/ 16 GiB instance at `P` therefore gives `P/8` and `P/32`, so a pod that fills
the node costs `4 × P/8 + 16 × P/32 = P`, the instance's own price, and a pod
asking for half the node costs half of it. Use the price the account actually
pays — on-demand or spot — and the same basis for every run compared.
