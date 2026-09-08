# Running a benchmark

Phase 1 runs everything on one machine through Docker Compose. That is enough
to check a change end to end. It is not enough to measure one: the harness, the
broker, the object store and the engine share the machine's cores, and on an
arm64 host the engine image is emulated. Treat local figures as a signal that
the pieces agree, never as a result.

## Prerequisites

- **Docker** with Compose v2 (`docker compose version`).
- **`jq`**, **`yq`** (mikefarah, v4) and **`curl`** on the host. The scripts
  read the run's facts with `jq`, the spec's scoring keys with `yq`, and the
  engine's readiness with `curl`.
- **16 GB RAM** available to Docker, and a few GB of its disk. The default
  smoke corpus is about 1.5 GB encoded and 0.9 GB stored, and it lives inside
  the object-store container until the stack is torn down.
- Free ports: 8081 (engine REST), 9000 / 9001 (object store), 8181 (catalog),
  9092 / 29092 (broker).

`uv sync` is only needed to run the tests and the tools outside a container;
the smoke builds its own image from the checkout.

## The smoke

```bash
scripts/smoke.sh                          # the full 300 s corpus, stock Flink
scripts/smoke.sh --set duration_s=30      # a 30 s corpus, for a quick check
scripts/smoke.sh --engine external        # stage and score; you start the engine
scripts/smoke.sh --keep                   # leave the stack up afterwards
```

| Flag | Effect |
|---|---|
| `--engine flink\|external` | which spec under `runs/` to stage. `flink` also starts the engine; `external` prints the facts and waits |
| `--set KEY=VALUE` | override a corpus preset key, repeatable |
| `--keep` | skip teardown, so the table and the stack can be inspected |
| `--external-ready-file PATH` | with `--engine external`, wait for `PATH` to appear instead of reading a newline from stdin |

`EPOCH_LEAD_S`, `IDLE_STOP_S`, `EXTERNAL_READY_WAIT_S`, `FLINK_REST`,
`FLINK_SLOT_WAIT_S` and `FLINK_JOB_WAIT_S` override the waits.

It exits 0 only when the scorer published `run_valid: true`, and prints the
verdict block before it does. On a failure it dumps the last lines of the
scorer's and the engine's logs before tearing the stack down.

Two things to know about repeat runs. Teardown wipes the object store, so each
run regenerates the corpus; with `--keep`, a second run at a *different*
`--set` leaves two corpora of the same name and staging refuses rather than
guess which one to score. Run directories live on the host under `runs/` and
survive teardown either way.

Two things to know about `--set duration_s=30`, both of which make its verdict
block read oddly. The shipped specs exclude the first 60 seconds after the epoch
from the freshness window, because a fleet meeting its first rows is
provisioning rather than lagging; a 30-second corpus is shorter than that, so
the window collapses to the run's last instant and `freshness.window` reports
one sample four times over. Read `freshness.full` instead for a short run's
whole lag curve. And `keepup.absorbed_at_offer_end` comes out near zero, because
the engine's first commit lands after the last batch was acked — with a
ten-second checkpoint interval there is barely one commit inside a
thirty-second offer. The short run is still a real check: the table has to
drain and exactness has to be clean. It is the freshness bound and the keep-up
fraction that only a corpus longer than the warmup exercises.

## The run directory

Staging writes `runs/<run_id>/`, and everything downstream reads it:

| Path | Written by | What it is |
|---|---|---|
| `spec.yaml` | stage | the run spec, copied verbatim |
| `facts.json` | stage | what an engine needs to join the run — see `docs/adding-an-engine.md` |
| `timeline.log` | stage | one line per phase transition |
| `job.sql` | stage | the engine's script, for a managed engine |
| `flink-conf.yaml` | stage | the settings the script is submitted with |
| `flink.env` | stage | the cluster's shape, which the stack sizes containers from |
| `publish_log-0.jsonl` | producer | one record per batch: rows, bytes, when it was due, when it was acked |
| `scores/summary.json` | scorer | the verdict, rewritten on every poll |
| `scores/freshness.json` | scorer | the lag quantiles and the whole lag curve, on both clocks |
| `scores/exactness.json` | scorer | loss, duplication, corruption, and the first violations |
| `scores/keepup.json` | scorer | the keep-up scalars |
| `scores/snapshots.jsonl` | scorer | one line per commit the table took |
| `scores/keepup_samples.jsonl` | scorer | offered against committed, once per poll |

The publish log is also uploaded to the object store as the run goes, because
the scorer reads the offered side from there rather than from the local disk.

## Reading the verdict

**`run_valid`** is the only field that decides whether a result may be
published. It is true when all four hold: the table drained, the freshness p95
stayed inside the spec's bound, exactness found no loss, duplication or
corruption in any offered batch, and the producer kept to its schedule. It is
false for a run still going — a partial run's lag is a lower bound and its
exactness an upper one.

**`state`** says how the run ended.

- `drained` — every offered batch arrived complete. The normal ending.
- `idle_stop` — the table stopped taking commits with rows still outstanding.
  The engine died, fell behind past the scorer's patience, or never consumed.
- `producer_bound` — see below.

**`producer_bound`** means the offer, not the engine, set the rate: some batch
was acked more than `producer.behind_max_ms` after it was due, or a delivery
errored. Such a run says nothing about how fresh an engine kept the table, so
it is void rather than reported as engine lag. It usually means the producer
was starved of CPU by everything else on the machine — the first thing to try
is a smaller corpus or a lower `--speed`.

Beyond those: `prefix` against `last_batch` is how far the contiguous
completeness watermark got, `freshness.window` carries the p50/p95/p99/max lag
in seconds after the warmup, and `keepup.absorbed_at_offer_end` is the fraction
of the offer that had already landed when the last batch was acked.

A breached freshness bound with clean exactness and a `drained` state is not a
malfunction: it is the fleet being too small for the offer, which is the thing
the benchmark exists to detect. `engines/flink/README.md` carries the two
measured points for the smoke corpus.

`gate --out runs/<run_id>/scores` answers `PASS`, `UNDERSIZED` or `VOID` from
the same artifacts while a run is still going, which is what a sweep uses to
abandon an undersized fleet early.
