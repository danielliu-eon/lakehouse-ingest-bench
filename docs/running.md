# Running a benchmark

Phase 1 runs everything on one machine through Docker Compose. That is enough to
check a change end to end, not to measure one: the harness, the broker, the
object store and the engine share the machine's cores, and on an arm64 host the
engine image is emulated. Local figures say the pieces agree, never how fast.

## Prerequisites

- **Docker** with Compose v2 (`docker compose version`).
- **`jq`**, **`yq`** (mikefarah, v4) and **`curl`** on the host: the scripts read
  the run's facts, the spec's scoring keys and the engine's readiness with them.
- **16 GB RAM** available to Docker, and a few GB of its disk. The default smoke
  corpus is about 1.5 GB encoded and 0.9 GB stored, and it lives in the
  object-store container until teardown.
- Free ports: 8081 (engine REST), 9000 / 9001 (object store), 8181 (catalog), 9092 / 29092 (broker).

`uv sync` is only needed to run the tests and the tools outside a container;
the smoke builds its own image from the checkout.

## The smoke

```bash
scripts/smoke.sh                          # the full 300 s corpus, stock Flink
scripts/smoke.sh --set duration_s=30      # a 30 s corpus, for a quick check
scripts/smoke.sh --engine external        # stage and score; you start the engine
```

| Flag | Effect |
|---|---|
| `--engine flink\|external` | which spec under `runs/` to stage. `flink` also starts the engine; `external` prints the facts and waits |
| `--set KEY=VALUE` | override a corpus preset key, repeatable |
| `--keep` | skip teardown, so the table and the stack can be inspected |
| `--external-ready-file PATH` | with `--engine external`, wait for `PATH` to appear instead of reading a newline from stdin |

`EPOCH_LEAD_S`, `IDLE_STOP_S`, `EXTERNAL_READY_WAIT_S`, `FLINK_REST`,
`FLINK_SLOT_WAIT_S` and `FLINK_JOB_WAIT_S` override the waits.

It exits 0 only when the scorer published `run_valid: true`, printing the verdict
block first; on a failure it dumps the tail of the scorer's and the engine's logs
before teardown.

On repeat runs: teardown wipes the object store, so each run regenerates the
corpus, and with `--keep` a second run at a *different* `--set` leaves two
corpora of the same name that staging refuses to choose between. Run directories
live on the host under `runs/` and survive teardown either way.

Two things make `--set duration_s=30`'s verdict block read oddly. The shipped
specs exclude the first 60 seconds after the epoch from the freshness window,
because a fleet meeting its first rows is provisioning rather than lagging; a
30-second corpus is shorter than that, so the window collapses to the run's last
instant and reports one sample four times over — read `freshness.full` for its
whole lag curve. And `keepup.absorbed_at_offer_end` comes out near zero, because
with a ten-second checkpoint interval there is barely one commit inside a
thirty-second offer. It still checks that the table drains and that exactness is
clean; only the freshness bound and the keep-up fraction need a longer corpus.

## Generating a corpus

`gen-corpus --preset <name>` builds the corpus a run is scored against. The
generator holds a whole batch in memory while it encodes one, so its peak
resident memory is roughly ten times the batch's encoded bytes. A preset's
batch is `offered_bytes_per_s x batch_interval_ms / 1000`:

| Preset | Batch | Peak memory |
|---|---|---|
| `events-100mbs-{uniform,skew}` | 100 MB | about 1 GB |
| `events-600mbs-{uniform,skew}` | 600 MB | about 6 GB |

`--shard-index` / `--shard-count` split the batches across processes, which is
how a large corpus is generated in parallel. Every shard still builds whole
batches, so sharding buys throughput and not headroom: either 600 MB/s preset
needs about 6 GB free per process. A streaming batch writer that removes the
whole-batch buffer is a planned follow-up.

## The Kafka topic

Staging creates the run's topic with `kafka.partitions` partitions. Its
replication factor is not a site knob: staging reads the broker count out of
the cluster's metadata and asks for `min(3, brokers)`. Three replicas mean one
broker dying mid-run does not end it; a smaller cluster gets one replica per
broker, because a factor above the broker count is refused outright. The local
stack, one broker, therefore gets 1.

## Credentials

The harness implements no authentication.

**Kafka.** `site.kafka.security` is handed to every Kafka client — the admin
client that creates the topic, and each producer shard — verbatim, as librdkafka
properties; `produce --kafka-prop key=value` adds the same properties to one
shard, over the producer's own defaults. A cluster this repository has never
heard of is therefore reachable by configuration alone.

**Object storage and catalogs.** The cloud SDK's default credential chain: pod
identity or an instance role in a cluster, an ambient profile on a laptop. Static
keys are for the local stack, where they are its published defaults.

**Anything secret is a reference.** Write `${env:NAME}` in `site.kafka.security`,
`site.catalog.props`, a `--catalog-prop` or a `--kafka-prop`, and the variable is
read inside the process that uses it, at the call that needs it. Nothing resolves
at load, so the site config, the run's `facts.json`, `job.sql` and
`flink-conf.yaml` all keep the placeholder and no rendered file or run artifact
holds a secret. A managed engine substitutes its own container's environment as
it submits the job, which is where the variable has to be set. An unset one is
refused by name rather than substituted empty. A literal credential still works
and is still redacted out of `facts.json` by property name; a placeholder is
published as it stands, since it names the variable a reader has to set.

## Sizing the producer

`scripts/measure-producer.sh` times one producer shard sending a corpus (3 GB
encoded, ~11.7M rows) into the local broker with `--epoch` an hour in the past,
so every batch is already due and the wall clock measures the producer rather
than the corpus's pacing.

Median of three runs on an Apple M5 Pro (arm64) under OrbStack. Nothing here is
emulated, and the one local broker shares this machine's cores with the harness
— a per-process ceiling, not a cluster's:

| Metric | Median | Runs |
|---|---|---|
| MB/s (encoded) | 115.6 | 115.57, 120.20, 115.57 |
| rows/s | 449,182 | 449182, 467149, 449182 |

Both clear the 30 MB/s floor below which the spec's fallback, a compiled
producer, would be worth building. Size an offer's shard count from the median:

```
shards = ceil(offered_bytes_per_s / measured_bytes_per_s * 1.5)
```

Offering 500 MB/s needs `ceil(500 / 115.6 * 1.5) = 7` shards. Rerun the script
after a change to the producer or the encoder, and on the machine that will run
the offer — this figure is one laptop's.

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

**`run_valid`** is the only field that decides whether a result may be published.
It is true when all five hold: the table held the corpus's columns, it drained,
the freshness p95 stayed inside the spec's bound, exactness found no loss,
duplication or corruption, and the producer kept to its schedule. It is false for
a run still going — a partial run's lag is a lower bound, its exactness an upper.

**`state`** says how the run ended.

- `drained` — every offered batch arrived complete. The normal ending.
- `idle_stop` — the table stopped taking commits with rows still outstanding.
  The engine died, fell behind past the scorer's patience, or never consumed.
- `producer_bound` — see below.
- `void` — the table's columns are not the ones the corpus published, so nothing
  measured against it describes the corpus. `reason` names each column that is
  missing, of the wrong type, or optional where the corpus is required.

**`producer_bound`** means the offer, not the engine, set the rate: a batch was
acked more than `producer.behind_max_ms` after it was due, or a delivery errored.
Such a run says nothing about how fresh an engine kept the table. It usually
means the producer was starved of CPU by everything else on the machine; try a
smaller corpus or a lower `--speed`.

Beyond those: `prefix` against `last_batch` is how far the contiguous
completeness watermark got, `freshness.window` carries the p50/p95/p99/max lag
in seconds after the warmup, and `keepup.absorbed_at_offer_end` is the fraction
of the offer that had already landed when the last batch was acked.

A breached freshness bound with clean exactness and a `drained` state is not a
malfunction: it is the fleet being too small for the offer, which is what the
benchmark exists to detect. `engines/flink/README.md` carries two measured
points for the smoke corpus.

`gate --out runs/<run_id>/scores` answers `PASS`, `UNDERSIZED` or `VOID` from the
same artifacts while a run is still going, which a sweep uses to abandon an
undersized fleet early.
