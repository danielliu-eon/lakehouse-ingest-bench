# Running a benchmark

The smoke runs everything on one machine through Docker Compose. That is
enough to check a change end to end, not to measure one: the harness, the
broker, the object store and the engine share the machine's cores, and on an
arm64 host the engine image is emulated. Local figures say the pieces agree,
never how fast. A measured run needs a cluster, which is "On a cloud" below.

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

## On a cloud

A measured run needs a cluster: the engine, the offer and the reader each get
their own pods, and the broker and the object store are managed services. The
AWS shape is Amazon MSK with IAM authentication, one S3 bucket, the Glue Iceberg
REST catalog and EKS. [`deploy/aws/README.md`](../deploy/aws/README.md) is what
that builds, what it costs and how to remove it; this section is the order the
drivers run in.

**The cluster is yours.** Neither deploy script creates, deletes or reconfigures
it, and it needs at least one **amd64** node: PyFlink publishes no aarch64 wheel
in any release, so the engine image is amd64-only, the preflight refuses a
cluster without such a node, and `push-images.sh` builds `linux/amd64` unless
told otherwise. If you have no cluster,
`deploy/aws/eksctl-cluster.example.yaml` makes a minimal one — see the last
section of that README.

### Once per account

```bash
export AWS_REGION=... CLUSTER_NAME=...
deploy/aws/setup.sh                        # bucket, ECR, MSK, IAM, namespace
cp site.aws.example.yaml site.yaml         # setup.sh prints every value to fill in
scripts/push-images.sh                     # both images, tagged with this commit
scripts/gen-corpus.sh events-100mbs-skew --shards 8
```

> **MSK bills by the hour whether or not a run is using it,** and reaching
> `ACTIVE` takes 15 to 30 minutes. Two brokers with 100 GiB each are a few
> dollars a day. Tear it down between campaigns with `deploy/aws/teardown.sh`
> and stand it up again with `setup.sh`; a corpus in the bucket outlives both.

### Once per run

```bash
RUN_ID=$(scripts/stage.sh runs/my-run.yaml | awk -F': ' '/^run_id: /{print $2}')
scripts/launch.sh "$RUN_ID"
scripts/gate.sh "$RUN_ID" --teardown       # every minute or so, while the run goes
scripts/teardown.sh "$RUN_ID"              # once the offer has drained
scripts/finish.sh "$RUN_ID"                # the verdict, read from the bucket
```

| Driver | What it does |
|---|---|
| `stage.sh <spec>` | runs `stage` as a Job, fetches the run directory it published, and for a managed engine applies the two documents it rendered and waits for the job to reach `RUNNING`. Prints `run_id: <id>` |
| `launch.sh <run_id>` | applies the scorer, waits for its first reading, then applies the producer shards. Records the run's epoch |
| `gate.sh <run_id>` | `PASS`, `UNDERSIZED` or `VOID` from the scorer's published artifacts, as exit code 0, 3 or 5. `--teardown` stops paying for a fleet that is not passing |
| `teardown.sh <run_id>` | deletes the engine, the producer and the scorer, drops the topic as a Job, and copies the table's last metadata document beside the run's artifacts |
| `finish.sh <run_id>` | fetches `scores/` and prints the verdict block. Exits 0 only on `run_valid: true` |

Teardown comes before `finish.sh` because the score is in the bucket either way,
and every minute a drained run's fleet stays up is a minute paid for nothing.
Neither `teardown.sh` nor anything else here deletes the table or the warehouse
data: a run's table is its result. Drop one by hand with `drop-table --table
<table>` when you are done with it.

### Which steps run in the cluster, and why

Two harness commands run as Jobs because they have to reach the broker:
`stage`, which creates the run's topic, and `drop-topic`, which removes it. MSK
brokers listen inside the VPC, and your laptop is not in it. The producer shards
and the scorer are Jobs for a different reason — the offer is hundreds of
megabytes a second into that same VPC, and the scorer reads the table on every
poll.

Everything else is your machine's: rendering manifests, applying them, waiting
on a Job, fetching artifacts, judging a verdict. So the harness image carries no
`kubectl` and no Kubernetes client, and the drivers need `kubectl`, `aws`, `jq`,
`yq` and `git` locally. Each driver takes `--site` (default `./site.yaml`) and
reads the cluster, the registry, the identities and the roots out of it.

### What the site declares

**Identity.** Nothing is passed to a pod. `setup.sh` binds one IAM role to both
ServiceAccounts through EKS Pod Identity, and every cloud SDK in every pod picks
its credentials up from the agent. The one value that must be stated is the
region: `site.kubernetes.aws_region` reaches every pod as `AWS_REGION`, which is
what an SDK reads when nothing else names one — both halves of an MSK IAM
connection need it, the token signer and S3 under the table's FileIO. A cluster
off AWS leaves the key out, and no pod is given the variable.

**Placement.** `site.kubernetes.node_selector` and
`site.kubernetes.tolerations` reach every Job and the engine's pods, and they
are the only place a node pool, label or taint of yours is named — nothing in
this repository knows about your cluster's shape. The engine's pods pin
`kubernetes.io/arch: amd64` over whatever the site selects, for the reason
above.

**Where files go.** `stage.sh` fetches the run directory into `./runs/<run_id>/`
beside your `site.yaml`, and `RUNS_DIR` moves that. The pods write to
`site.runs_root` in the bucket instead, because a pod's filesystem goes with the
pod: staging publishes its run directory, each producer shard its publish log,
and the scorer mirrors every artifact on each poll. That is also why `gate.sh`
and `finish.sh` read from the bucket rather than from anything still running.

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

The harness implements no authentication, bar the one exception named below.

**Kafka.** `site.kafka.security` reaches the admin client that creates the topic
verbatim, as librdkafka properties. A producer shard reads no site config, so a
driver carries the same properties to it with `produce --kafka-prop key=value`,
applied over the producer's own defaults. Nothing about SASL, mTLS or a
proprietary broker is implemented here; configuration is the whole interface.

The one exception is a mechanism no property can express: `sasl.mechanism:
OAUTHBEARER` against Amazon MSK wants a token signed from the caller's own
credentials, per connection, that expires within the hour. Declare the region
to sign in as `aws.region` alongside it — the harness's own key, stripped
before the properties reach a client — and install the harness with its `aws`
extra. A site that arranges its own tokens sets any `sasl.oauthbearer.*`
property instead, and its configuration is passed through untouched.

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
| `flinkdeployment.yaml` | stage | the engine as the Flink operator takes it, for a run on a cluster |
| `flink-job-configmap.yaml` | stage | `job.sql` and `flink-conf.yaml`, as the ConfigMap the engine's pods mount |
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

One recorded run of the smoke, with the two artifacts behind its verdict, is in
[`examples/smoke-flink/`](examples/smoke-flink/).
