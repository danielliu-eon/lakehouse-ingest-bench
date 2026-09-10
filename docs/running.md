# Running a benchmark

The smoke runs everything on one machine through Docker Compose, which is enough
to check a change end to end and not to measure one — see the closing note of
[`../README.md`](../README.md) for why. A measured run needs a cluster, which is
"On a cloud" below. Terms are defined in [`methodology.md`](methodology.md); the
keys of the two YAML files are in [`run-spec.md`](run-spec.md).

## Prerequisites

- **Docker** with Compose v2 (`docker compose version`).
- **`jq`**, **`yq`** (mikefarah, v4) and **`curl`** on the host: the scripts read
  the run's facts, the spec's scoring keys and the engine's readiness with them.
- **16 GB RAM** available to Docker, and a few GB of its disk. The default smoke
  corpus is about 1.5 GB encoded (300 s at 5 MB/s) and under a gigabyte stored —
  its payload is incompressible by construction — and it lives in the
  object-store container until teardown.
- Free ports: 9000 / 9001 (object store), 8181 (catalog), 9092 / 29092 (broker),
  and the engine's own — 8081 for Flink's REST, 4040 for the Spark driver's UI.
  Only one engine runs at a time.
- On an arm64 host, whatever your Docker runs amd64 images with: the Flink image
  is amd64-only.

`uv sync` is only needed to run the tests and the tools outside a container; the
smoke builds its own image from the checkout.

## The smoke

```bash
scripts/smoke.sh                          # the full 300 s corpus, stock Flink
scripts/smoke.sh --engine spark           # the same run on Spark
scripts/smoke.sh --set duration_s=30      # a 30 s corpus, for a quick check
scripts/smoke.sh --engine external        # stage and score; you start the engine
```

| Flag | Effect |
|---|---|
| `--engine flink\|spark\|external` | which spec under `runs/` to stage. `flink` and `spark` also start the engine; `external` prints the facts and waits |
| `--spec PATH` | a spec under `runs/` to stage instead of `runs/smoke-<engine>.yaml` |
| `--set KEY=VALUE` | override a corpus preset key, repeatable |
| `--keep` | skip teardown, so the table and the stack can be inspected |
| `--external-ready-file PATH` | with `--engine external`, wait for `PATH` to appear instead of reading a newline from stdin |

`EPOCH_LEAD_S`, `IDLE_STOP_S` and `EXTERNAL_READY_WAIT_S` override the waits;
`FLINK_REST`, `FLINK_SLOT_WAIT_S`, `FLINK_JOB_WAIT_S` and `SPARK_UI`,
`SPARK_APP_WAIT_S`, `SPARK_QUERY_WAIT_S` override where an engine is polled and
for how long.

It exits 0 only when the scorer published `run_valid: true`, printing the verdict
block first; on a failure it dumps the tail of the scorer's and the engine's logs
before teardown.

The two engines are not started the same way. Flink's fleet registers its slots,
the job is submitted, and then `verify-flink` holds the running job to the spec —
so a smoke that reaches the offer is one whose engine is running what the spec
asked for. Spark's driver *is* the fleet under `--master local[N]`, and
`verify-spark` reads pods, so the local Spark run waits for an application named
after the run and one active streaming query instead. `executor_mem_mb` is never
spent there — the driver's heap holds the whole fleet — which is the one thing to
know before reading a local Spark figure.

`kafka.value_encoding: confluent` on a spec offers every value behind the
five-byte Confluent header and registers the corpus's schema at staging. The
local stack runs a registry, so
`scripts/smoke.sh --engine flink --spec runs/smoke-flink-confluent.yaml`
needs nothing extra; the framing itself is in
[`adding-an-engine.md`](adding-an-engine.md) §Confluent values.

On repeat runs: teardown wipes the object store, so each run regenerates the
corpus, and with `--keep` a second run at a *different* `--set` leaves two
corpora of the same name that staging refuses to choose between. Run directories
live on the host under `runs/` and survive teardown either way.

Two things make `--set duration_s=30`'s verdict block read oddly. The shipped
specs exclude the first 60 seconds after the epoch from the freshness window, and
a 30-second corpus is shorter than that, so the window collapses to the run's
last instant and reports one sample four times over — read `freshness.full` for
its whole lag curve. And `keepup.absorbed_at_offer_end` comes out near zero,
because with a ten-second checkpoint interval there is barely one commit inside a
thirty-second offer. It still checks that the table drains and that exactness is
clean; only the freshness bound and the keep-up fraction need a longer corpus.

Those 60 seconds are the spec's own `scoring.warmup_s`, and the hour-long specs
set it to 120. An engine whose first commit lands after the warmup has ended
carries its cold start into the window, so raise it for one that starts slowly —
as far as leaving enough run behind it to measure allows.

## CI

`ci.yml` runs on every pull request and on pushes to `main`: ruff, mypy, the
pytest suite and `scripts/validate-results.py`. It does not run the smoke —
`smoke.yml` does that, once per engine, on `workflow_dispatch` only.

## On a cloud

A measured run needs a cluster: the engine, the offer and the reader each get
their own pods, and the broker and the object store are managed services. The
AWS shape is Amazon MSK with IAM authentication, one S3 bucket, the Glue Iceberg
REST catalog and EKS. [`../deploy/aws/README.md`](../deploy/aws/README.md) is
what an account needs, what `setup.sh` builds, what it costs, how to remove it,
and the once-per-account sequence that ends with a corpus in the bucket. This
section is the order the drivers run in, once per run.

```bash
scripts/run.sh runs/aws-100mbs-skew-flink-hash.yaml --publish results/
scripts/purge.sh <the run id it printed> --artifacts   # once you are done with the table
```

`run.sh` prints `run_id: <id>` as soon as staging returns and then runs the
sequence below: it judges the run every `--gate-interval-s` seconds (60) until
the scorer's `state` stops being `running`, tears it down, and exits with
`finish.sh`'s status — or 6, a code no other driver uses, when the teardown did
not converge and the fleet may still be running. `RUN_MAX_S` bounds how long it
waits from the launch — two hours, after which it tears the run down, prints
what the artifacts say and refuses.

It always gates with `--teardown`, so a fleet that has not passed for
`--breaches` ticks (3) is destroyed mid-run; `run.sh` then reads the verdict
instead of tearing the run down a second time. A launch that fails leaves the
fleet up on purpose — `launch.sh`'s refusals point at pod events a teardown
would delete — and the line it prints names `scripts/teardown.sh <run_id>`.

`--publish` and `--variant` are `finish.sh`'s; `--breaches` is `gate.sh`'s. An
`engine: external` spec waits after staging for `--external-ready-file <path>`
to appear, or for a newline on stdin.

The same run one driver at a time, which is what to reach for when a chained run
stops half way — each takes the run id `run.sh` printed:

```bash
RUN_ID=$(scripts/stage.sh runs/aws-100mbs-skew-flink-hash.yaml | awk -F': ' '/^run_id: /{print $2}')
scripts/launch.sh "$RUN_ID"
scripts/gate.sh "$RUN_ID" --teardown       # every minute or so, while the run goes
scripts/teardown.sh "$RUN_ID"              # once the offer has drained
scripts/finish.sh "$RUN_ID" --publish results/   # geometry, the verdict, the result
scripts/purge.sh "$RUN_ID" --artifacts     # once you are done with the table
```

`runs/aws-100mbs-skew-flink-hash.yaml` and its Spark sibling are the shipped
hour-long runs to copy: same corpus, same topic, same offer, so the two differ
only in the engine. Their fleets are where the probe ladder starts and each file
says so — raise the fleet and re-run until the gate stops reporting
`UNDERSIZED`, and publish the one that passed.

The sequence is the same for either managed engine — the drivers read the kind of
object a run is, where its state sits and which Service carries its API out of
the engine's own module, and run `verify-<engine>` against the copied spec before
the run is offered a corpus.

| Driver | What it does |
|---|---|
| `run.sh <spec>` | stages, launches, gates, tears down and finishes one run, in the order below. Prints `run_id: <id>`, then the verdict block. Exits with `finish.sh`'s status, or 6 when the teardown did not converge and the fleet may still be running |
| `stage.sh <spec>` | runs `stage` as a Job, fetches the run directory it published, and for either managed engine applies the two documents it rendered, waits for the engine to reach its running state *and for its fleet to be placed* — an operator reports running before every pod has an image to start from — holds it to the spec with `verify-<engine>` and records the image it is running. Prints `run_id: <id>` |
| `launch.sh <run_id>` | counts the nodes with 2 CPU free and warns when the scorer and the shards will not all fit — see [`pitfalls.md`](pitfalls.md) — applies the scorer, waits for its first reading, then applies the producer shards. Records the run's epoch |
| `gate.sh <run_id>` | `PASS`, `UNDERSIZED` or `VOID` from the scorer's published artifacts, as exit code 0, 3 or 5. `--teardown` stops paying for a fleet that is not passing, once the verdict has repeated — three ticks, or `--breaches N` |
| `teardown.sh <run_id>` | deletes the engine, the producer and the scorer, drops the topic as a Job, copies the table's last metadata document beside the run's artifacts, and collects the run |
| `finish.sh <run_id>` | measures the file geometry, collects the run again, prints the verdict block and the geometry line. `--publish <dir>` also writes the result. Exits 0 only on `run_valid: true` |
| `purge.sh <run_id>` | drops the table and removes its files, and with `--artifacts` the run's own prefix. Names everything first and asks; `--yes` answers |

Each driver takes `--site` (default `./site.yaml`) and reads the cluster, the
registry, the identities and the roots out of it. Each also takes its own waits
and pod sizes as environment variables, listed in its `--help`;
`SCORER_READ_WORKERS` on `launch.sh` is the one that changes what the scorer
does rather than how long a driver waits for it.

Teardown comes before `finish.sh` because the score is in the bucket either way,
and every minute a drained run's fleet stays up is a minute paid for nothing.
Neither `teardown.sh` nor anything else before `purge.sh` deletes the table or
the warehouse data: a run's table is its result, and reclaiming it is a separate
decision taken once the result has been read.

`purge.sh` is the only script that deletes measured data. It reads the table's
location out of the metadata document teardown copied rather than deriving it
from the table's name, refuses while the run's scorer is still in the namespace,
prints the table, the prefix and — with `--artifacts` — the run's prefix, and
then asks. Nothing is removed without `--yes` or a `y` at the prompt. A run that
left no such document — torn down by hand, or not torn down at all — has its
table looked up in the catalog instead, and a catalog that cannot be reached is
a refusal rather than an absent table.

### Publishing a result

`finish.sh <run_id> --publish results/` writes the run's document under
`results/<engine>/<date>-<engine>-<corpus>-<variant>.json` and re-renders
`results/RESULTS.md` from every document there. `--variant <name>` records the
tuning the run stands for; any engine-specific tuning beyond the run-spec knobs
is a separately named variant rather than a second version of one file.

A run whose `run_valid` is false is refused unless `--publish-invalid` says to
keep it labelled by its validity state. The rules a published result has to meet
are in [`../results/README.md`](../results/README.md), and the document's own
schema is in [`results-format.md`](results-format.md).

### Which steps run in the cluster, and why

Six harness commands run as Jobs, for three reasons. `stage` and `drop-topic`
have to reach the broker, and MSK brokers listen inside the VPC where your laptop
is not. The producer shards and the scorer are there because the offer is
hundreds of megabytes a second into that same VPC and the scorer reads the table
on every poll. `gen-corpus` and `merge-corpus` are there because a corpus is tens
to hundreds of gigabytes written into the bucket the pods already hold identity
for.

Everything else is your machine's: rendering manifests, applying them, waiting on
a Job, fetching artifacts, judging a verdict. So the harness image carries no
`kubectl` and no Kubernetes client, and the drivers need `aws`, `kubectl`, `yq`,
`jq`, `git`, `curl` and `gzip` locally (`push-images.sh` also needs `docker`).

Install the harness itself with its `aws` extra — `uv sync --extra aws` in a
checkout, or `pip install '.[aws]'` — because three of the drivers reach the
catalog or the bucket through it: `teardown.sh` reads the table's last metadata
document, `finish.sh` walks its manifests for the geometry, and `purge.sh` drops
the table. pyiceberg imports `boto3` only when it comes to sign a Glue request,
so a default install reaches the catalog and then fails on that import.

### What the site declares about a cluster

**Identity.** Nothing is passed to a pod. `setup.sh` binds one IAM role to all
three ServiceAccounts through EKS Pod Identity, and every cloud SDK in every pod
picks its credentials up from the agent. The one value that must be stated is the
region, as `site.kubernetes.aws_region`; every pod gets it under both names an
SDK reads it as, for the reason in [`pitfalls.md`](pitfalls.md). A cluster off
AWS leaves the key out, and no pod is given the variable.

**Placement.** `site.kubernetes.node_selector` and `site.kubernetes.tolerations`
reach every Job and every engine pod, and they are the only place a node pool,
label or taint of yours is named — nothing in this repository knows about your
cluster's shape. A Flink run's pods pin `kubernetes.io/arch: amd64` over whatever
the site selects, because the image has no aarch64 PyFlink to run — which is
why `deploy/aws/setup.sh` warns about a cluster with no amd64 node, and why a
Spark-only campaign needs none. A Spark run's pods take the selector as it
stands.

**Where files go.** `stage.sh` fetches the run directory into `./runs/<run_id>/`
beside your `site.yaml`, and `RUNS_DIR` moves that. The pods write to
`site.runs_root` in the bucket instead, because a pod's filesystem goes with the
pod: staging publishes its run directory, each producer shard its publish log,
and the scorer mirrors every artifact on each poll. That is also why `gate.sh`
and `finish.sh` read from the bucket rather than from anything still running.

## The run directory

Staging writes `runs/<run_id>/`, and everything downstream reads it:

| Path | Written by | What it is |
|---|---|---|
| `spec.yaml` | stage | the run spec, copied verbatim |
| `facts.json` | stage | what an engine needs to join the run — see [`adding-an-engine.md`](adding-an-engine.md) |
| `timeline.log` | stage | one line per phase transition |
| `job.sql`, `flink-conf.yaml`, `flink.env` | stage | a Flink run's script, the settings it is submitted with, and the cluster shape the local stack sizes containers from |
| `spark-defaults.conf` | stage | a Spark run's settings, and `job.json`, `reader-schema.avsc`, `job.env` beside it |
| `flinkdeployment.yaml` / `sparkapplication.yaml` | stage | the engine as its operator takes it, for a run on a cluster |
| `flink-job-configmap.yaml` / `spark-job-configmap.yaml` | stage | those rendered files, as the ConfigMap the engine's pods mount |
| `engine-image.json` | stage | the image the engine ran and the digest the node pulled |
| `publish_log-<i>.jsonl` | producer | one record per batch: rows, bytes, when it was due, when it was acked. Beside the spec locally, under `producer/` on a cluster |
| `scores/summary.json` | scorer | the verdict, rewritten on every poll |
| `scores/freshness.json` | scorer | the lag quantiles and the whole lag curve, on both clocks |
| `scores/exactness.json` | scorer | loss, duplication, corruption, and the first violations |
| `scores/keepup.json` | scorer | the keep-up scalars |
| `scores/geometry.json` | `file-sizes` | the file geometry along the run and at its end |
| `scores/snapshots.jsonl` | scorer | one line per commit the table took |
| `scores/keepup_samples.jsonl` | scorer | offered against committed, once per poll |
| `table-metadata.final.json` | teardown | the table's last metadata document, copied |
| `run.json` | `collect` | the whole result, redacted — see [`results-format.md`](results-format.md) |

The publish log is also uploaded to the object store as the run goes, because the
scorer reads the offered side from there rather than from the local disk.

## Reading the verdict

`runs/<run_id>/scores/summary.json` is the whole answer, and the scripts print
the part that matters. What every field means, when `run_valid` is true, what
`reason` names and what each `state` says are in
[`methodology.md`](methodology.md) §The verdict.

Two commands read the same artifacts on their own:

```bash
gate --out runs/<run_id>/scores                  # PASS / UNDERSIZED / VOID, mid-run
file-sizes --metadata runs/<run_id>/table-metadata.final.json \
           --epoch <facts.epoch> --out runs/<run_id>/scores
```

`finish.sh` runs the second and prints the file-size p50 and small-file share as
the last line of the verdict block. Run it before anything expires the run's
snapshots — see [`pitfalls.md`](pitfalls.md).

Three recorded runs, each with the artifacts behind its verdict, are under
[`examples/`](examples/): the local smoke on Flink, and the same smoke on AWS
against Flink and against Spark.
