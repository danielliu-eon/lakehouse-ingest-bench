# Running a benchmark

Use the Docker Compose smoke to check changes end to end on one machine.
Publishable measurements require a cluster; see [On a cloud](#on-a-cloud).
See [`methodology.md`](methodology.md) for measurement definitions and
[`run-spec.md`](run-spec.md) for configuration keys.

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

For Flink, the smoke waits for slots, submits the job and runs `verify-flink`
before starting the producer. Local Spark uses `--master local[N]`, so the
driver runs the whole workload. The smoke waits for the named application and
one active streaming query; it cannot use the pod-based `verify-spark` check.
`executor_mem_mb` does not allocate executor memory in this mode.

`kafka.value_encoding: confluent` on a spec offers every value behind the
five-byte Confluent header and registers the corpus's schema at staging. The
local stack runs a registry, so
`scripts/smoke.sh --engine flink --spec runs/smoke-flink-confluent.yaml`
needs nothing extra; the framing itself is in
[`adding-an-engine.md`](adding-an-engine.md) §Confluent values.

Teardown clears object storage, so subsequent smokes regenerate the corpus.
With `--keep`, changing `--set` can leave multiple corpora with the same preset
name; staging rejects that ambiguity. Host run directories under `runs/` survive
teardown.

A 30-second corpus is useful for checking drain and exactness, but gives little
evidence about steady-state performance. The smoke specs exclude 60 seconds of
warmup; if the run ends before then, all window quantiles use the final sample.
Read `freshness.full` for the full lag curve. Startup and commit cadence can also
leave a low `keepup.absorbed_at_offer_end` in a short run.

`scoring.warmup_s` is 60 in smoke specs and 120 in hour-long specs. Choose a
warmup that covers expected startup while leaving enough time to measure steady
state; the full series still reports the excluded lag.

## CI

`ci.yml` runs on every pull request and on pushes to `main`: ruff, mypy, the
pytest suite and `scripts/validate-results.py`. It does not run the smoke —
`smoke.yml` does that, once per engine, on `workflow_dispatch` only.

## On a cloud

Measured runs place the engine, producer and scorer in separate pods. The AWS
setup uses EKS, Amazon MSK with IAM authentication, S3 and the Glue Iceberg REST
catalog. Complete the prerequisites and corpus setup in the
[AWS guide](../deploy/aws/README.md), then follow this sequence for each run.

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

To run or recover individual steps, use the run id printed by staging:

```bash
RUN_ID=$(scripts/stage.sh runs/aws-100mbs-skew-flink-hash.yaml | awk -F': ' '/^run_id: /{print $2}')
scripts/launch.sh "$RUN_ID"
scripts/gate.sh "$RUN_ID" --teardown       # every minute or so, while the run goes
scripts/teardown.sh "$RUN_ID"              # once the offer has drained
scripts/finish.sh "$RUN_ID" --publish results/   # geometry, the verdict, the result
scripts/purge.sh "$RUN_ID" --artifacts     # once you are done with the table
```

Use `runs/aws-100mbs-skew-flink-hash.yaml` and its Spark sibling as hour-long
starting specs. They share the corpus, Kafka settings and offer. Their fleet
sizes are starting points for capacity probes: increase capacity and rerun while
the gate reports `UNDERSIZED`, then publish a passing run.

Both managed engines use the same drivers. Engine modules supply resource and
status details; `verify-<engine>` checks the running configuration against the
copied spec before production starts.

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

Teardown releases compute before `finish.sh` reads the artifacts from storage.
The table and warehouse files remain available until you run `purge.sh`.

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

### In-cluster stack

The [in-cluster stack](../deploy/k8s/stack/README.md) uses the same run sequence.
For catalog hosts named `<service>.<namespace>.svc` (optionally followed by
`.cluster.local`), local drivers open a `kubectl port-forward` tunnel and rewrite
only the catalog URI. The catalog must use plain HTTP. `CATALOG_FORWARD_PORT`
(default `18181`) sets the local port; `CATALOG_FORWARD_PROBE` (default `/health`)
sets the readiness path. `teardown.sh`, `finish.sh` and `purge.sh` use this shared
property reader, including when reading copied metadata rather than the catalog.

### Which steps run in the cluster, and why

Six harness commands run as Jobs. `stage` and `drop-topic` need access to the
private MSK brokers. Producer shards and the scorer need sustained throughput
and frequent table reads. `gen-corpus` and `merge-corpus` write large corpora
using the pods' storage identity.

Your machine renders and applies manifests, waits for Jobs, fetches artifacts
and evaluates verdicts. The harness image needs no Kubernetes client. Install
`aws`, `kubectl`, `yq`, `jq`, `git`, `curl` and `gzip` locally; `push-images.sh`
also requires `docker`.

Install the harness itself with its `aws` extra — `uv sync --extra aws` in a
checkout, or `pip install '.[aws]'` — because three of the drivers reach the
catalog or the bucket through it: `teardown.sh` reads the table's last metadata
document, `finish.sh` walks its manifests for the geometry, and `purge.sh` drops
the table. pyiceberg imports `boto3` only when it comes to sign a Glue request,
so a default install reaches the catalog and then fails on that import.

### What the site declares about a cluster

**Identity.** `setup.sh` binds an IAM role to the three ServiceAccounts through
EKS Pod Identity. SDKs obtain credentials from the agent. Set
`site.kubernetes.aws_region`; pods receive both AWS region variables as explained
in [`pitfalls.md`](pitfalls.md). Omit this key for non-AWS clusters.

**Placement.** `site.kubernetes.node_selector` and `site.kubernetes.tolerations`
apply to every Job and engine pod. Flink overrides the architecture selector
with `kubernetes.io/arch: amd64` because its image lacks aarch64 PyFlink. Spark
uses the site selector unchanged.

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

`runs/<run_id>/scores/summary.json` contains the verdict. Scripts print its key
fields; see definitions of `run_valid`, `reason` and `state` in
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
