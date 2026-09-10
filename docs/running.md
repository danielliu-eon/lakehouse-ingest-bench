# Running a benchmark

Use the Docker Compose smoke test to check changes end to end on one machine.
Publishable measurements require a cluster; see [On a cloud](#on-a-cloud).
See [`methodology.md`](methodology.md) for measurement definitions and
[`run-spec.md`](run-spec.md) for configuration keys.

## Prerequisites

- **Docker** with Compose v2 (`docker compose version`).
- **`jq`**, **`yq`** (mikefarah, v4) and **`curl`** on the host: the scripts read
  the run's facts, the spec's scoring keys and the engine's readiness with them.
- **16 GB RAM** available to Docker, and several GB of free disk space. The default
  smoke corpus is about 1.5 GB encoded (300 s at 5 MB/s) and under a gigabyte stored —
  its payload is incompressible by construction — and it lives in the
  object-store container until teardown.
- Free ports: 9000 / 9001 (object store), 8181 (catalog), 9092 / 29092 (broker),
  and the engine's own — 8081 for Flink's REST, 4040 for the Spark driver's UI.
  Only one engine runs at a time.

`uv sync` is only needed to run the tests and the tools outside a container; the
smoke builds its own image from the checkout.

## The smoke

Run these commands from the repository root:

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

After scoring, the script measures the live table's file geometry and prints it
with the verdict. It exits 0 only if the scorer reports `run_valid: true`. On
failure, it prints the end of the scorer and engine logs before teardown.

For Flink, the smoke waits for slots, submits the job and runs `verify-flink`
before starting the producer. Local Spark uses `--master local[N]`, so the
driver runs the whole workload. The smoke waits for the named application and
one active streaming query; it cannot use the pod-based `verify-spark` check.
`executor_mem_mb` does not allocate executor memory in this mode.

Setting `kafka.value_encoding: confluent` prefixes each value with the five-byte
Confluent header and registers the corpus schema during staging. The
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

Measured runs place the engine, producer and scorer in separate pods. AWS runs
use EKS and S3 with either Amazon MSK and Glue or in-cluster Kafka and
Lakekeeper. Complete the prerequisites and corpus setup in the
[AWS guide](../deploy/aws/README.md), then follow this sequence for each run.

```bash
scripts/run.sh runs/aws-100mbs-skew-flink-hash.yaml --publish results/
scripts/purge.sh <the run id it printed> --artifacts   # once you are done with the table
```

`run.sh` prints `run_id: <id>` after staging. It checks the gate every
`--gate-interval-s` seconds (default 60) until the scorer leaves the `running`
state or the gate triggers teardown. It then tears down any remaining fleet and
runs `finish.sh`.

Before the scheduled epoch, a healthy gate check reports `PASS` with the time
remaining rather than a negative lag. A stale scorer sample still reports
`VOID` during this wait.

The script normally returns `finish.sh`'s exit status. It returns 6 if teardown
fails and the fleet may still be running. `RUN_MAX_S` limits the wait after
launch to two hours by default; on timeout, the script tears down the run,
collects its verdict and exits with an error.

The gate always uses `--teardown`. After `--breaches` consecutive non-passing
checks (default 3), it tears down the fleet. A launch failure leaves the fleet
running so you can inspect pod events; the error output includes the command
to tear it down afterward.

`--publish` and `--variant` are passed to `finish.sh`; `--breaches` configures
`gate.sh`. For `engine: external`, the driver waits after staging for the file specified by
`--external-ready-file <path>` to appear, or for a newline on stdin.

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
the gate reports `UNDERSIZED`, then publish a passing run. Before staging, replace
`machine_type: YOUR_MACHINE_TYPE` with the actual node instance type and fill in
the site pricing. Smoke specs leave `machine_type` optional; `finish.sh --publish`
rejects missing or placeholder machine types. The knob records the fleet; use
site placement settings to select nodes.

Both managed engines use the same drivers. Engine modules supply resource and
status details; `verify-<engine>` checks the running configuration against the
copied spec before production starts.

| Driver | What it does |
|---|---|
| `run.sh <spec>` | Runs the sequence below, excluding purge. Prints the run ID and verdict; returns the finish status, or 6 if teardown fails. |
| `stage.sh <spec>` | Stages the run as a Job and downloads its artifacts. For managed engines, applies the rendered manifests, waits for the fleet, verifies the running configuration and records the image. Prints the run ID. |
| `launch.sh <run_id>` | Checks available node capacity, starts the scorer and waits for its first reading, then starts producer shards. Records the run epoch. See [placement pitfalls](pitfalls.md). |
| `gate.sh <run_id>` | Reports `PASS`, `UNDERSIZED` or `VOID` from scorer artifacts, with exit codes 0, 3 or 5. With `--teardown`, stops the fleet after three consecutive non-passing checks, configurable with `--breaches N`. |
| `teardown.sh <run_id>` | Deletes engine, producer and scorer resources; drops the topic; copies the final table metadata; and collects run artifacts. |
| `finish.sh <run_id>` | Measures file geometry, collects artifacts and prints the verdict and geometry. `--publish <dir>` also writes a result. Exits 0 only for `run_valid: true`. |
| `purge.sh <run_id>` | Drops the table and deletes its files. `--artifacts` also deletes the run's storage prefix. Lists deletion targets and asks for confirmation; `--yes` skips the prompt. |

Each driver accepts `--site` (default `./site.yaml`) for cluster, registry,
identity and storage settings. Its `--help` lists environment variables for
wait limits and pod sizes. `SCORER_READ_WORKERS` on `launch.sh` controls scorer
read concurrency.

Teardown releases compute before `finish.sh` reads the stored artifacts. The
table and warehouse files remain until you run `purge.sh`.

`purge.sh` deletes measured data only after `--yes` or an interactive `y`.
It refuses to run while the scorer is still in the namespace. It reads the
table location from the metadata saved during teardown; if that document is
missing, it queries the catalog. An unreachable catalog causes an error,
not an assumption that the table is absent.

### Publishing a result

`finish.sh <run_id> --publish results/` writes the run's document under
`results/<engine>/<date>-<engine>-<corpus>-<variant>.json` and re-renders
`results/RESULTS.md` from every document there. `--variant <name>` records the
tuning the run stands for; any engine-specific tuning beyond the run-spec knobs
is a separately named variant rather than a second version of one file.

Publishing a run with `run_valid: false` requires `--publish-invalid`; the
result retains its validity label. This flag does not waive publication metadata
requirements: every fleet role must still have a real `machine_type`. Missing or
placeholder values fail before a result is written to the publication directory.
See [publication requirements](../results/README.md)
and the [result schema](results-format.md).

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

Install the harness with its `aws` extra: `uv sync --extra aws` in a checkout,
or `pip install '.[aws]'`. Local drivers use it to read final table metadata,
measure file geometry and drop tables. Without the extra, Glue request signing
fails because `boto3` is missing.

### What the site declares about a cluster

**Identity.** `setup.sh` binds an IAM role to the three ServiceAccounts through
EKS Pod Identity. SDKs obtain credentials from the agent. Set
`site.kubernetes.aws_region`; pods receive both AWS region variables as explained
in [`pitfalls.md`](pitfalls.md). Omit this key for non-AWS clusters.

**Placement.** `site.kubernetes.node_selector` and `site.kubernetes.tolerations`
apply to every Job and engine pod. Build all images for the selected nodes with
`push-images.sh --platform`; the default is `linux/amd64`.

**Where files go.** `stage.sh` downloads artifacts to `./runs/<run_id>/` beside
`site.yaml`; set `RUNS_DIR` to change that directory. Pods publish artifacts to
`site.runs_root` in object storage so they survive pod deletion. Staging uploads
the run directory, producers upload publish logs, and the scorer mirrors its
artifacts on each poll. `gate.sh` and `finish.sh` read these stored artifacts.

## The run directory

Staging writes `runs/<run_id>/`, and everything downstream reads it:

| Path | Written by | What it is |
|---|---|---|
| `spec.yaml` | stage | the run spec, copied verbatim |
| `facts.json` | stage | what an engine needs to join the run — see [`adding-an-engine.md`](adding-an-engine.md) |
| `timeline.log` | stage | one line per phase transition |
| `job.sql`, `flink-conf.yaml`, `flink.env` | stage | a Flink run's script, the settings it is submitted with, and the cluster shape the local stack sizes containers from |
| `spark-defaults.conf` | stage | a Spark run's settings, and `job.json`, `reader-schema.avsc`, `job.env` beside it |
| `flinkdeployment.yaml` / `sparkapplication.yaml` | stage | the engine resource applied to its Kubernetes operator |
| `flink-job-configmap.yaml` / `spark-job-configmap.yaml` | stage | rendered files packaged as a ConfigMap for engine pods |
| `engine-image.json` | stage | the image the engine ran and the digest the node pulled |
| `publish_log-<i>.jsonl` | producer | one record per batch: rows, bytes, when it was due, when it was acked. Beside the spec locally, under `producer/` on a cluster |
| `scores/summary.json` | scorer | the verdict, rewritten on every poll |
| `scores/freshness.json` | scorer | the lag quantiles and the whole lag curve, on both clocks |
| `scores/exactness.json` | scorer | loss, duplication, corruption, and the first violations |
| `scores/keepup.json` | scorer | the keep-up scalars |
| `scores/geometry.json` | `file-sizes` | file geometry over time and at the end of the run |
| `scores/snapshots.jsonl` | scorer | one record per table commit |
| `scores/keepup_samples.jsonl` | scorer | offered and committed counts, once per poll |
| `table-metadata.final.json` | teardown | the table's last metadata document, copied |
| `run.json` | `collect` | the whole result, redacted — see [`results-format.md`](results-format.md) |

The publish log is also uploaded to the object store as the run goes, because the
scorer reads the offered side from there rather than from the local disk.

## Reading the verdict

`runs/<run_id>/scores/summary.json` contains the verdict. Scripts print its key
fields; see definitions of `run_valid`, `reason` and `state` in
[`methodology.md`](methodology.md) §The verdict.

To inspect artifacts directly, run:

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
