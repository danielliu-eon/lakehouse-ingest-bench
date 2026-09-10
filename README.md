# lakehouse-ingest-bench

Benchmark streaming ingest from Apache Kafka into Apache Iceberg. The harness
scores engines on four measures:

- **Keep-up:** whether the engine sustains the offered rate.
- **Freshness:** time from a row's scheduled offer to its visibility in the table.
- **Exactness:** whether row counts and ID checksums match the offered corpus.
- **File geometry:** the sizes and distribution of output files.

Apache Flink and Apache Spark are included as managed engines. For other
engines, the external tier prepares the topic and table for you to consume.

## Status

Both managed engines support local Docker Compose and Kubernetes runs.
AWS deployment options include Amazon MSK with the Glue Iceberg REST catalog,
or Kafka and Lakekeeper inside the cluster. See
[`deploy/aws/README.md`](deploy/aws/README.md) and
[`deploy/k8s/stack/README.md`](deploy/k8s/stack/README.md).

Three recorded smoke runs are in [`docs/examples/`](docs/examples/).
No benchmark results have been published in [`results/`](results/) yet.
The hour-long `runs/aws-100mbs-skew-*.yaml` specs provide starting fleets for
capacity testing; their sizing has not been established by a published result.
The 600 MB/s presets and tuned engine variants also await published results.
There is no GCP deployment: the harness supports `gs://`, but the shell drivers
require `s3://` storage roots.

## How results are measured

Each run uses a frozen corpus whose manifest is checked against stored bytes
by an independent decoder. Engines use their standard connectors and
serializers. The scorer reads the table's metadata, manifests and data files
to measure committed rows independently of the writer.

A headline result requires `run_valid: true`: scoring completed, the table
preserved the required columns and drained, freshness met its p95 and maximum
lag limits, exactness checks passed, and the producer kept to its schedule.
[`docs/methodology.md`](docs/methodology.md) defines each measure and its limits.

## Quickstart

Install Docker with Compose v2, `jq`, `yq` (mikefarah v4), and `curl`.
Use `uv` for tests and tools run outside containers. Give Docker 16 GB of RAM.

```bash
git clone <this repository> && cd lakehouse-ingest-bench
uv sync                                # host tools and tests
scripts/smoke.sh                        # Flink; about ten minutes
scripts/smoke.sh --engine spark
```

The smoke test generates a five-minute, 5 MB/s corpus, creates a Kafka topic
and an Iceberg table, starts the engine, replays the corpus on its scheduled
timeline, and scores the table. It prints a verdict like this abridged output
from the [recorded Flink smoke run](docs/examples/smoke-flink/):

```json
{
  "run_valid": true,
  "state": "drained",
  "producer_bound": false,
  "prefix": 299,
  "last_batch": 299,
  "committed_rows": 5840896,
  "offered_rows": 5840896,
  "freshness": { "p50_s": 10.937, "p95_s": 17.636, "p99_s": 27.470, "max_s": 29.930 },
  "exactness": { "exact": true, "loss_rows": 0, "duplicate_rows": 0 },
  "keepup": { "absorbed_at_offer_end": 0.969, "drain_s": 4.87 }
}
```

In this run, all 5,840,896 offered rows were accounted for by the exactness
checks, freshness met the 60-second bound, and the producer kept to schedule.
`absorbed_at_offer_end: 0.969` means 96.9% of offered rows were committed when
the offer ended. Full output also includes `reason` and backlog figures.
Artifacts remain in `runs/<run_id>/`.

For a shorter check, add `--set duration_s=30`, as CI does. The offer may end
before the freshness warmup ends or the first commit occurs; see [`docs/running.md`](docs/running.md) for how to interpret its verdict.

**Local smoke tests are integration checks, not performance results.**
The harness, broker, object store and engine share one machine's resources.

## Run on a cluster

You provide and manage the Kubernetes cluster. Follow
[`deploy/aws/README.md`](deploy/aws/README.md) to configure the AWS services,
fill in `site.yaml`, push images and generate a corpus. Its cluster-sizing
section explains the resources a run needs.

For each run, [`docs/running.md`](docs/running.md) describes this sequence:
`stage.sh` → `launch.sh` → `gate.sh` → `teardown.sh` → `finish.sh` → `purge.sh`.
`scripts/run.sh <spec>` runs all steps except purge. Tear down billable
services such as Amazon MSK between campaigns.

For an in-cluster deployment, start from `site.k8s.example.yaml` and keep
brokers on dedicated nodes for measured runs. This separates broker resource
use from engine measurements.

## Publish a result

`scripts/finish.sh <run_id> --publish results/` writes redacted JSON under
`results/<engine>/` and regenerates `results/RESULTS.md`.
[`results/README.md`](results/README.md) lists publication requirements;
[`docs/results-format.md`](docs/results-format.md) defines the result schema
and redaction rules.

## Repository layout

| Path | Contents |
|---|---|
| `ingest_bench/` | Corpus generator, producer, scorer, table tools, spec loaders and manifest renderer |
| `engines/<name>/` | Managed-engine implementation and configuration |
| `scripts/` | Local smoke test and cluster run drivers |
| `deploy/compose/local/` | Local stack |
| `deploy/aws/`, `deploy/k8s/` | Cloud setup and Kubernetes manifests |
| `workloads/` | Corpus schemas and presets |
| `runs/` | Run specs (`*.yaml`) and per-run artifacts (`<run_id>/`) |
| `results/` | Published results and their generated comparison table |

## Documentation

- [Methodology](docs/methodology.md): terms, measures and verdicts.
- [Running](docs/running.md): local smoke tests, cluster drivers and artifacts.
- [Run specs](docs/run-spec.md): run and site configuration reference.
- [Corpora](docs/corpus.md): schemas, presets, generation and offer sizing.
- [Pitfalls](docs/pitfalls.md): common run failures and misleading measurements.
- [Adding an engine](docs/adding-an-engine.md): external and managed contracts.
- [Result format](docs/results-format.md) and [publishing](results/README.md).
- [AWS setup](deploy/aws/README.md): infrastructure, sizing and cleanup.
- [Flink](engines/flink/README.md) and [Spark](engines/spark/README.md): setup,
  tuning and limitations.
- [Contributing](CONTRIBUTING.md): checks and extension guidelines.

## Licence

[Apache-2.0](LICENSE). Before publishing the project, fill in the copyright
placeholders in [NOTICE](NOTICE) and the matching metadata in `pyproject.toml`.
