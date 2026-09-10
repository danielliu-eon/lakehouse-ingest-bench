# lakehouse-ingest-bench

Benchmark for streaming ingest from Apache Kafka into Apache Iceberg. It scores
any engine that consumes a Kafka topic and appends to an Iceberg table on four
measures: **keep-up**, whether it holds the rate it is offered; **freshness**,
how long a row waits between being offered and being readable in the table;
**exactness**, whether every row arrives once and intact; and **file geometry**,
the size and shape of the files it leaves behind. Apache Flink and Apache Spark
ship as managed engines; any other engine joins on the external tier, which the
harness prepares a run for and never touches.

**Status.** Both engines run two ways: on Docker Compose locally, and on AWS
(EKS, Amazon MSK, one S3 bucket, the Glue Iceberg REST catalog). Three recorded
smoke runs are under [`docs/examples/`](docs/examples/). Not here yet: any
published result in [`results/`](results/) — the hour-long 100 MB/s runs fill
it, and `runs/aws-100mbs-skew-*.yaml` are their specs, sized as a probe ladder's
start rather than as an answer — the 600 MB/s presets, tuned engine variants,
and any GCP deployment. The harness reads and writes `gs://` paths, but the
drivers refuse a root that is not `s3://` and nothing here stands a cluster up
on GCP.

## What makes a result fair

The corpus is generated once and frozen, and every figure in its manifest is
re-derived from the stored bytes by a reader that shares nothing with the
encoder. The harness writes no source, sink or serializer for any engine, so a
Flink result is Flink's. The scorer never asks a writer what it wrote: freshness
and exactness come out of the table's own metadata and manifests, the surface
any reader of the table sees. And `run_valid: true` is the only field that
permits publishing a result — true only when the scoring loop reached a verdict
rather than abandoning the run, and the table held the corpus's columns,
drained, kept its p95 lag inside the spec's bound, was exact, and the producer
kept to its schedule. [`docs/methodology.md`](docs/methodology.md) is how each
of those is defined, and why.

## Quickstart

You need Docker with Compose v2, plus `jq`, `yq` (mikefarah v4) and `curl` on
the host, and `uv` for the tests and the tools outside a container. Give Docker
16 GB of RAM.

```bash
git clone <this repository> && cd lakehouse-ingest-bench
uv sync                                 # only for the tests and the tools outside a container
scripts/smoke.sh                        # about ten minutes
scripts/smoke.sh --engine spark         # the same run on Spark
```

That builds a five-minute 5 MB/s corpus, creates a Kafka topic and an Iceberg
table, starts stock Flink on them, offers the corpus on its original timeline
and scores what lands in the table. It ends with the verdict — these are the
figures of the run recorded in
[`docs/examples/smoke-flink/`](docs/examples/smoke-flink/):

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

`run_valid: true` is the whole point: all 5,840,896 offered rows arrived exactly
once, the table stayed inside that spec's 60-second freshness bound, and the
producer kept to its schedule. `absorbed_at_offer_end: 0.969` says the engine
held the offered rate with little standing debt. The block is abridged — the
tool also prints `reason` and two backlog figures — and every artifact behind it
stays in `runs/<run_id>/`.

`--set duration_s=30` gives a 30-second corpus and a run in a few minutes, which
is what CI uses. Two of its verdict fields then read oddly, for reasons
[`docs/running.md`](docs/running.md) gives: a 30-second corpus is shorter than
the freshness warmup and than one commit cycle.

**Nothing measured locally is a result.** The harness, the broker, the object
store and the engine share one machine's cores, and on an arm64 host the engine
image runs emulated. The smoke proves that the pieces agree about a run, not how
fast an engine is.

## On a cloud

A measured run needs a cluster, and the cluster is yours: neither deploy script
creates, deletes or reconfigures it. Once per account,
[`deploy/aws/README.md`](deploy/aws/README.md) takes you from an empty account to
a corpus in a bucket — `setup.sh`, a filled-in `site.yaml`, `push-images.sh`,
`gen-corpus.sh`. Once per run, [`docs/running.md`](docs/running.md) is the order
the drivers go in: `stage.sh` → `launch.sh` → `gate.sh` → `teardown.sh` →
`finish.sh` → `purge.sh`. Amazon MSK bills by the hour whether or not a run is
using it, so tear it down between campaigns.

## Publishing a result

`scripts/finish.sh <run_id> --publish results/` writes one redacted JSON
document under `results/<engine>/` and re-renders `results/RESULTS.md` from
every document there. What a published result must satisfy is in
[`results/README.md`](results/README.md); the document's own schema, and the
five site roots redaction substitutes out of it, are in
[`docs/results-format.md`](docs/results-format.md).

## Repository layout

| Path | What is in it |
|---|---|
| `ingest_bench/` | the harness: corpus generator, producer, scorer, table tools, spec loaders, manifest renderer |
| `engines/<name>/` | everything specific to one managed engine, and the harness has no engine branches outside it |
| `scripts/` | the local smoke and the cluster drivers |
| `deploy/compose/local/` | the local stack |
| `deploy/aws/`, `deploy/k8s/` | the cloud account and its manifests |
| `workloads/` | the schemas and presets a corpus is built from |
| `runs/` | `runs/*.yaml` are the shipped run specs, `runs/<run_id>/` one run's artifacts |
| `results/` | published results, and the table generated from them |

## Where to go next

- [`docs/methodology.md`](docs/methodology.md) — every term, how freshness,
  exactness, keep-up and geometry are defined, and what the verdict means.
- [`docs/running.md`](docs/running.md) — the local smoke, CI, the cloud driver
  sequence, the run directory.
- [`docs/run-spec.md`](docs/run-spec.md) — every key of a run spec and of a site
  config, its default and its effect.
- [`docs/corpus.md`](docs/corpus.md) — the presets, the schema a corpus is
  declared with, generation and its memory needs, sizing the offer.
- [`docs/pitfalls.md`](docs/pitfalls.md) — the traps that cost a run, or produce
  a figure that looks fine.
- [`docs/adding-an-engine.md`](docs/adding-an-engine.md) — the six-rule contract
  an engine must honour, the external tier that is the primary way in, a
  walk-through, and what a managed engine's directory holds.
- [`docs/results-format.md`](docs/results-format.md) and
  [`results/README.md`](results/README.md) — the result document, and the rules
  for publishing one.
- [`deploy/aws/README.md`](deploy/aws/README.md) — what a run on AWS needs of an
  account, what `setup.sh` builds, what it costs, and how to remove it.
- [`engines/flink/README.md`](engines/flink/README.md) and
  [`engines/spark/README.md`](engines/spark/README.md) — each managed engine:
  what it runs, its knobs, and its traps.
- [`CONTRIBUTING.md`](CONTRIBUTING.md) — the checks, and how to add a corpus
  shape, an engine or a result.

## Licence

Apache-2.0 — [`LICENSE`](LICENSE), with the copyright holder in
[`NOTICE`](NOTICE), since the licence names no licensor of its own. Whoever
publishes this fills its placeholders and the matching one in `pyproject.toml`.
