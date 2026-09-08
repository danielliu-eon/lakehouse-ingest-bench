# lakehouse-ingest-bench

Benchmark for streaming ingest from Apache Kafka into Apache Iceberg. Measures keep-up,
freshness (contiguous-prefix lag), exactness (loss, duplication, corruption) and file geometry
for any engine that consumes a Kafka topic and appends to an Iceberg table. Ships Apache Flink
and Apache Spark as managed engines.

Status: phase 1 — local end to end. See `scripts/smoke.sh`.

## Quickstart

You need Docker with Compose v2, plus `jq`, `yq` (mikefarah v4) and `curl` on
the host. Give Docker 16 GB of RAM.

```bash
git clone <this repository> && cd lakehouse-ingest-bench
uv sync                     # only for the tests and the tools outside a container
scripts/smoke.sh            # add --set duration_s=30 for a quicker one
```

That builds a five-minute 5 MB/s corpus, creates a Kafka topic and an Iceberg
table, starts stock Flink on them, offers the corpus on its original timeline
and scores what lands in the table. It ends with the verdict:

```json
{
  "run_valid": true,
  "state": "drained",
  "producer_bound": false,
  "freshness": { "p50_s": 8.6, "p95_s": 13.4, "p99_s": 14.6, "max_s": 14.8 },
  "exactness": { "exact": true, "loss_rows": 0, "duplicate_rows": 0 },
  "keepup": { "absorbed_at_offer_end": 0.98, "drain_s": 11.2 }
}
```

`run_valid: true` is the whole point: every offered row arrived exactly once,
the table stayed inside its freshness bound, and the producer kept to its
schedule. The artifacts behind it stay in `runs/<run_id>/`.

Nothing measured locally is a result. The harness, the broker, the object store
and the engine share one machine's cores, and on an arm64 host the engine image
runs emulated. The smoke proves that the pieces agree about a run, not how fast
an engine is.

## Where to go next

- [`docs/running.md`](docs/running.md) — prerequisites, the smoke's flags, the
  run directory, and how to read the verdict.
- [`docs/adding-an-engine.md`](docs/adding-an-engine.md) — the six-rule contract
  an engine must honour, a walk-through with an engine the harness does not
  manage, and what a managed engine's directory holds.
- `docs/methodology.md` — how freshness, exactness and keep-up are defined, and
  why (phase 3).
- [`engines/flink/README.md`](engines/flink/README.md) — the managed Flink
  engine: what it runs, its knobs, and its traps.
