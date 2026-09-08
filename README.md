# lakehouse-ingest-bench

Benchmark for streaming ingest from Apache Kafka into Apache Iceberg. Measures keep-up,
freshness (contiguous-prefix lag), exactness (loss, duplication, corruption) and file geometry
for any engine that consumes a Kafka topic and appends to an Iceberg table. Ships Apache Flink
and Apache Spark as managed engines.

Status: phase 1 — local end to end. See `scripts/smoke.sh`.
