# A recorded AWS smoke run

`deploy/aws/setup.sh`, `scripts/push-images.sh`, `scripts/stage.sh
runs/aws-smoke-flink.yaml`, `scripts/launch.sh`, `scripts/gate.sh` (every
minute), `scripts/teardown.sh`, `scripts/finish.sh` ran this on 2026-09-09,
on EKS: three `m6i.xlarge` amd64 nodes, Flink Kubernetes Operator 1.15.0,
Amazon MSK (three `kafka.m5.2xlarge` brokers, Kafka 3.9.x, IAM auth), Glue
Iceberg REST catalog, EKS Pod Identity, images tagged `6d24375`.
`runs/aws-smoke-flink.yaml`: 2 taskmanagers x 4 slots, 4 GiB each, 4
partitions, one producer shard. Verdict `run_valid: true`, drained,
5,840,896 rows exact (0 loss, 0 duplicates), freshness p50 6.745 s / p95
18.12 s / max 31.645 s vs a 60 s bound, ~95.7% absorbed at offer end, 6.598 s
to drain, backlog max 214,016 rows.

`geometry.json` is what `finish.sh` measured from the table's final
metadata document: 1,984 data files, 723,633,080 bytes (~690 MiB), p50 file
size 178,340.5 bytes (~0.17 MiB), 100% of files under the 32 MiB
small-file threshold. Its `at` map is empty: the geometry ladder starts at
600 s, so every rung of a 300 s run reads `absent` and only the final
snapshot is populated. `run.json` is the `collect` v2 record
(`schema_version: 2`) the results table is rendered from, exactly as
`collect` wrote it: the site's object-store roots, its registry and the
Glue warehouse id (the account id) are substituted with placeholders. Its
`run` object predates `run.compression`, which every document collected now
carries.

`scripts/gate.sh` reported one `UNDERSIZED` tick mid-offer that the next
tick, and the final scorer verdict, both contradicted, an artifact of
`gate.sh` sampling fixed 60 s windows against Flink's 10 s checkpoint
cadence rather than a real backlog.
