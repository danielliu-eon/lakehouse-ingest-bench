# A recorded AWS smoke run

`deploy/aws/setup.sh`, `scripts/push-images.sh`, `scripts/stage.sh
runs/aws-smoke-spark.yaml`, `scripts/launch.sh`, `scripts/gate.sh` (every
minute), `scripts/teardown.sh`, `scripts/finish.sh` ran this on 2026-09-09,
on EKS: three `m6i.xlarge` amd64 nodes, Kubeflow spark-operator 2.5.2 (webhook
enabled), Amazon MSK (three `kafka.m5.2xlarge` brokers, Kafka 3.9.x, IAM
auth), Glue Iceberg REST catalog, EKS Pod Identity, images tagged `e508270`.
`runs/aws-smoke-spark.yaml`: 2 executors x 2 cores, 4 GiB each, 2 GiB driver,
a 10 s trigger interval, hash distribution mode, 4 Kafka partitions, one
producer shard. Verdict `run_valid: true`, drained, 5,840,896 rows exact (0
loss, 0 duplicates, 300 scored batches), freshness p50 9.695 s / p95 19.943 s
/ max 36.01 s vs a 60 s bound (120 s max bound), ~96.5% absorbed at offer
end, 8.418 s to drain, backlog max 272,384 rows, 32 snapshots.

`geometry.json` is what `finish.sh` measured from the table's final
metadata document: 1,984 data files, 719,577,266 bytes (~686 MiB), p50 file
size 176,877.5 bytes (~0.17 MiB), 100% of files under the 32 MiB
small-file threshold. `run.json` is the `collect` v2 record
(`schema_version: 2`) the results table is rendered from, exactly as
`collect` wrote it: the site's object-store roots, its registry and the
Glue warehouse id (the account id) are substituted with placeholders.

`scripts/gate.sh` reported `PASS` on every tick, lag never exceeding 36 s
against the 120 s max bound. This is the second attempt at the cell: the
first was refused by `stage.sh`'s `verify-spark` step before launch because
the two executors were still pulling the image (pods `Pending`) when the
`SparkApplication` reported `RUNNING`; `stage.sh` now waits for the fleet to
be placed instead of calling that drift (commit `e508270`).
