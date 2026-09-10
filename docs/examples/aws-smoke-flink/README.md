# Recorded AWS Flink smoke

This run used the setup, image-push, stage, launch, gate, teardown and finish
scripts on 2026-09-09. The gate ran once per minute.

- EKS with three amd64 `m6i.xlarge` nodes.
- Flink Kubernetes Operator 1.15.0.
- Amazon MSK: three `kafka.m5.2xlarge` brokers, Kafka 3.9.x and IAM authentication.
- Glue Iceberg REST catalog and EKS Pod Identity.
- Images tagged `6d24375`; spec `runs/aws-smoke-flink.yaml`.

The run predates the scorer and producer's 2-CPU requests. The current smoke
requires four nodes; see [cluster sizing](../../../deploy/aws/README.md#sizing-the-cluster).

## Run and verdict

`runs/aws-smoke-flink.yaml`: 2 taskmanagers x 4 slots, 4 GiB each, 4
partitions, one producer shard. Verdict `run_valid: true`, drained,
5,840,896 rows exact (0 loss, 0 duplicates), freshness p50 6.745 s / p95
18.12 s / max 31.645 s vs a 60 s bound, ~95.7% absorbed at offer end, 6.598 s
to drain, backlog max 214,016 rows.

## File geometry

`finish.sh` measured `geometry.json` from the final table metadata:
1,984 data files, 723,633,080 bytes (~690 MiB), p50 file
size 178,340.5 bytes (~0.17 MiB), 100% of files under the 32 MiB
small-file threshold.
All five offsets in its `at` map are `absent`: the first is 600 s, beyond
this 300-second run. Only the final snapshot has geometry measurements.

## Result artifacts

`run.json` is the original `collect` record (`schema_version: 2`). Object-store
roots, the image registry and the Glue warehouse account id are replaced with
placeholders. It predates the `run.compression` field.

`scripts/gate.sh` reported one `UNDERSIZED` tick during the offer, followed by
a passing tick and a valid final verdict. The transient gate result was attributed
to its 60-second sampling windows interacting with Flink's 10-second checkpoints.
