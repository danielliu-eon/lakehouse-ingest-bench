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

The run used two TaskManagers with four slots and 4 GiB each, four Kafka
partitions, and one producer shard. The final verdict was `run_valid: true`:

- The run drained with 5,840,896 rows; count and checksum checks passed.
- Freshness was 6.745 s at p50, 18.12 s at p95, and 31.645 s maximum.
  The p95 bound was 60 s.
- About 95.7% of offered rows were absorbed at offer end; the rest drained
  in 6.598 s.
- Backlog peaked at 214,016 rows.

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
