# A recorded AWS smoke run

`deploy/aws/setup.sh`, `scripts/push-images.sh`, `scripts/gen-corpus.sh smoke
--shards 4`, `scripts/stage.sh runs/aws-smoke-flink.yaml`,
`scripts/launch.sh`, `scripts/gate.sh`, `scripts/finish.sh`,
`scripts/teardown.sh` — on 2026-09-09, on EKS: three `m6i.xlarge` amd64 nodes,
Flink Kubernetes Operator 1.15.0, Amazon MSK (two `kafka.m5.large` brokers,
Kafka 3.6.0, IAM auth), Glue Iceberg REST catalog, EKS Pod Identity.
`runs/aws-smoke-flink.yaml`: 2 taskmanagers x 4 slots, 4 GiB each, 4
partitions. Verdict `run_valid: true`, drained, 5,840,896 rows exact (0 loss,
0 duplicates), freshness p50 7.7 s/p95 14.4 s/max 27.4 s vs a 60 s bound,
99.3% absorbed at offer end, 5.9 s to drain. `corpus_uri` in `summary.json` is
redacted to a placeholder; every other figure is as the scorer wrote it.
