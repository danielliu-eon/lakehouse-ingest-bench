# Run Kafka and the catalog inside Kubernetes

`setup.sh` installs Kafka through Strimzi and an Iceberg REST catalog through
Lakekeeper with Postgres. It prints the values needed for `site.yaml`;
`teardown.sh` removes the stack. Both check existing resources so interrupted
operations can be rerun.

Use [site.k8s.example.yaml](../../../site.k8s.example.yaml) for this deployment.
The run drivers, engines, and scorer use the same interfaces as the managed
broker deployment. Manage the Kubernetes cluster, node groups, and add-ons
separately.

## Prerequisites

- **Kubernetes and object storage.** On AWS, use EKS with the Pod Identity agent
  and an accessible bucket. `deploy/aws/setup.sh` can provision the bucket and
  image registry.
- **EBS CSI driver on AWS.** Broker and database volumes require
  `ebs.csi.aws.com`. Setup checks for it and suggests:

  ```bash
  eksctl create addon --cluster YOUR_CLUSTER_NAME --region YOUR_REGION \
    --name aws-ebs-csi-driver --auto-apply-pod-identity-associations
  ```

- **Dedicated broker nodes for measured runs.** Keep broker CPU and network
  activity off engine nodes. The [node group example](../../aws/eksctl-kafka-nodegroup.example.yaml)
  includes a broker taint, selector, and toleration. Dedicated nodes are optional
  for smoke checks.
- **Engine operators.** Install the Flink or Spark operator needed by your run;
  `deploy/aws/setup.sh` installs both. The Spark operator must watch the stack
  namespace. Stack setup warns and prints an upgrade command if it does not,
  leaving shared operator configuration for you to change.
- **Host tools.** Use the tools listed in the [AWS guide](../../aws/README.md)
  plus `helmfile` 0.170 or later.

## Environment

| Variable | Default | What it is |
|---|---|---|
| `CLOUD` | *required* | `aws`; selects `deploy/aws/_stack_hooks.sh`. Other clouds are unsupported |
| `KUBE_CONTEXT` | `$CLUSTER_NAME` | The kubeconfig context |
| `NAMESPACE` | `ingest-bench` | Namespace for the stack and runs; use a separate namespace when also running the managed-broker deployment |
| `BUCKET` | *required* | The bucket with `corpus/`, `runs/` and `warehouse/` |
| `AWS_REGION`, `CLUSTER_NAME` | *required with `CLOUD=aws`* | The region, and the EKS cluster for the pod identity associations |
| `KAFKA_BROKERS` | `3` | Dual-role broker/controller replicas. Replication is `min(3, brokers)`; minimum in-sync replicas is `max(1, replication − 1)` |
| `KAFKA_VOLUME_GI` | `500` | Volume per broker. Grown on a re-run when raised, never shrunk |
| `KAFKA_VOLUME_THROUGHPUT_MIBS` / `KAFKA_VOLUME_IOPS` | `250` / `6000` | Provisioned gp3 throughput and IOPS |
| `KAFKA_CPU` / `KAFKA_MEM_GI` | `4` / `16` | Broker requests and limits; the heap is `KAFKA_JVM_HEAP` (`6g`) and the rest is page cache |
| `KAFKA_NODE_SELECTOR` / `KAFKA_TOLERATIONS` | `{}` / `[]` | One-line JSON placing the brokers on their node group |
| `CATALOG_NODE_SELECTOR` / `CATALOG_TOLERATIONS` | `{}` / `[]` | Placement for Lakekeeper, Postgres, and the schema registry |
| `STRIMZI_VERSION` | `1.2.0` | The operator chart, `oci://quay.io/strimzi-helm/strimzi-kafka-operator` |
| `LAKEKEEPER_CHART_VERSION` | `0.12.0` | From `https://lakekeeper.github.io/lakekeeper-charts/` |
| `WITH_SCHEMA_REGISTRY` | `false` | `true` also applies `deploy/k8s/schema-registry.yaml.tmpl`, for a run whose spec says `kafka.value_encoding: confluent` |
| `KAFKA_READY_WAIT_S` / `CATALOG_READY_WAIT_S` | `900` / `600` | How long the Kafka CR and the catalog may take to be ready |


## What setup creates

1. The namespace, three run service accounts, and engine RBAC, using the shared
   namespace manifest.
2. On AWS, the `lakehouse-ingest-bench-stack` IAM role, scoped to the bucket's
   three prefixes. Pod Identity binds it to the run accounts and
   `ingest-bench-catalog`. This role is independent of MSK permissions.
3. On AWS, the expandable gp3 StorageClass `ingest-bench-kafka`, with provisioned
   throughput and binding in the scheduled pod's availability zone.
4. The `ingest-bench-catalog-keys` Secret: encryption key, database passwords,
   and warehouse credential external ID. Setup creates it once and preserves it
   on reruns so stored secrets remain readable.
5. Three Helm releases: Strimzi in `strimzi-operator`, Kafka named `ingest-bench`,
   and Lakekeeper with Postgres. Strimzi watches the stack namespace. Kafka uses
   an internal, unauthenticated listener on port 9092 and dual-role nodes;
   Lakekeeper also has authentication disabled.
6. The `ingest-bench` warehouse at `s3://$BUCKET/warehouse`. Credential vending
   and remote signing are disabled because pods access storage through their
   own identities. Hard deletes match `purge.sh`'s file cleanup.

## Set up the stack

```bash
export CLOUD=aws AWS_REGION=... CLUSTER_NAME=... BUCKET=...
export KAFKA_NODE_SELECTOR='{"lakehouse-ingest-bench/role":"kafka"}'
export KAFKA_TOLERATIONS='[{"key":"lakehouse-ingest-bench/kafka","operator":"Equal","value":"true","effect":"NoSchedule"}]'
deploy/k8s/stack/setup.sh
cp site.k8s.example.yaml site.yaml        # fill in the values printed by setup
```

Build images and generate a corpus with `scripts/push-images.sh` and
`scripts/gen-corpus.sh`, as described in the [AWS guide](../../aws/README.md).
Existing images and corpus data can be reused. Then follow the
[per-run workflow](../../../docs/running.md).

Broker nodes and volumes continue billing while idle. The hour-long workload
uses three 500-GiB gp3 volumes with provisioned throughput and three eight-vCPU
nodes. Use smaller volumes and a smaller node group for a new smoke deployment.
Teardown removes the stack's volumes but leaves the node group.

## Tear down the stack

```bash
deploy/k8s/stack/teardown.sh              # Kafka, catalog, namespace, and identities
deploy/k8s/stack/teardown.sh --all        # also Strimzi and the StorageClass
deploy/k8s/stack/teardown.sh --all --yes  # same cleanup without confirmation
```

Teardown asks for confirmation before removing the listed resources. It deletes
Kafka while Strimzi is still running so the operator can remove broker claims,
then removes the catalog, namespace, and identities. Namespace deletion also
removes database claims and run objects.

The node group, CSI add-on, and bucket remain. Even with `--all`, Strimzi's CRDs
remain because Helm does not delete CRDs installed from `crds/`.

## Troubleshooting

- **Pending persistent volumes:** check the CSI driver and node placement.
  `WaitForFirstConsumer` creates a volume in the scheduled pod's zone; the
  selector and tolerations must allow that pod onto a suitable node.
- **Catalog cannot access the bucket:** warehouse creation checks read/write
  access. A catalog pod started before its Pod Identity association may need
  a deployment restart, followed by another setup invocation. Also inspect the
  returned catalog error for permission or storage configuration problems.

## Cloud hooks

Cloud-specific behavior lives in `deploy/aws/_stack_hooks.sh`. It defines
preflight checks, identity binding and removal, StorageClass management, catalog
settings, and warehouse storage/credential JSON. Setup-only storage checks are
separate so teardown does not require a bucket or CSI driver that is already gone.

A future cloud implementation must supply the same nine hook functions and add
support for its storage URI scheme in the drivers. `CLOUD=gcp` is currently
unsupported.
