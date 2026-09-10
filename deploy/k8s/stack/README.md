# Running the broker and the catalog inside the cluster

`setup.sh` installs Apache Kafka (through the Strimzi operator) and Lakekeeper
(an Iceberg REST catalog, with its own Postgres) into the Kubernetes cluster
the engine runs in, and prints the values to put in `site.yaml`. `teardown.sh`
removes them. Both are idempotent: every step describes before it creates or
deletes, so a re-run after a timeout converges instead of failing.

Nothing about a run changes. The drivers, the engines and the scorer read the
broker and the catalog out of `site.yaml`, and this is a different way of
filling that file in — the same shape the local Compose stack has, on a
cluster. Copy `site.k8s.example.yaml` rather than the AWS example.

Neither script creates, deletes or reconfigures the cluster, its node groups
or its add-ons. Those are yours; the prerequisites below say what they need.

## Prerequisites

- **A Kubernetes cluster** with a bucket its pods can be given an identity
  for. On AWS that is an EKS cluster with the Pod Identity agent, and
  `deploy/aws/setup.sh` has run once for the bucket and the registry.
- **The EBS CSI driver** (AWS). The brokers and the catalog's database need
  persistent volumes, and a cluster without `ebs.csi.aws.com` leaves every
  claim pending forever. `setup.sh` refuses without it and prints the line:

  ```bash
  eksctl create addon --cluster YOUR_CLUSTER_NAME --region YOUR_REGION \
    --name aws-ebs-csi-driver --auto-apply-pod-identity-associations
  ```
- **A node group for the brokers.** Placed beside the engine they would share
  its cores and its network, and the engine's figure would carry the
  broker's cost. `../../aws/eksctl-kafka-nodegroup.example.yaml` makes a
  tainted one; the selector and toleration to give `setup.sh` are in its
  header. Optional for a smoke, not for a measured run.
- **The engine operators.** The Flink operator and the Kubeflow
  spark-operator come from `deploy/aws/setup.sh`. The spark-operator watches
  only the namespaces it was installed with; a Spark run in a namespace it
  does not watch never starts. `setup.sh` checks and prints the
  `helm upgrade` line rather than running it, because an operator shared with
  another namespace is not this script's to change.
- **Host tools**: everything `deploy/aws/README.md` lists, plus `helmfile`
  (0.170 or later). Only these two scripts need it.

## Environment

| Variable | Default | What it is |
|---|---|---|
| `CLOUD` | *required* | `aws`. The account-side hook, `deploy/<cloud>/_stack_hooks.sh`. `gcp` is refused by name: it is the next cloud, and the seam is that one file |
| `KUBE_CONTEXT` | `$CLUSTER_NAME` | The kubeconfig context |
| `NAMESPACE` | `ingest-bench` | Where the broker, the catalog, the harness Jobs and both engines' runs live. A cluster that also runs the managed-broker shape gives this one its own |
| `BUCKET` | *required* | The bucket with `corpus/`, `runs/` and `warehouse/` |
| `AWS_REGION`, `CLUSTER_NAME` | *required with `CLOUD=aws`* | The region, and the EKS cluster for the pod identity associations |
| `KAFKA_BROKERS` | `3` | Node pool replicas, each a controller and a broker. Replication is `min(3, brokers)`, in sync one fewer — the factor staging asks for |
| `KAFKA_VOLUME_GI` | `500` | Volume per broker. Grown on a re-run when raised, never shrunk |
| `KAFKA_VOLUME_THROUGHPUT_MIBS` / `KAFKA_VOLUME_IOPS` | `250` / `6000` | The gp3 class's provisioned throughput and iops |
| `KAFKA_CPU` / `KAFKA_MEM_GI` | `4` / `16` | Broker requests and limits; the heap is `KAFKA_JVM_HEAP` (`6g`) and the rest is page cache |
| `KAFKA_NODE_SELECTOR` / `KAFKA_TOLERATIONS` | `{}` / `[]` | One-line JSON placing the brokers on their node group |
| `CATALOG_NODE_SELECTOR` / `CATALOG_TOLERATIONS` | `{}` / `[]` | The same for Lakekeeper, its Postgres and the schema registry |
| `STRIMZI_VERSION` | `1.2.0` | The operator chart, `oci://quay.io/strimzi-helm/strimzi-kafka-operator` |
| `LAKEKEEPER_CHART_VERSION` | `0.12.0` | From `https://lakekeeper.github.io/lakekeeper-charts/` |
| `WITH_SCHEMA_REGISTRY` | `false` | `true` also applies `deploy/k8s/schema-registry.yaml.tmpl`, for a run whose spec says `kafka.value_encoding: confluent` |
| `KAFKA_READY_WAIT_S` / `CATALOG_READY_WAIT_S` | `900` / `600` | How long the Kafka CR and the catalog may take to be ready |

## What `setup.sh` creates

1. The namespace, the three run identities and each engine's RBAC, from the
   same manifest `deploy/aws/setup.sh` applies.
2. **Identity** (AWS): one role, `lakehouse-ingest-bench-stack`, over the
   bucket's three prefixes and nothing else, bound through Pod Identity to the
   three run identities and to the catalog's, `ingest-bench-catalog` — the
   catalog writes the warehouse's metadata. Separate from the role
   `deploy/aws/setup.sh` makes, so the stack stands without a managed broker.
3. **StorageClass** `ingest-bench-kafka` (AWS): gp3 with provisioned
   throughput, bound when a broker is scheduled, expandable.
4. **Secret** `ingest-bench-catalog-keys`: the catalog's encryption key, its
   database passwords and the warehouse credential's external id. Made once
   and never regenerated — the key guards what the catalog has stored.
5. **Three releases**, through `helmfile.yaml.gotmpl`: the Strimzi operator
   in `strimzi-operator`, watching this namespace alone; the Kafka cluster
   `ingest-bench` from `charts/kafka` — one plain listener on 9092, no
   authentication, one dual-role node pool; Lakekeeper with the chart's
   bundled Postgres, authentication off, as `ingest-bench-catalog`.
6. **The warehouse** `ingest-bench` over `s3://$BUCKET/warehouse`, created
   through the catalog's management API with credential vending and remote
   signing off and hard deletes. Off, because every pod already reaches the
   bucket as its own identity — the catalog stays off the data path. Hard,
   because `purge.sh` removes a table's files itself.

It ends by printing the values to fill into `site.yaml`.

## Once per cluster

```bash
export CLOUD=aws AWS_REGION=... CLUSTER_NAME=... BUCKET=...
export KAFKA_NODE_SELECTOR='{"lakehouse-ingest-bench/role":"kafka"}'
export KAFKA_TOLERATIONS='[{"key":"lakehouse-ingest-bench/kafka","operator":"Equal","value":"true","effect":"NoSchedule"}]'
deploy/k8s/stack/setup.sh
cp site.k8s.example.yaml site.yaml        # setup.sh prints every value to fill in
```

Then `scripts/push-images.sh` and `scripts/gen-corpus.sh` as in
`deploy/aws/README.md`, and the per-run drivers of `docs/running.md`. Images
and corpus already in the bucket from the managed-broker shape serve this one
too.

> **The brokers' nodes and volumes bill whether or not a run is using them.**
> Three 500 GiB gp3 volumes with provisioned throughput and three 8-vCPU nodes
> are the hour-run shape; a smoke campaign lowers `KAFKA_VOLUME_GI` and the
> node group, and `teardown.sh` between campaigns returns everything but the
> node group.

## `teardown.sh`

```bash
deploy/k8s/stack/teardown.sh          # the Kafka cluster, the catalog, the namespace, the identities
deploy/k8s/stack/teardown.sh --all    # also the Strimzi operator and the StorageClass
deploy/k8s/stack/teardown.sh --all --yes   # unattended
```

In that order because each deletion needs the one before it: the Kafka
cluster goes while its operator still runs, since the operator is what
deletes the brokers' claims; the namespace takes the catalog's claim and every
run object with it. The node group, the CSI add-on and the bucket stay, and so
do the Strimzi operator's CRDs under `--all` — Helm never removes what it
installed from `crds/`.

## Two traps

- **A claim that pends.** A broker or the database `Pending` with
  `no persistent volumes available` is a cluster without the CSI driver, or
  a StorageClass whose zone has no node for the pod. `setup.sh` refuses the
  first; for the second, the brokers' node group needs a node in each zone
  a volume may land in — `WaitForFirstConsumer` binding makes the pod's zone
  the volume's.
- **A catalog that cannot reach the bucket.** The warehouse is created with a
  read-and-write check, and it fails when the catalog pod started before its
  pod identity association held. `setup.sh` binds before it installs, so this
  is a re-run after a partial first attempt: restart the deployment and
  re-run, as the refusal says.

## The cloud seam

Everything cloud-specific is one sourced file, `deploy/aws/_stack_hooks.sh`:
nine functions — `stack_preflight` and `stack_preflight_storage` (the account
and the storage checks, split so a teardown does not need the bucket or the
CSI add-on), `stack_bind_identity` and `stack_unbind_identity`,
`stack_storage_class` and `stack_delete_storage_class`,
`stack_catalog_settings`, and `stack_storage_profile_json` and
`stack_storage_credential_json` for the warehouse. A GCP hook is the same
functions over Workload Identity, a GCS storage profile with
`gcp-system-identity` and a `pd-balanced` class — plus the `gs://` roots the
drivers do not reach yet, which is why `CLOUD=gcp` is refused today.
