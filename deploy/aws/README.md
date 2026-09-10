# Running the benchmark on AWS

`setup.sh` provisions the shared AWS resources needed by benchmark runs.
`teardown.sh` removes them. Both check existing resources, so you can rerun an
interrupted operation. Manage the EKS cluster separately; neither script creates,
reconfigures, or deletes it.

## Prerequisites

- **EKS cluster.** Build the benchmark images for the cluster's node
  architecture. For a new cluster, use the [EKS example](#create-an-eks-cluster).
- **Host tools.** Scripts check required tools before proceeding. `run.sh`
  checks the tools needed by all five drivers it invokes.

  | Tool | Needed by |
  |---|---|
  | `aws` CLI v2 | setup, teardown, and all run drivers except `launch.sh` |
  | `kubectl` | setup, teardown, and run drivers that access Kubernetes; `finish.sh` also requires it for catalog tunnels |
  | `helm` | `setup.sh` / `teardown.sh`, for the two operators |
  | `envsubst` (GNU gettext) | `setup.sh`, for the IAM and namespace templates |
  | `yq` (mikefarah v4) | every driver, to read the site configuration and copied run spec |
  | `jq` | every driver that reads a run's facts or an object's status |
  | `git` | drivers that resolve image tags from the Git commit |
  | `curl` | `stage.sh` for engine verification; catalog tunnels also probe readiness with it |
  | `docker` | `push-images.sh` |
  | `gzip` | `teardown.sh` and `purge.sh`, to read compressed table metadata |
- **AWS credentials.** Use an identity in the cluster's account with permission
  to manage S3, ECR, MSK, security groups, IAM roles, EKS add-ons, and Pod Identity
  associations. Scripts use the AWS CLI's ambient credentials without setting
  a profile. Export `AWS_PROFILE` or sign in with
  `aws sso login --profile <name>` or `aws login`, then verify the identity with
  `aws sts get-caller-identity` in each shell. Setup and teardown check credentials
  immediately; other drivers report credential failures on their first AWS call.

## Sizing the cluster

Kubernetes schedules pods by resource requests. Budget for these requests:

| Pod | How many | CPU | Memory |
|---|---|---|---|
| Scorer | one per run | `2` | `2Gi` |
| Producer shard | `producer.shards` | `2` | `PRODUCER_MEMORY` (`2Gi`) |
| Corpus generator | `gen-corpus.sh --shards` | `1` | `GEN_MEMORY` (`2Gi`) |
| Staging and other harness Jobs | one at a time | `500m` | `1Gi` |
| Schema registry | one when `WITH_SCHEMA_REGISTRY=true` | `200m` | `512Mi` |
| Flink jobmanager | one | `jm_cpu` (`1`) | `jm_mem_mb` |
| Flink taskmanager | `taskmanagers` | `tm_cpu` | `tm_mem_mb` |
| Spark driver | one | `driver_cores` (`1`) | `driver_mem_mb` |
| Spark executor | `executors` | `executor_cores` | `executor_mem_mb` |

Harness requests come from `deploy/k8s/` templates. Engine requests come from
run knobs, rendered by [Flink](../../engines/flink/knobs.py) and
[Spark](../../engines/spark/knobs.py).

A node fits `floor((allocatable CPU − DaemonSet requests) / 2)` two-CPU pods.
A four-vCPU node has less than four allocatable cores, so it fits only one.
At the shipped sizes, CPU generally limits placement before memory. See
[pitfalls](../../docs/pitfalls.md) for the scheduling consequences.

Count the engine fleet, scorer, and producer shards together. Corpus generation
and the staging Job finish before that peak. For four-vCPU nodes:

- `aws-smoke-flink.yaml` needs **four nodes**: two TaskManagers, one producer,
  and one scorer, with the one-CPU JobManager sharing a node.
  `aws-smoke-spark.yaml` has the same requirement.
- `aws-100mbs-skew-flink-hash.yaml` needs **14 nodes**: eight TaskManagers,
  five producers, and one scorer. Its JobManager can share a node.
- `aws-100mbs-skew-spark-hash.yaml` needs **15 nodes** because its two-CPU
  driver also needs a node.

Raise the node group's maximum as well as its desired size:

```bash
eksctl scale nodegroup --cluster <name> --name amd64 --nodes 14 --nodes-max 14
```

`launch.sh` warns if too few nodes have two free CPUs for the scorer and
producers. The engine fleet is already running at this point. The warning does
not block launch because Pending pods can trigger a cluster autoscaler.

## Configure the environment

Pass site-specific values through environment variables:

| Variable | Default | What it is |
|---|---|---|
| `AWS_REGION` | *required* | The region the EKS cluster is in |
| `CLUSTER_NAME` | *required* | The EKS cluster's name |
| `KUBE_CONTEXT` | `$CLUSTER_NAME` | The kubeconfig context. Written with `aws eks update-kubeconfig --alias` if it is missing |
| `BUCKET` | `lakehouse-ingest-bench-<account id>` | Bucket for `corpus/`, `runs/`, and `warehouse/`; the account ID helps avoid global name collisions |
| `SITE_FILE` | `./site.yaml` | Site configuration read by setup and teardown; `--site PATH` overrides it |
| `MSK_NAME` | `lakehouse-ingest-bench` | MSK cluster name; its security group is `<name>-msk` |
| `MSK_BROKER_TYPE` | `kafka.m5.large` | Broker instance type |
| `MSK_BROKERS` | `2` | Broker count; requires this many availability zones with private subnets |
| `MSK_KAFKA_VERSION` | newest `ACTIVE` `3.x` | Kafka version; setup prints the selected version |
| `MSK_VOLUME_GIB` | `100` | EBS GiB per broker. Increase for longer offers or larger backlogs. Existing volumes can grow with `update-broker-storage`, but cannot shrink |
| `FLINK_OPERATOR_VERSION` | `1.15.0` | Flink operator chart installed from `archive.apache.org` when its CRD is absent |
| `SPARK_OPERATOR_VERSION` | `2.5.2` | Kubeflow operator chart installed when its CRD is absent; configured to watch `$NAMESPACE` and use the benchmark's Spark identity |
| `NAMESPACE` | `ingest-bench` | Namespace shared by harness Jobs and engine runs |
| `WITH_SCHEMA_REGISTRY` | `false` | Create an in-memory Apicurio registry for Confluent runs at `http://schema-registry.<namespace>.svc:8080/apis/ccompat/v7`; removed with the namespace |
| `NODE_SELECTOR` / `TOLERATIONS` | `{}` / `[]` | One-line JSON for registry placement. Harness Jobs read placement from `site.yaml` |
| `MSK_ACTIVE_WAIT_S` | `3600` | How long `setup.sh` waits for MSK to reach `ACTIVE` |
| `MSK_DELETED_WAIT_S` | `1800` | How long `teardown.sh` waits for MSK to disappear before deleting its security group |

## Provision shared resources

Setup first checks credentials, cluster access, and node architectures. It
installs the Pod Identity agent if absent and waits for it to become active.
It also installs the Flink operator when its CRD is absent, disabling its
validating webhook so cert-manager is not required.

Setup then provisions:

- **S3:** a bucket with public access blocked and the benchmark ownership tag.
  It suspends enabled versioning to avoid retaining billable deleted corpus
  data. Existing buckets must carry `lakehouse-ingest-bench=true`; setup will
  not overwrite tags or change versioning on an unrelated bucket.
- **ECR:** the `lakehouse-ingest-bench/harness`, `lakehouse-ingest-bench/flink`,
  and `lakehouse-ingest-bench/spark` repositories.
- **MSK (only for `kafka.deployment: managed`):** a provisioned cluster in the
  EKS VPC's private subnets, with one broker per availability zone, IAM
  authentication, and TLS. Its security group allows port 9098 from all VPC
  CIDRs, covering node and pod ranges; IAM controls client authorization.
- **IAM:** the `lakehouse-ingest-bench-harness` role, trusted by
  `pods.eks.amazonaws.com` for `sts:AssumeRole` and `sts:TagSession`. Its inline
  policy covers the bucket's three prefixes, the `ingest_bench` Glue database
  except for in-cluster sites, and, for managed Kafka, the MSK cluster's topics
  and consumer groups. Pod Identity binds unassociated benchmark service
  accounts to the role; existing bindings remain unchanged.
- **Kubernetes:** the namespace, harness/Flink/Spark service accounts, and
  engine RBAC. The Spark operator is installed after the namespace because its
  chart creates a Role there. It watches `spark.jobNamespaces={$NAMESPACE}`
  and uses the benchmark Spark identity. Its webhook must be enabled to mount
  run ConfigMaps in driver and executor pods. Setup does not repair an existing
  Spark operator installation whose webhook is disabled.
- **Optional registry:** with `WITH_SCHEMA_REGISTRY=true`, an in-memory
  Apicurio Deployment and Service, followed by a rollout wait.

Setup and teardown read `kafka.deployment` from `--site PATH`, defaulting to
`SITE_FILE` and then `./site.yaml`, before contacting AWS. The value is required
and must be `managed`, `in-cluster`, or `external`. Only `managed` permits MSK
and its security-group operations. The other modes omit MSK permissions and
readiness waits. Environment variables cannot override this choice.

`--write-site PATH` writes a new configuration and refuses to overwrite an
existing file. On first setup, use the matching example as input. Generated
managed sites use MSK and Glue; in-cluster sites use the Kafka and Lakekeeper
service addresses. External sites preserve the input Kafka and catalog blocks;
replace all `YOUR_` placeholders in those blocks before using `--write-site`.
Generated prices remain zero until you fill them in.

```bash
export AWS_REGION=... CLUSTER_NAME=...
deploy/aws/setup.sh --site site.aws.example.yaml --write-site site.yaml
scripts/push-images.sh                    # harness and engine images, tagged with this commit
scripts/gen-corpus.sh smoke --shards 4     # generate a corpus in the bucket
```

Fill in `site.yaml` pricing before publishing results; validation rejects zero
prices. Without `--write-site`, update your input site using the printed values.

`push-images.sh` builds all images for `linux/amd64` by default. Use
`--platform linux/arm64` or a comma-separated platform list to change this.
Image tags identify commits, so the script rejects uncommitted changes unless
`--allow-dirty` is set.

Choose corpus presets and shard counts using the [corpus guide](../../docs/corpus.md),
then follow the [per-run workflow](../../docs/running.md).

MSK continues billing while idle and can take 15–30 minutes to become active.
Check regional pricing before a campaign and tear it down between campaigns.
Rerunning setup reuses the cluster and can grow broker storage when requested.

## Remove shared resources

```bash
deploy/aws/teardown.sh              # namespace and identity; MSK when managed
deploy/aws/teardown.sh --all        # also ECR, both operators, and the bucket
deploy/aws/teardown.sh --all --yes  # same cleanup without the bucket prompt
```

Teardown stops before contacting AWS if the site is missing or lacks a valid
`kafka.deployment`. If the original file is unavailable, select the example
matching the deployment you are removing:

```bash
deploy/aws/teardown.sh --site site.aws.example.yaml  # managed Kafka: includes MSK
deploy/aws/teardown.sh --site site.k8s.example.yaml  # in-cluster Kafka: skips MSK
```

Run only the matching command. Teardown reads only the deployment mode from the
site; keep `AWS_REGION`, `CLUSTER_NAME`, and any resource-name overrides set to
their original values. For external Kafka, use a file containing
`kafka: {deployment: external}`. No completed site configuration is required.

Cleanup follows dependency order: stop workloads and remove their identity.
For managed Kafka, delete MSK, then remove its security group after network
interfaces release it.
Without `--all`, the bucket, images, and operators remain available for another
campaign or other workloads.

With `--all`, bucket deletion happens last and requires the benchmark ownership
tag plus confirmation. It removes every corpus, run artifact, and warehouse
object. `--yes` supplies confirmation for unattended use. By the time the prompt
appears, the images and operators have already been removed. Declining keeps
the bucket but does not restore those resources.

The EKS cluster and `eks-pod-identity-agent` add-on remain installed.

## Template conventions

AWS IAM documents and `k8s/namespace.yaml.tmpl` use `${NAME}` placeholders,
rendered by Bash with `envsubst`. Harness Kubernetes templates use `__NAME__`
markers, rendered in Python. Separate delimiters keep embedded shell or Helm
syntax intact.

## Create an EKS cluster

`eksctl-cluster.example.yaml` creates four `m6i.xlarge` amd64 nodes in private
subnets, the Pod Identity agent, and a VPC across three availability zones.
This fits the smoke workload described above. Replace both placeholders first.

```bash
eksctl create cluster -f deploy/aws/eksctl-cluster.example.yaml   # about 20 minutes
eksctl delete cluster -f deploy/aws/eksctl-cluster.example.yaml
```

Run these commands yourself; setup and teardown do not invoke `eksctl`.

## In-cluster AWS resources

The [in-cluster stack](../k8s/stack/README.md) uses Kafka and Lakekeeper in
place of MSK and Glue. Its AWS hooks create a bucket-scoped
`lakehouse-ingest-bench-stack` role for the catalog and any run service accounts
that are not already associated. They also create separate gp3 StorageClasses
for Kafka and catalog Postgres and require the EBS CSI driver.

Use `eksctl-kafka-nodegroup.example.yaml` for dedicated broker nodes. The AWS
setup supplies the bucket, image registry, and engine operators without MSK:

```bash
deploy/aws/setup.sh --site site.k8s.example.yaml --write-site site.yaml
CLOUD=aws deploy/k8s/stack/setup.sh
```

The generated Kafka and Lakekeeper addresses match the stack defaults. Use the
same namespace in both steps so the Spark operator watches the run namespace.
Both teardown scripts read the same site; stack commands require
`kafka.deployment: in-cluster`.

An existing MSK cluster is not removed when the site selects `in-cluster`. If an
earlier setup created one that no run uses, delete the cluster named by
`MSK_NAME`, wait for its network interfaces to disappear, then delete its tagged
`<MSK_NAME>-msk` security group. Rerunning shared setup with the in-cluster site
removes the `MskCluster`, `MskTopics`, `MskGroups`, and `GlueIcebergCatalog`
statements from the harness role. Do not use shared teardown for selective MSK
cleanup: it also removes the run namespace and identities.
