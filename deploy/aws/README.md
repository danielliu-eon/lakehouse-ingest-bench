# Running the benchmark on AWS

`setup.sh` builds everything on an AWS account that a run needs and no run
creates for itself. `teardown.sh` removes it again. Both are idempotent: every
step describes before it creates or deletes, so a re-run after a timeout or a
revoked token converges instead of failing.

Neither script creates, deletes or reconfigures the EKS cluster. That is yours.

## Prerequisites

- **An EKS cluster**, with at least one **amd64** node to run Flink on. The
  Flink image is amd64-only, because PyFlink publishes no aarch64 wheel, and
  the preflight warns about a cluster without one rather than refusing it — a
  Spark-only campaign needs no amd64 node. If you have no cluster,
  `eksctl-cluster.example.yaml` makes a minimal one — see the last section.
- **Host tools**, on the machine you run all of this from. Every script here and
  every cluster driver under `scripts/` refuses up front on a missing one, and
  points at this list. `run.sh` chains five of those drivers, so it refuses on
  any tool one of them needs:

  | Tool | Needed by |
  |---|---|
  | `aws` CLI v2 | these two scripts, and every driver that reads the bucket or the registry — all of them but `launch.sh` |
  | `kubectl` | these two scripts, and every driver that applies or reads a Kubernetes object. `gate.sh` and `finish.sh` read only the bucket, so they need none |
  | `helm` | `setup.sh` / `teardown.sh`, for the two operators |
  | `envsubst` (GNU gettext) | `setup.sh`, for the IAM and namespace templates |
  | `yq` (mikefarah v4) | every driver, to read the site and the copied spec |
  | `jq` | every driver that reads a run's facts or an object's status |
  | `git` | every driver that names an image, since the tag is a commit |
  | `curl` | `stage.sh`, to read a running engine through a port-forward |
  | `docker` | `push-images.sh` |
  | `gzip` | `teardown.sh` and `purge.sh`, since a table may write its metadata document compressed |
- Credentials for the account the cluster is in, with permission to create S3
  buckets, ECR repositories, MSK clusters, security groups, IAM roles and EKS
  add-ons and pod identity associations.

## Sizing the cluster

A run's pods are scheduled on their CPU requests, and most of them ask for two
cores. What each asks for:

| Pod | How many | CPU | Memory |
|---|---|---|---|
| Scorer | one per run | `2` | `2Gi` |
| Producer shard | `producer.shards` | `2` | `PRODUCER_MEMORY` (`2Gi`) |
| Corpus generator | `gen-corpus.sh --shards` | `1` | `GEN_MEMORY` (`2Gi`) |
| Stage, and every other harness Job | one at a time | `500m` | `1Gi` |
| Schema registry | one, under `WITH_SCHEMA_REGISTRY=true` | `200m` | `512Mi` |
| Flink jobmanager | one | `jm_cpu` (`1`) | `jm_mem_mb` |
| Flink taskmanager | `taskmanagers` | `tm_cpu` | `tm_mem_mb` |
| Spark driver | one | `driver_cores` (`1`) | `driver_mem_mb` |
| Spark executor | `executors` | `executor_cores` | `executor_mem_mb` |

The first five are the templates under `deploy/k8s/`; the engines' four are
knobs their spec sets, rendered into a `FlinkDeployment` by
[`engines/flink/knobs.py`](../../engines/flink/knobs.py) and a
`SparkApplication` by [`engines/spark/knobs.py`](../../engines/spark/knobs.py).

A node takes `floor((allocatable − daemonsets) / 2)` of the 2-CPU pods among
them, and on a 4-vCPU node that is one: allocatable is already under 4 CPU
before a daemonset has asked for anything, so no two of them fit however little
the rest holds. CPU is what runs out first at these sizes — the largest memory
request any shipped spec makes is `executor_mem_mb: 8192`, on a node with 16
GiB. What ignoring the rule costs a run is
[`docs/pitfalls.md`](../../docs/pitfalls.md) §One 2-CPU pod per small node.

Count the pods that are up together, which is the engine's fleet, the scorer and
the shards: a corpus is generated before a run, and the stage Job has finished
before `stage.sh` applies the engine's documents.

- `aws-smoke-flink.yaml` — two taskmanagers at `tm_cpu: 2`, one shard and the
  scorer are four 2-CPU pods, so **four nodes**, with the 1-CPU jobmanager
  beside one of them. `aws-smoke-spark.yaml` counts the same way, its driver
  defaulting to one core.
- `aws-100mbs-skew-flink-hash.yaml` — eight taskmanagers, five shards and the
  scorer: **14 nodes**, the jobmanager again beside a taskmanager.
  `aws-100mbs-skew-spark-hash.yaml` sets `driver_cores: 2`, which is a node of
  its own: **15**.

Grow a node group by raising its maximum along with its size, since the maximum
is what caps it:

```bash
eksctl scale nodegroup --cluster <name> --name amd64 --nodes 14 --nodes-max 14
```

`launch.sh` counts the nodes with 2 CPU free before it applies anything and
warns when there are fewer than the scorer and the shards need — the fleet is
already running by then, so it is not in that count. It warns and never refuses:
on an autoscaled cluster, the Pending pod is what buys the node.

## Environment

Everything site-specific reaches the scripts through the environment; nothing in
this repository names an account, a region, a cluster or a bucket.

| Variable | Default | What it is |
|---|---|---|
| `AWS_REGION` | *required* | The region the EKS cluster is in |
| `CLUSTER_NAME` | *required* | The EKS cluster's name |
| `KUBE_CONTEXT` | `$CLUSTER_NAME` | The kubeconfig context. Written with `aws eks update-kubeconfig --alias` if it is missing |
| `BUCKET` | `lakehouse-ingest-bench-<account id>` | The one bucket, with prefixes `corpus/`, `runs/` and `warehouse/`. Bucket names are global, hence the account id in the default |
| `MSK_NAME` | `lakehouse-ingest-bench` | The MSK cluster's name; its security group is `<name>-msk` |
| `MSK_BROKER_TYPE` | `kafka.m5.large` | Broker instance type |
| `MSK_BROKERS` | `2` | Broker count. One broker per availability zone, so the VPC needs a private subnet in this many zones |
| `MSK_KAFKA_VERSION` | newest `ACTIVE` `3.x` | Kafka version, printed either way |
| `MSK_VOLUME_GIB` | `100` | EBS GiB per broker. An offer sits on the brokers for as long as the engine is behind, so an hour run needs more than a smoke does. Raised on an existing cluster with `update-broker-storage`; never lowered, because a broker volume cannot shrink |
| `FLINK_OPERATOR_VERSION` | `1.15.0` | The operator chart installed when the CRD is absent, from `archive.apache.org` — it keeps every release, where the download mirror serves only current ones |
| `SPARK_OPERATOR_VERSION` | `2.5.2` | The Kubeflow spark-operator chart installed when the `sparkapplications` CRD is absent, from `https://kubeflow.github.io/spark-operator`. Installed with `spark.jobNamespaces={$NAMESPACE}` so the controller watches this namespace, and with its own spark ServiceAccount and RBAC off — a run's driver runs as `ingest-bench-spark`, which is the name Pod Identity is bound to |
| `NAMESPACE` | `ingest-bench` | The Kubernetes namespace the harness Jobs and both engines' runs live in |
| `WITH_SCHEMA_REGISTRY` | `false` | `true` also applies `deploy/k8s/schema-registry.yaml.tmpl` — one Apicurio Deployment and Service in the namespace, at `http://schema-registry.<namespace>.svc:8080/apis/ccompat/v7`. Only a run whose spec says `kafka.value_encoding: confluent` needs one; the namespace delete in `teardown.sh` removes it |
| `NODE_SELECTOR` / `TOLERATIONS` | `{}` / `[]` | One-line JSON placing the registry Deployment, for a cluster whose nodes are labelled or tainted. The harness Jobs read the same two values out of `site.yaml` instead |
| `MSK_ACTIVE_WAIT_S` | `3600` | How long `setup.sh` waits for MSK to reach `ACTIVE` |
| `MSK_DELETED_WAIT_S` | `1800` | How long `teardown.sh` waits for MSK to disappear before deleting its security group |

## What `setup.sh` creates

Preflight first, and each refusal names its fix: the caller's identity, the
cluster, `kubectl` reaching it, an amd64 node (a warning, not a refusal), the
`eks-pod-identity-agent` add-on (installed and waited for if absent) and the
`flinkdeployments.flink.apache.org` CRD (the operator is installed with
`webhook.create=false` if absent, so no cert-manager is needed, and its chart
version is printed either way). Then:

- **S3** — the bucket, with public access blocked, the `lakehouse-ingest-bench`
  tag, and versioning **suspended if it was on** — a corpus is regenerated
  rather than restored, and every deleted object of a hundred-gigabyte corpus
  would otherwise keep being billed. Neither call is additive, so a bucket that
  already exists and carries no tag of ours is refused rather than
  reconfigured: name one of your own with `BUCKET`, or tag that one
  `lakehouse-ingest-bench=true` if it is meant to be this benchmark's.
- **ECR** — `lakehouse-ingest-bench/harness`, `lakehouse-ingest-bench/flink` and
  `lakehouse-ingest-bench/spark`.
- **MSK** — a provisioned cluster, IAM its only client authentication and no
  unauthenticated listener, TLS in transit, `MSK_VOLUME_GIB` per broker, brokers in the
  EKS cluster's own private subnets one per availability zone. Its security
  group opens 9098 to every CIDR the VPC has: IAM decides who may connect, the
  group only scopes the network, and a CIDR rule reaches every node in the VPC
  where one naming the cluster's own group reaches only those carrying it.
- **IAM** — one role, `lakehouse-ingest-bench-harness`, trusted by
  `pods.eks.amazonaws.com` for `sts:AssumeRole` and `sts:TagSession`, with an
  inline policy over the bucket's three prefixes, the `ingest_bench` Glue
  database, and this MSK cluster's topics and consumer groups. Pod identity
  associations bind it to all three ServiceAccounts.
- **Kubernetes** — the namespace, the `ingest-bench-harness`,
  `ingest-bench-flink` and `ingest-bench-spark` ServiceAccounts, and the Role
  and RoleBinding each engine needs to raise its own fleet: a JobManager
  creates its TaskManagers, and a Spark driver creates its executors. Then the
  **Kubeflow spark-operator**, installed when its CRD is absent — after the
  namespace, because its chart grants the controller a Role in each namespace
  named by `spark.jobNamespaces`, which is what makes this one eligible. It is
  installed with `webhook.enable=true`, because the webhook is what grafts a
  run's ConfigMap volume onto the driver and executor pods — a `SparkApplication`
  carries the volume and the CRD alone does not apply it, so a driver on an
  install without it starts with no `/opt/bench/run` and dies opening the run's
  job document. A cluster that already had the operator installed *without* the
  webhook is the one shape this preflight cannot fix for you.
  With `WITH_SCHEMA_REGISTRY=true`, also a `schema-registry`
  Deployment and Service (Apicurio, in-memory storage), waited on until its
  rollout completes.

It ends by printing the values to fill into `site.yaml` (copy
`site.aws.example.yaml`), the IAM bootstrap string among them.

## Once per account

```bash
export AWS_REGION=... CLUSTER_NAME=...
deploy/aws/setup.sh                        # bucket, ECR, MSK, IAM, namespace, operators
cp site.aws.example.yaml site.yaml         # setup.sh prints every value to fill in
scripts/push-images.sh                     # harness and both engine images, tagged with this commit
scripts/gen-corpus.sh smoke --shards 4     # a corpus in the bucket, as a Job
```

`push-images.sh` builds the harness and Spark images for `linux/amd64` by
default; `--platform linux/arm64`, or two comma-separated platforms for a
manifest list, builds for something else. The Flink image is amd64 whatever is
passed. It refuses an uncommitted tree, because the tag is the commit and would
then name something that is not in the image — `--allow-dirty` overrides that.

`gen-corpus.sh <preset> --shards N` generates in N pods and merges them; see
[`../../docs/corpus.md`](../../docs/corpus.md) for choosing a preset and a shard
count. Then run a benchmark: [`../../docs/running.md`](../../docs/running.md) is
the per-run driver sequence.

> **MSK bills by the hour whether or not a run is using it,** and reaching
> `ACTIVE` takes 15 to 30 minutes. The default two `kafka.m5.large` brokers with
> 100 GiB each cost a few dollars a day — check the current MSK price for your
> region before a long campaign. Tear it down between campaigns; `setup.sh`
> recreates it, and a re-run against an existing cluster changes nothing.

## `teardown.sh`

```bash
deploy/aws/teardown.sh          # the namespace, every association, the role, MSK and its security group
deploy/aws/teardown.sh --all    # also the ECR repositories, both operators and the bucket
deploy/aws/teardown.sh --all --yes   # the same, unattended
```

In that order, because each deletion needs the one before it: the namespace
goes first so no pod is still using MSK or the role, and the security group last
because MSK's network interfaces hold it for a few minutes after the cluster
goes. Without `--all` the bucket, the images and the operators stay — a corpus
is expensive to rebuild, images are slow to push, and an operator may be
shared.

Under `--all` the bucket goes **last, and only after it is asked about**: it is
every corpus, every run's artifacts and the warehouse, so it is matched on the
tag the way the security group is — a bucket of that name this benchmark did not
create is refused — and named before it is emptied. `--yes` answers the prompt
for a teardown run from a script. A refusal there leaves the images and the
operators already gone rather than a teardown to run again.
The `eks-pod-identity-agent` add-on is always left installed: it is free, and it
is a property of the cluster rather than of this benchmark.

## Templating

Two conventions, on purpose. Files here — the IAM documents under `iam/` and
`k8s/namespace.yaml.tmpl` — use `${NAME}` and are rendered by these bash scripts
with `envsubst`. The Kubernetes templates the harness renders in Python use
`__NAME__` instead, so a manifest carrying shell or Helm syntax of its own is
never touched by the wrong renderer.

## If you have no cluster

`eksctl-cluster.example.yaml` creates a minimal one: four `m6i.xlarge` amd64
nodes in private subnets — a smoke spec's peak, by the count above — the Pod
Identity agent, and its own VPC across three zones, where `setup.sh` also puts
the MSK brokers. Replace both placeholders.

```bash
eksctl create cluster -f deploy/aws/eksctl-cluster.example.yaml   # about 20 minutes
eksctl delete cluster -f deploy/aws/eksctl-cluster.example.yaml
```

Neither script above runs `eksctl`, and neither deletes what it made.
