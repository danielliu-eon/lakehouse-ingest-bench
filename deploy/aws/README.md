# Running the benchmark on AWS

`setup.sh` builds everything on an AWS account that a run needs and no run
creates for itself. `teardown.sh` removes it again. Both are idempotent: every
step describes before it creates or deletes, so a re-run after a timeout or a
revoked token converges instead of failing.

Neither script creates, deletes or reconfigures the EKS cluster. That is yours.

## Prerequisites

- **An EKS cluster** with at least one **amd64** node. The Flink image is
  amd64-only, because PyFlink publishes no aarch64 wheel, and the preflight
  refuses a cluster without one. If you have no cluster,
  `eksctl-cluster.example.yaml` makes a minimal one — see the last section.
- **`aws` CLI v2**, **`kubectl`**, **`helm`**, **`jq`** and **`envsubst`** (from
  GNU gettext) on the machine you run these from.
- Credentials for the account the cluster is in, with permission to create S3
  buckets, ECR repositories, MSK clusters, security groups, IAM roles and EKS
  add-ons and pod identity associations.

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

```bash
export AWS_REGION=... CLUSTER_NAME=...
deploy/aws/setup.sh
```

## What `setup.sh` creates

Preflight first, and each refusal names its fix: the caller's identity, the
cluster, `kubectl` reaching it, an amd64 node, the `eks-pod-identity-agent`
add-on (installed and waited for if absent) and the
`flinkdeployments.flink.apache.org` CRD (the operator is installed with
`webhook.create=false` if absent, so no cert-manager is needed, and its chart
version is printed either way). Then:

- **S3** — the bucket, with public access blocked, versioning left off and the
  `lakehouse-ingest-bench` tag.
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
  named by `spark.jobNamespaces`, which is what makes this one eligible. With
  `WITH_SCHEMA_REGISTRY=true`, also a `schema-registry` Deployment and Service
  (Apicurio, in-memory storage), waited on until its rollout completes.

It ends by printing the values to fill into `site.yaml` (copy
`site.aws.example.yaml`), the IAM bootstrap string among them.

With it filled in, `scripts/push-images.sh` builds and pushes the three images, and
`scripts/gen-corpus.sh <preset>` builds a corpus into the bucket as a Job.

> **MSK bills by the hour whether or not a run is using it,** and reaching
> `ACTIVE` takes 15 to 30 minutes. Two `kafka.m5.large` brokers with 100 GiB
> each are a few dollars a day. Tear it down between campaigns; `setup.sh`
> recreates it, and a re-run against an existing cluster changes nothing.

## `teardown.sh`

```bash
deploy/aws/teardown.sh          # the namespace, every association, the role, MSK and its security group
deploy/aws/teardown.sh --all    # also the bucket and its contents, the ECR repositories and both operators
```

In that order, because each deletion needs the one before it: the namespace
goes first so no pod is still using MSK or the role, and the security group last
because MSK's network interfaces hold it for a few minutes after the cluster
goes. Without `--all` the bucket, the images and the operators stay — a corpus
is expensive to rebuild, images are slow to push, and an operator may be
shared.
The `eks-pod-identity-agent` add-on is always left installed: it is free, and it
is a property of the cluster rather than of this benchmark.

## Templating

Two conventions, on purpose. Files here — the IAM documents under `iam/` and
`k8s/namespace.yaml.tmpl` — use `${NAME}` and are rendered by these bash scripts
with `envsubst`. The Kubernetes templates the harness renders in Python use
`__NAME__` instead, so a manifest carrying shell or Helm syntax of its own is
never touched by the wrong renderer.

## If you have no cluster

`eksctl-cluster.example.yaml` creates a minimal one: three `m6i.xlarge` amd64
nodes in private subnets, the Pod Identity agent, and its own VPC across three
zones, where `setup.sh` also puts the MSK brokers. Replace both placeholders.

```bash
eksctl create cluster -f deploy/aws/eksctl-cluster.example.yaml   # about 20 minutes
eksctl delete cluster -f deploy/aws/eksctl-cluster.example.yaml
```

Neither script above runs `eksctl`, and neither deletes what it made.
