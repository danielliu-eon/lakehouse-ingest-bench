#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Provision shared AWS resources: S3, ECR, MSK, IAM, and Kubernetes identities.
# Check existing resources before creation so interrupted setup can be rerun.
# The EKS cluster is managed separately; see eksctl-cluster.example.yaml.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
AWS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_lib.sh
source "$AWS_DIR/../../scripts/_lib.sh"
# shellcheck source=deploy/aws/_resources.sh
source "$AWS_DIR/_resources.sh"

usage() {
	cat <<'USAGE'
usage: deploy/aws/setup.sh [--write-site PATH]

  --write-site PATH   write site.yaml to PATH using the printed settings;
                      fail if PATH already exists

Configure this script through environment variables. AWS_REGION and
CLUSTER_NAME are required. See Environment in deploy/aws/README.md for
all variables and defaults.
USAGE
}

WRITE_SITE=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--write-site)
		WRITE_SITE="${2:?--write-site needs a path}"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		printf 'unknown argument %s\n\n' "$1" >&2
		usage >&2
		exit 2
		;;
	esac
done

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Export the region once for all AWS CLI calls.
export AWS_REGION="${AWS_REGION:?AWS_REGION must name the region the EKS cluster is in}"
CLUSTER_NAME="${CLUSTER_NAME:?CLUSTER_NAME must name an existing EKS cluster}"
KUBE_CONTEXT="${KUBE_CONTEXT:-$CLUSTER_NAME}"
MSK_NAME="${MSK_NAME:-lakehouse-ingest-bench}"
NAMESPACE="${NAMESPACE:-ingest-bench}"
MSK_BROKER_TYPE="${MSK_BROKER_TYPE:-kafka.m5.large}"
MSK_BROKERS="${MSK_BROKERS:-2}"
# Create an optional registry for Confluent runs. External registries are
# configured in site.yaml instead.
WITH_SCHEMA_REGISTRY="${WITH_SCHEMA_REGISTRY:-false}"
# Registry placement as one-line JSON, matching site.kubernetes conventions.
NODE_SELECTOR="${NODE_SELECTOR:-}"
TOLERATIONS="${TOLERATIONS:-[]}"
# Escape the brace in the empty-object default so it does not close ${...}.
if [[ -z $NODE_SELECTOR ]]; then
	NODE_SELECTOR='{}'
fi
# Bound the MSK wait; rerunning setup resumes it without recreating the cluster.
MSK_ACTIVE_WAIT_S="${MSK_ACTIVE_WAIT_S:-3600}"

# Shared resource tag for ownership checks and cost reporting.
TAG_KEY=lakehouse-ingest-bench

# Storage per broker. Larger offers need room for records the engine has
# not yet consumed.
MSK_VOLUME_GIB="${MSK_VOLUME_GIB:-100}"
# MSK IAM listener; IAM authorizes clients and the security group scopes access.
MSK_IAM_PORT=9098

# Pin a CRD version compatible with the rendered FlinkDeployment fields.
# Operator 1.15 supports the engine's v1_20 label.
FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.15.0}"
FLINK_OPERATOR_RELEASE=flink-kubernetes-operator
FLINK_OPERATOR_NAMESPACE=flink-operator

ROLE_NAME=lakehouse-ingest-bench-harness
# Use an inline policy scoped to this bucket and MSK cluster.
POLICY_NAME=lakehouse-ingest-bench-harness
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink
SPARK_SERVICE_ACCOUNT=ingest-bench-spark

ECR_REPOSITORIES="lakehouse-ingest-bench/harness lakehouse-ingest-bench/flink lakehouse-ingest-bench/spark"

[[ $MSK_BROKERS =~ ^[1-9][0-9]*$ ]] || die "MSK_BROKERS must be a positive integer, got '$MSK_BROKERS'"
[[ $MSK_VOLUME_GIB =~ ^[1-9][0-9]*$ ]] || die "MSK_VOLUME_GIB must be a positive integer, got '$MSK_VOLUME_GIB'"
# Reject an existing output path before the potentially long MSK wait.
[[ -z $WRITE_SITE || ! -e $WRITE_SITE ]] ||
	die "$WRITE_SITE already exists; --write-site cannot overwrite it. Choose another path or move the existing file"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

require_host_tools aws kubectl helm jq envsubst
# Only --write-site needs yq to validate the generated configuration.
[[ -z $WRITE_SITE ]] || require_host_tools yq

if ! ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>&1)"; then
	die "aws sts get-caller-identity failed: $ACCOUNT — sign in first (aws configure, or aws sso login --profile ...)"
fi
log "account $ACCOUNT, region $AWS_REGION"

# Include the account ID to reduce collisions in S3's global bucket namespace.
BUCKET="${BUCKET:-lakehouse-ingest-bench-$ACCOUNT}"
log "bucket s3://$BUCKET"

if ! CLUSTER_JSON="$(aws eks describe-cluster --name "$CLUSTER_NAME" --output json 2>&1)"; then
	die "aws eks describe-cluster --name $CLUSTER_NAME failed in $AWS_REGION: $CLUSTER_JSON
     Check CLUSTER_NAME and AWS_REGION, or create a cluster with deploy/aws/eksctl-cluster.example.yaml"
fi
VPC_ID="$(jq -r '.cluster.resourcesVpcConfig.vpcId' <<<"$CLUSTER_JSON")"
log "cluster $CLUSTER_NAME in $VPC_ID"

if ! kubectl config get-contexts -o name 2>/dev/null | grep -qxF "$KUBE_CONTEXT"; then
	log "no kubeconfig context $KUBE_CONTEXT; writing one"
	aws eks update-kubeconfig --name "$CLUSTER_NAME" --alias "$KUBE_CONTEXT" >/dev/null
fi

if ! NODES_JSON="$(kubectl --context "$KUBE_CONTEXT" get nodes -o json 2>&1)"; then
	die "kubectl --context $KUBE_CONTEXT cannot reach $CLUSTER_NAME: $NODES_JSON
     Your principal needs an EKS access entry (or an aws-auth mapping) on the cluster."
fi
ARCHITECTURES="$(jq -r '[.items[].status.nodeInfo.architecture] | unique | join(" ")' <<<"$NODES_JSON")"
# Warn if Flink's required amd64 nodes are absent. Spark-only campaigns can
# use an arm64 cluster with matching harness and Spark images.
grep -qw amd64 <<<"$ARCHITECTURES" ||
	log "warning: $CLUSTER_NAME has no amd64 nodes (found: ${ARCHITECTURES:-none}); Flink requires amd64. Spark runs can proceed.
     Add an amd64 node group — deploy/aws/eksctl-cluster.example.yaml has one."
log "node architectures: $ARCHITECTURES"

if aws eks describe-addon --cluster-name "$CLUSTER_NAME" --addon-name eks-pod-identity-agent >/dev/null 2>&1; then
	log "eks-pod-identity-agent add-on is installed"
else
	log "installing the eks-pod-identity-agent add-on"
	aws eks create-addon --cluster-name "$CLUSTER_NAME" --addon-name eks-pod-identity-agent >/dev/null
fi
# Wait even for an existing add-on: pods need it ACTIVE to receive credentials.
aws eks wait addon-active --cluster-name "$CLUSTER_NAME" --addon-name eks-pod-identity-agent

if kubectl --context "$KUBE_CONTEXT" get crd flinkdeployments.flink.apache.org >/dev/null 2>&1; then
	log "the flinkdeployments CRD is present"
else
	log "installing the Flink Kubernetes Operator $FLINK_OPERATOR_VERSION"
	# Use the archive so pinned charts remain available after newer releases.
	helm repo add flink-operator-repo \
		"https://archive.apache.org/dist/flink/flink-kubernetes-operator-$FLINK_OPERATOR_VERSION/" --force-update
	# Disable the validating webhook to avoid requiring cert-manager.
	helm --kube-context "$KUBE_CONTEXT" install "$FLINK_OPERATOR_RELEASE" \
		flink-operator-repo/flink-kubernetes-operator \
		--namespace "$FLINK_OPERATOR_NAMESPACE" --create-namespace \
		--set webhook.create=false --wait
fi
# Record the installed chart version, including preexisting installations.
# Capture helm errors before parsing so pipefail cannot bypass diagnostics.
if ! OPERATOR_RELEASES="$(helm --kube-context "$KUBE_CONTEXT" list --all-namespaces \
	--filter "^$FLINK_OPERATOR_RELEASE\$" --output json 2>&1)"; then
	die "helm could not list the releases on $KUBE_CONTEXT: $OPERATOR_RELEASES"
fi
OPERATOR_CHART="$(jq -r '.[0].chart // "not a helm release on this cluster"' <<<"$OPERATOR_RELEASES")"
log "flink operator: $OPERATOR_CHART"

# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

create_bucket

# ---------------------------------------------------------------------------
# ECR
# ---------------------------------------------------------------------------

# shellcheck disable=SC2086  # a deliberate expansion: the names are space separated
for repository in $ECR_REPOSITORIES; do
	if aws ecr describe-repositories --repository-names "$repository" >/dev/null 2>&1; then
		log "ecr repository $repository exists"
	else
		log "creating ecr repository $repository"
		aws ecr create-repository --repository-name "$repository" \
			--tags "Key=$TAG_KEY,Value=true" >/dev/null
	fi
done

# ---------------------------------------------------------------------------
# MSK
# ---------------------------------------------------------------------------

# Place brokers in private EKS subnets, one per availability zone. Select
# as many zones as brokers to satisfy MSK's broker/zone count constraint.
SUBNET_IDS="$(jq -r '.cluster.resourcesVpcConfig.subnetIds | join(" ")' <<<"$CLUSTER_JSON")"
# shellcheck disable=SC2086  # a deliberate expansion: one --subnet-ids argument per id
SUBNETS_JSON="$(aws ec2 describe-subnets --subnet-ids $SUBNET_IDS --output json)"
PRIVATE_SUBNETS="$(jq -r '
	[.Subnets[] | select(.MapPublicIpOnLaunch == false)]
	| group_by(.AvailabilityZone) | map(.[0]) | sort_by(.AvailabilityZone)
	| map(.SubnetId) | join(" ")' <<<"$SUBNETS_JSON")"

MSK_SUBNETS=""
MSK_SUBNET_COUNT=0
# shellcheck disable=SC2086  # a deliberate expansion: the ids are space separated
for subnet in $PRIVATE_SUBNETS; do
	if ((MSK_SUBNET_COUNT >= MSK_BROKERS)); then
		break
	fi
	MSK_SUBNETS="${MSK_SUBNETS:+$MSK_SUBNETS }$subnet"
	MSK_SUBNET_COUNT=$((MSK_SUBNET_COUNT + 1))
done
if ((MSK_SUBNET_COUNT < MSK_BROKERS)); then
	die "MSK_BROKERS is $MSK_BROKERS but $CLUSTER_NAME has private subnets in only $MSK_SUBNET_COUNT availability zone(s) ($PRIVATE_SUBNETS).
     MSK places one broker per subnet, so lower MSK_BROKERS or give the VPC a private subnet in another zone."
fi
log "msk subnets: $MSK_SUBNETS"

MSK_SG_NAME="$MSK_NAME-msk"
MSK_SG_ID="$(aws ec2 describe-security-groups \
	--filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=$MSK_SG_NAME" \
	--query 'SecurityGroups[0].GroupId' --output text)"
if [[ -z $MSK_SG_ID || $MSK_SG_ID == None ]]; then
	log "creating security group $MSK_SG_NAME in $VPC_ID"
	MSK_SG_ID="$(aws ec2 create-security-group --group-name "$MSK_SG_NAME" \
		--description "MSK brokers for lakehouse-ingest-bench" --vpc-id "$VPC_ID" \
		--tag-specifications "ResourceType=security-group,Tags=[{Key=$TAG_KEY,Value=true}]" \
		--query GroupId --output text)"
fi
log "msk security group $MSK_SG_ID"

# Allow all VPC CIDRs, including secondary pod ranges. CIDR rules cover
# managed, self-managed, and autoscaled nodes regardless of security group.
VPC_CIDRS="$(aws ec2 describe-vpcs --vpc-ids "$VPC_ID" \
	--query 'Vpcs[0].CidrBlockAssociationSet[?CidrBlockState.State==`associated`].CidrBlock' --output text)"
[[ -n $VPC_CIDRS ]] || die "$VPC_ID reports no associated CIDR block; nothing to open $MSK_IAM_PORT to"
# shellcheck disable=SC2086  # a deliberate expansion: --output text tab-separates the CIDRs
for cidr in $VPC_CIDRS; do
	if AUTHORIZE_ERROR="$(aws ec2 authorize-security-group-ingress --group-id "$MSK_SG_ID" \
		--protocol tcp --port "$MSK_IAM_PORT" --cidr "$cidr" 2>&1)"; then
		log "opened $MSK_IAM_PORT/tcp on $MSK_SG_ID to $cidr"
	else
		case "$AUTHORIZE_ERROR" in
		*InvalidPermission.Duplicate*) log "$MSK_IAM_PORT/tcp on $MSK_SG_ID is already open to $cidr" ;;
		*) die "could not open $MSK_IAM_PORT/tcp on $MSK_SG_ID to $cidr: $AUTHORIZE_ERROR" ;;
		esac
	fi
done

# The API name filter is a prefix match; also require the exact cluster name.
MSK_ARN="$(aws kafka list-clusters --cluster-name-filter "$MSK_NAME" \
	--query "ClusterInfoList[?ClusterName=='$MSK_NAME'].ClusterArn | [0]" --output text)"
if [[ -z $MSK_ARN || $MSK_ARN == None ]]; then
	if [[ -z ${MSK_KAFKA_VERSION:-} ]]; then
		if ! KAFKA_VERSIONS="$(aws kafka list-kafka-versions \
			--query "KafkaVersions[?Status=='ACTIVE'].Version" --output text 2>&1)"; then
			die "aws kafka list-kafka-versions failed: $KAFKA_VERSIONS — set MSK_KAFKA_VERSION to choose one yourself"
		fi
		choose_kafka_version "$KAFKA_VERSIONS"
		log "kafka version $MSK_KAFKA_VERSION (newest ACTIVE 3.x)"
	else
		log "kafka version $MSK_KAFKA_VERSION (from MSK_KAFKA_VERSION)"
	fi
	BROKER_GROUP="$(jq -nc \
		--arg type "$MSK_BROKER_TYPE" --arg sg "$MSK_SG_ID" \
		--arg subnets "$MSK_SUBNETS" --argjson volume "$MSK_VOLUME_GIB" '{
			InstanceType: $type,
			ClientSubnets: ($subnets | split(" ")),
			SecurityGroups: [$sg],
			StorageInfo: {EbsStorageInfo: {VolumeSize: $volume}}
		}')"
	log "creating MSK cluster $MSK_NAME ($MSK_BROKERS x $MSK_BROKER_TYPE, ${MSK_VOLUME_GIB} GiB each)"
	# Enable IAM authentication only; disable the unauthenticated listener.
	MSK_ARN="$(aws kafka create-cluster \
		--cluster-name "$MSK_NAME" \
		--kafka-version "$MSK_KAFKA_VERSION" \
		--number-of-broker-nodes "$MSK_BROKERS" \
		--broker-node-group-info "$BROKER_GROUP" \
		--client-authentication '{"Sasl":{"Iam":{"Enabled":true}},"Unauthenticated":{"Enabled":false}}' \
		--encryption-info '{"EncryptionInTransit":{"ClientBroker":"TLS","InCluster":true}}' \
		--tags "$TAG_KEY=true" \
		--query ClusterArn --output text)"
else
	log "MSK cluster $MSK_NAME exists"
	grow_broker_volume
fi
log "msk cluster $MSK_ARN"

# Derive topic and group ARNs from the cluster ARN to retain its UUID scope.
MSK_TOPIC_ARN="${MSK_ARN/:cluster/:topic}/*"
MSK_GROUP_ARN="${MSK_ARN/:cluster/:group}/*"

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

# Render IAM while MSK provisions; its ARN is already available. Use the
# template's REGION variable separately from the AWS CLI's AWS_REGION.
REGION="$AWS_REGION"
export ACCOUNT REGION BUCKET MSK_ARN MSK_TOPIC_ARN MSK_GROUP_ARN
PLACEHOLDERS='${ACCOUNT} ${REGION} ${BUCKET} ${MSK_ARN} ${MSK_TOPIC_ARN} ${MSK_GROUP_ARN}'
TRUST_POLICY="$(envsubst "$PLACEHOLDERS" <"$AWS_DIR/iam/trust.json")"
HARNESS_POLICY="$(envsubst "$PLACEHOLDERS" <"$AWS_DIR/iam/harness-policy.json")"

if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
	log "iam role $ROLE_NAME exists; refreshing its trust policy"
	aws iam update-assume-role-policy --role-name "$ROLE_NAME" --policy-document "$TRUST_POLICY"
else
	log "creating iam role $ROLE_NAME"
	aws iam create-role --role-name "$ROLE_NAME" \
		--assume-role-policy-document "$TRUST_POLICY" \
		--tags "Key=$TAG_KEY,Value=true" >/dev/null
fi
# Replace the inline policy so reruns apply the checked-in permissions.
aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME" \
	--policy-document "$HARNESS_POLICY"
log "iam policy $POLICY_NAME applied to $ROLE_NAME"

ROLE_ARN="arn:aws:iam::$ACCOUNT:role/$ROLE_NAME"
for service_account in "$HARNESS_SERVICE_ACCOUNT" "$FLINK_SERVICE_ACCOUNT" "$SPARK_SERVICE_ACCOUNT"; do
	if ASSOCIATE_ERROR="$(aws eks create-pod-identity-association \
		--cluster-name "$CLUSTER_NAME" --namespace "$NAMESPACE" \
		--service-account "$service_account" --role-arn "$ROLE_ARN" \
		--tags "$TAG_KEY=true" 2>&1)"; then
		log "pod identity: $NAMESPACE/$service_account now assumes $ROLE_NAME"
	else
		case "$ASSOCIATE_ERROR" in
		*ResourceInUseException*) log "pod identity: $NAMESPACE/$service_account is already associated" ;;
		*) die "could not associate $NAMESPACE/$service_account with $ROLE_NAME: $ASSOCIATE_ERROR" ;;
		esac
	fi
done

# ---------------------------------------------------------------------------
# Kubernetes
# ---------------------------------------------------------------------------

export NAMESPACE
log "applying the namespace, all three service accounts and the engine RBAC"
envsubst '${NAMESPACE}' <"$AWS_DIR/k8s/namespace.yaml.tmpl" |
	kubectl --context "$KUBE_CONTEXT" apply -f -

# Create the namespace first: the Spark chart installs a Role in every
# namespace listed in spark.jobNamespaces.
install_spark_operator

if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
	log "applying the schema registry"
	# Use sed to avoid a Python dependency. Keep substitutions aligned with the
	# template markers; tests/test_scripts.py checks coverage.
	sed -e "s|__NAMESPACE__|$NAMESPACE|g" \
		-e "s|__NODE_SELECTOR__|$NODE_SELECTOR|g" \
		-e "s|__TOLERATIONS__|$TOLERATIONS|g" \
		<"$AWS_DIR/../k8s/schema-registry.yaml.tmpl" |
		kubectl --context "$KUBE_CONTEXT" apply --namespace "$NAMESPACE" -f -
	kubectl --context "$KUBE_CONTEXT" rollout status deployment/schema-registry \
		--namespace "$NAMESPACE" --timeout 300s
else
	log "skipping schema registry setup (WITH_SCHEMA_REGISTRY is '$WITH_SCHEMA_REGISTRY')"
fi

# ---------------------------------------------------------------------------
# The wait, and what to put in site.yaml
# ---------------------------------------------------------------------------

MSK_STATE="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" --query ClusterInfo.State --output text)"
[[ $MSK_STATE == ACTIVE ]] || log "waiting for $MSK_NAME to reach ACTIVE (typically 15-30 minutes on a first run)"
waited=0
while [[ $MSK_STATE != ACTIVE ]]; do
	case "$MSK_STATE" in
	CREATING | UPDATING | MAINTENANCE) ;;
	*) die "MSK cluster $MSK_NAME is $MSK_STATE; inspect the cluster in the MSK console before retrying" ;;
	esac
	if ((waited >= MSK_ACTIVE_WAIT_S)); then
		die "$MSK_NAME is still $MSK_STATE after ${waited}s; re-run this script to keep waiting, or raise MSK_ACTIVE_WAIT_S"
	fi
	if ((waited % 300 == 0)); then
		log "  ... $MSK_NAME is $MSK_STATE (${waited}s)"
	fi
	sleep 30
	waited=$((waited + 30))
	MSK_STATE="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" --query ClusterInfo.State --output text)"
done

BOOTSTRAP="$(aws kafka get-bootstrap-brokers --cluster-arn "$MSK_ARN" \
	--query BootstrapBrokerStringSaslIam --output text)"

if [[ -n $WRITE_SITE ]]; then
	log "resources are ready. Writing $WRITE_SITE with these values:"
else
	log "setup complete. Copy site.aws.example.yaml to site.yaml and set:"
fi
cat <<SITE
  kafka.bootstrap_servers:        $BOOTSTRAP
  kafka.security.aws.region:      $AWS_REGION
  corpus_root / runs_root / warehouse: s3://$BUCKET/{corpus,runs,warehouse}
  catalog.props.uri:              https://glue.$AWS_REGION.amazonaws.com/iceberg
  catalog.props.warehouse:        "$ACCOUNT"
  catalog.props.rest.signing-region / s3.region: $AWS_REGION
  kubernetes.context:             $KUBE_CONTEXT
  kubernetes.namespace:           $NAMESPACE
  kubernetes.registry:            $ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com
  kubernetes.aws_region:          $AWS_REGION
SITE

if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
	cat <<SITE
  kafka.schema_registry.url:      http://schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7
SITE
fi
if [[ -n $WRITE_SITE ]]; then
	write_site "$WRITE_SITE"
	log "fill in the pricing block in $WRITE_SITE before publishing results. See Cost in docs/methodology.md."
fi
log "MSK incurs hourly charges while idle. Run deploy/aws/teardown.sh when finished."
