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

usage() {
	cat <<'USAGE'
usage: deploy/aws/setup.sh [--write-site PATH]

  --write-site PATH   also write a complete site.yaml at PATH, from the same
                      values this prints at the end. Refuses rather than
                      overwrite a file that is already there

Environment: AWS_REGION and CLUSTER_NAME are required, and every other
parameter is an environment variable as well — deploy/aws/README.md, under
Environment, lists them all with their defaults.
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

# Pin a CRD version compatible with the rendered SparkApplication fields.
# spark.jobNamespaces controls which namespaces the operator watches.
SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-2.5.2}"
SPARK_OPERATOR_RELEASE=spark-operator
SPARK_OPERATOR_NAMESPACE=spark-operator
SPARK_OPERATOR_REPO=https://kubeflow.github.io/spark-operator

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
	die "$WRITE_SITE already exists, and --write-site never overwrites a site config; name another path or move that file"

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
	log "warning: no node in $CLUSTER_NAME reports architecture amd64 (found: ${ARCHITECTURES:-none}); the Flink image is amd64-only, so no Flink run will be placed here. A Spark-only campaign may proceed.
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

# Missing tags may be an API error or an empty list; both mean unowned here.
bucket_is_ours() {
	local tags
	tags="$(aws s3api get-bucket-tagging --bucket "$1" \
		--query "TagSet[?Key=='$TAG_KEY'].Value" --output text 2>/dev/null)" || return 1
	[[ $tags == true ]]
}

# Only configure existing buckets carrying the benchmark tag. Tag replacement
# and versioning changes could otherwise alter an unrelated bucket.
create_bucket() {
	if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
		bucket_is_ours "$BUCKET" ||
			die "s3://$BUCKET already exists and carries no $TAG_KEY tag, so this script did not create it;
     tagging and versioning are set below and neither call is additive, so it will not adopt one.
     Name a bucket of your own with BUCKET, or tag that one $TAG_KEY=true if it is meant to be this benchmark's."
		log "s3://$BUCKET exists"
	else
		log "creating s3://$BUCKET"
		# CreateBucket in us-east-1 rejects an explicit LocationConstraint.
		if [[ $AWS_REGION == us-east-1 ]]; then
			aws s3api create-bucket --bucket "$BUCKET" >/dev/null
		else
			aws s3api create-bucket --bucket "$BUCKET" \
				--create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
		fi
	fi
	aws s3api put-public-access-block --bucket "$BUCKET" \
		--public-access-block-configuration \
		BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
	aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[{Key=$TAG_KEY,Value=true}]"
	# Suspend enabled versioning to avoid retaining billable deleted corpus data.
	if [[ "$(aws s3api get-bucket-versioning --bucket "$BUCKET" --query 'Status' --output text)" == Enabled ]]; then
		log "suspending versioning on s3://$BUCKET"
		aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Suspended
	fi
}

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

# Grow broker storage to MSK_VOLUME_GIB when needed; MSK cannot shrink it.
# Enough storage prevents broker capacity from limiting the offered rate.
grow_broker_volume() {
	local reported state version current update_error
	# Read size, state, and update version from one response.
	reported="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" \
		--query 'ClusterInfo.[State,CurrentVersion,BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize]' \
		--output text)" ||
		die "could not read $MSK_NAME's broker storage; try: aws kafka describe-cluster --cluster-arn $MSK_ARN"
	IFS=$'\t' read -r state version current <<<"$reported"
	# Reject missing numeric fields instead of interpreting AWS CLI's None as zero.
	if [[ -z $current || $current == None ]]; then
		die "$MSK_NAME reports no broker volume size, so this cannot tell whether it holds ${MSK_VOLUME_GIB} GiB"
	fi
	if ((current >= MSK_VOLUME_GIB)); then
		log "msk broker volumes are ${current} GiB, at or above the ${MSK_VOLUME_GIB} GiB asked for"
		return 0
	fi
	# An in-progress update may still report the old size; do not request it twice.
	if [[ $state != ACTIVE ]]; then
		log "msk broker volumes are ${current} GiB and $MSK_NAME is $state, so the growth to ${MSK_VOLUME_GIB} GiB is left to the update already running"
		return 0
	fi
	log "growing the msk broker volumes from ${current} to ${MSK_VOLUME_GIB} GiB"
	# Use the reported version for the update. The final ACTIVE wait covers it.
	if update_error="$(aws kafka update-broker-storage --cluster-arn "$MSK_ARN" --current-version "$version" \
		--target-broker-ebs-volume-info "KafkaBrokerNodeId=All,VolumeSizeGB=$MSK_VOLUME_GIB" 2>&1)"; then
		return 0
	fi
	case "$update_error" in
	# Cooldowns and concurrent updates are retryable on a later setup invocation.
	*ACTIVE* | *UPDATING* | *ooldown* | *"6 hour"* | *"6-hour"*)
		log "$MSK_NAME will not take the growth to ${MSK_VOLUME_GIB} GiB yet: $update_error"
		;;
	*)
		die "could not grow $MSK_NAME's broker volumes to ${MSK_VOLUME_GIB} GiB: $update_error"
		;;
	esac
}

# The API name filter is a prefix match; also require the exact cluster name.
MSK_ARN="$(aws kafka list-clusters --cluster-name-filter "$MSK_NAME" \
	--query "ClusterInfoList[?ClusterName=='$MSK_NAME'].ClusterArn | [0]" --output text)"
if [[ -z $MSK_ARN || $MSK_ARN == None ]]; then
	if [[ -z ${MSK_KAFKA_VERSION:-} ]]; then
		if ! KAFKA_VERSIONS="$(aws kafka list-kafka-versions \
			--query "KafkaVersions[?Status=='ACTIVE'].Version" --output text 2>&1)"; then
			die "aws kafka list-kafka-versions failed: $KAFKA_VERSIONS — set MSK_KAFKA_VERSION to choose one yourself"
		fi
		# Choose the newest plain ACTIVE 3.x release, excluding tiered variants.
		# Sort a trailing x after numeric patches using a temporary sentinel.
		# Allow no-match through pipefail so the next check can explain the failure.
		MSK_KAFKA_VERSION="$(tr '\t' '\n' <<<"$KAFKA_VERSIONS" |
			grep -E '^3\.[0-9]+\.([0-9]+|x)$' |
			while IFS=. read -r major minor patch; do
				sort_patch=$patch
				[[ $patch == x ]] && sort_patch=999999
				printf '%s %s %s\t%s.%s.%s\n' "$major" "$minor" "$sort_patch" "$major" "$minor" "$patch"
			done |
			sort -k1,1n -k2,2n -k3,3n | tail -1 | cut -f2 || true)"
		[[ -n $MSK_KAFKA_VERSION ]] ||
			die "no ACTIVE 3.x Kafka version among ${KAFKA_VERSIONS//$'\t'/ }; set MSK_KAFKA_VERSION yourself"
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
if kubectl --context "$KUBE_CONTEXT" get crd sparkapplications.sparkoperator.k8s.io >/dev/null 2>&1; then
	log "the sparkapplications CRD is present"
else
	log "installing the Kubeflow spark-operator $SPARK_OPERATOR_VERSION"
	helm repo add "$SPARK_OPERATOR_RELEASE" "$SPARK_OPERATOR_REPO" --force-update
	# Use the benchmark service account bound to Pod Identity; the namespace
	# manifest supplies its RBAC. Enable the webhook explicitly because it adds
	# the ConfigMap volume mounts needed at /opt/bench/run.
	helm --kube-context "$KUBE_CONTEXT" install "$SPARK_OPERATOR_RELEASE" \
		"$SPARK_OPERATOR_RELEASE/spark-operator" \
		--namespace "$SPARK_OPERATOR_NAMESPACE" --create-namespace \
		--version "$SPARK_OPERATOR_VERSION" \
		--set "spark.jobNamespaces={$NAMESPACE}" \
		--set spark.serviceAccount.create=false \
		--set spark.rbac.create=false \
		--set webhook.enable=true \
		--wait
fi
# Record the installed chart version, including preexisting installations.
if ! SPARK_OPERATOR_RELEASES="$(helm --kube-context "$KUBE_CONTEXT" list --all-namespaces \
	--filter "^$SPARK_OPERATOR_RELEASE\$" --output json 2>&1)"; then
	die "helm could not list the releases on $KUBE_CONTEXT: $SPARK_OPERATOR_RELEASES"
fi
log "spark operator: $(jq -r '.[0].chart // "not a helm release on this cluster"' <<<"$SPARK_OPERATOR_RELEASES")"

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
	log "no schema registry (WITH_SCHEMA_REGISTRY is '$WITH_SCHEMA_REGISTRY')"
fi

# ---------------------------------------------------------------------------
# The wait, and what to put in site.yaml
# ---------------------------------------------------------------------------

# Write a complete site.yaml matching site.aws.example.yaml. Quote scalars,
# including the numeric-looking Glue warehouse account ID. Leave pricing at
# zero for the operator to fill in before publishing results.
write_site() {
	local path=$1 parsed read_back=""
	[[ ! -e $path ]] ||
		die "$path already exists, and --write-site never overwrites a site config; name another path or move that file"
	{
		cat <<-SITE
			# Generated by setup.sh --write-site; see site.aws.example.yaml for field details.
			corpus_root: "s3://$BUCKET/corpus"
			runs_root: "s3://$BUCKET/runs"
			warehouse: "s3://$BUCKET/warehouse"
			kafka:
			  bootstrap_servers: "$BOOTSTRAP"
			  security:
			    security.protocol: SASL_SSL
			    sasl.mechanism: OAUTHBEARER
			    aws.region: "$AWS_REGION"
		SITE
		if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
			cat <<-SITE
				  schema_registry:
				    url: "http://schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7"
			SITE
		fi
		cat <<-SITE
			catalog:
			  props:
			    uri: "https://glue.$AWS_REGION.amazonaws.com/iceberg"
			    warehouse: "$ACCOUNT"
			    rest.sigv4-enabled: "true"
			    rest.signing-name: glue
			    rest.signing-region: "$AWS_REGION"
			    s3.region: "$AWS_REGION"
			kubernetes:
			  context: "$KUBE_CONTEXT"
			  namespace: "$NAMESPACE"
			  harness_service_account: "$HARNESS_SERVICE_ACCOUNT"
			  flink_service_account: "$FLINK_SERVICE_ACCOUNT"
			  spark_service_account: "$SPARK_SERVICE_ACCOUNT"
			  service_account_annotations: {}
			  registry: "$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
			  aws_region: "$AWS_REGION"
			  # Optional Secret supplying environment variables for \${env:NAME} references.
			  # secret_name: bench-env
			  node_selector: $NODE_SELECTOR
			  tolerations: $TOLERATIONS
			# Set hourly vCPU and GiB prices using docs/methodology.md's Cost section.
			# Published results cannot use these zero defaults.
			pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}
		SITE
	} >"$path"
	# Validate the generated YAML. Remove invalid output so the overwrite guard
	# does not prevent a corrected retry.
	if ! parsed="$(yq -e '.kafka.bootstrap_servers' "$path" 2>&1)"; then
		read_back="yq could not read a site config out of it: $parsed"
	elif [[ $parsed != "$BOOTSTRAP" ]]; then
		read_back="its kafka.bootstrap_servers reads back as '$parsed' rather than '$BOOTSTRAP'"
	fi
	if [[ -n $read_back ]]; then
		rm -f "$path"
		die "wrote $path and removed it again: $read_back"
	fi
	log "wrote $path"
}

MSK_STATE="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" --query ClusterInfo.State --output text)"
[[ $MSK_STATE == ACTIVE ]] || log "waiting for $MSK_NAME to reach ACTIVE (typically 15-30 minutes on a first run)"
waited=0
while [[ $MSK_STATE != ACTIVE ]]; do
	case "$MSK_STATE" in
	CREATING | UPDATING | MAINTENANCE) ;;
	*) die "MSK cluster $MSK_NAME is $MSK_STATE, which it will not leave on its own; look at it in the MSK console" ;;
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
	log "setup complete. $WRITE_SITE is being written with these values:"
else
	log "setup complete. Copy site.aws.example.yaml to site.yaml and fill it in with:"
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
	log "fill in $WRITE_SITE's pricing block before publishing a result from it; docs/methodology.md, under Cost, is the rule."
fi
log "MSK bills by the hour whether or not a run is using it — deploy/aws/teardown.sh when you are done."
