#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Everything on an AWS account that a run needs and no run creates for itself:
# a bucket, two ECR repositories, an MSK cluster with its security group, one
# IAM role reached through EKS Pod Identity, and the namespace and
# ServiceAccounts the harness Jobs and the Flink deployments run in.
#
# Every step describes before it creates, so a re-run converges rather than
# failing on what is already there — which is what makes this safe to run
# again after a timeout, a revoked token or a half-finished first attempt.
#
# It never creates, deletes or reconfigures the EKS cluster. That is the
# operator's; deploy/aws/eksctl-cluster.example.yaml makes a minimal one.
set -euo pipefail
# The tools a missing prerequisite points at.
PREREQ_DOC="deploy/aws/README.md"
AWS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_lib.sh
source "$AWS_DIR/../../scripts/_lib.sh"

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Exported so that every `aws` call below reads the region from the environment
# rather than from a flag each one would have to carry.
export AWS_REGION="${AWS_REGION:?AWS_REGION must name the region the EKS cluster is in}"
CLUSTER_NAME="${CLUSTER_NAME:?CLUSTER_NAME must name an existing EKS cluster}"
KUBE_CONTEXT="${KUBE_CONTEXT:-$CLUSTER_NAME}"
MSK_NAME="${MSK_NAME:-lakehouse-ingest-bench}"
NAMESPACE="${NAMESPACE:-ingest-bench}"
MSK_BROKER_TYPE="${MSK_BROKER_TYPE:-kafka.m5.large}"
MSK_BROKERS="${MSK_BROKERS:-2}"
# Whether the namespace also gets a schema registry, which only a run offered
# in the Confluent wire format needs. Off by default: it is another Deployment
# to keep alive, and a site that brings its own registry names that one in
# site.yaml instead.
WITH_SCHEMA_REGISTRY="${WITH_SCHEMA_REGISTRY:-false}"
# Where the registry Deployment is placed, for a cluster whose nodes are
# tainted or labelled. The same two values site.kubernetes carries for the
# harness Jobs, as the one-line JSON a manifest takes.
NODE_SELECTOR="${NODE_SELECTOR:-}"
TOLERATIONS="${TOLERATIONS:-[]}"
# An empty selector is `{}`, which cannot be written as a `${VAR:-...}` default
# without escaping the brace that would close the expansion.
if [[ -z $NODE_SELECTOR ]]; then
	NODE_SELECTOR='{}'
fi
# A cluster that has not reached ACTIVE in this long is reported rather than
# waited on forever; the script is idempotent, so re-running it resumes the
# wait without recreating anything.
MSK_ACTIVE_WAIT_S="${MSK_ACTIVE_WAIT_S:-3600}"

# The tag every resource created here carries, so `teardown.sh` and a cost
# report can both find them by one key.
TAG_KEY=lakehouse-ingest-bench

# Per broker. An offer of a few hundred GB has to sit on the brokers for as long
# as the engine is behind, so the default holds a smoke corpus and an hour run
# needs raising — which is why this is a parameter rather than a constant.
MSK_VOLUME_GIB="${MSK_VOLUME_GIB:-100}"
# MSK's IAM listener. IAM decides who may connect; the security group only
# scopes the network.
MSK_IAM_PORT=9098

# Pinned rather than tracked: the operator's CRD version and the FlinkDeployment
# fields the harness renders have to agree, and `latest` would move under a run.
# 1.15's CRD still lists `flinkVersion: v1_20`, which is what the engine renders.
FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.15.0}"
FLINK_OPERATOR_RELEASE=flink-kubernetes-operator
FLINK_OPERATOR_NAMESPACE=flink-operator

# Pinned for the same reason as the Flink operator's: the CRD version and the
# SparkApplication fields the harness renders have to agree, and `latest` would
# move under a run. 2.5.2 is the newest release on the chart repository's index,
# and `spark.jobNamespaces` is the values key it watches namespaces by.
SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-2.5.2}"
SPARK_OPERATOR_RELEASE=spark-operator
SPARK_OPERATOR_NAMESPACE=spark-operator
SPARK_OPERATOR_REPO=https://kubeflow.github.io/spark-operator

ROLE_NAME=lakehouse-ingest-bench-harness
# One inline policy on the role rather than a managed one: it names this
# account's bucket and this cluster's MSK ARN, so it is not reusable anyway and
# an inline policy is deleted with the role.
POLICY_NAME=lakehouse-ingest-bench-harness
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink
SPARK_SERVICE_ACCOUNT=ingest-bench-spark

ECR_REPOSITORIES="lakehouse-ingest-bench/harness lakehouse-ingest-bench/flink lakehouse-ingest-bench/spark"

(($# == 0)) || die "setup.sh takes no arguments; every parameter is an environment variable (see $PREREQ_DOC)"
[[ $MSK_BROKERS =~ ^[1-9][0-9]*$ ]] || die "MSK_BROKERS must be a positive integer, got '$MSK_BROKERS'"
[[ $MSK_VOLUME_GIB =~ ^[1-9][0-9]*$ ]] || die "MSK_VOLUME_GIB must be a positive integer, got '$MSK_VOLUME_GIB'"

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

require_host_tools aws kubectl helm jq envsubst

if ! ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>&1)"; then
	die "aws sts get-caller-identity failed: $ACCOUNT — sign in first (aws configure, or aws sso login --profile ...)"
fi
log "account $ACCOUNT, region $AWS_REGION"

# S3 bucket names are global, so a default has to carry something unique to the
# operator; the account id is, and it already appears in every ARN the role
# below carries.
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
# PyFlink publishes no aarch64 wheel, so the engine image is amd64 and a
# cluster of arm64 nodes has nowhere to place a TaskManager. Refusing here is
# minutes; the alternative is an unschedulable FlinkDeployment mid-run.
grep -qw amd64 <<<"$ARCHITECTURES" ||
	die "no node in $CLUSTER_NAME reports architecture amd64 (found: ${ARCHITECTURES:-none}); the Flink image is amd64-only.
     Add an amd64 node group — deploy/aws/eksctl-cluster.example.yaml has one."
log "node architectures: $ARCHITECTURES"

if aws eks describe-addon --cluster-name "$CLUSTER_NAME" --addon-name eks-pod-identity-agent >/dev/null 2>&1; then
	log "eks-pod-identity-agent add-on is installed"
else
	log "installing the eks-pod-identity-agent add-on"
	aws eks create-addon --cluster-name "$CLUSTER_NAME" --addon-name eks-pod-identity-agent >/dev/null
fi
# Waited on either way: an add-on found in CREATING or DEGRADED hands out no
# credentials, and a pod that starts before it does gets none.
aws eks wait addon-active --cluster-name "$CLUSTER_NAME" --addon-name eks-pod-identity-agent

if kubectl --context "$KUBE_CONTEXT" get crd flinkdeployments.flink.apache.org >/dev/null 2>&1; then
	log "the flinkdeployments CRD is present"
else
	log "installing the Flink Kubernetes Operator $FLINK_OPERATOR_VERSION"
	# archive.apache.org, not downloads.apache.org: the download mirror serves
	# only the current releases, so the moment a pinned version stops being one
	# its chart 404s. The archive keeps every release, current ones included.
	helm repo add flink-operator-repo \
		"https://archive.apache.org/dist/flink/flink-kubernetes-operator-$FLINK_OPERATOR_VERSION/" --force-update
	# webhook.create=false: the chart's validating webhook needs cert-manager,
	# which is a second operator to install and keep alive for validation the
	# harness does not depend on.
	helm --kube-context "$KUBE_CONTEXT" install "$FLINK_OPERATOR_RELEASE" \
		flink-operator-repo/flink-kubernetes-operator \
		--namespace "$FLINK_OPERATOR_NAMESPACE" --create-namespace \
		--set webhook.create=false --wait
fi
# Recorded in the log because a run's engine behaviour belongs to the operator's
# version, and a cluster that had the CRD already may be running any of them.
#
# Read into a variable before it is parsed rather than piped straight into one:
# under `pipefail` a failing `helm` inside a command substitution aborts the
# script at the assignment, so any refusal written after it never runs.
if ! OPERATOR_RELEASES="$(helm --kube-context "$KUBE_CONTEXT" list --all-namespaces \
	--filter "^$FLINK_OPERATOR_RELEASE\$" --output json 2>&1)"; then
	die "helm could not list the releases on $KUBE_CONTEXT: $OPERATOR_RELEASES"
fi
OPERATOR_CHART="$(jq -r '.[0].chart // "not a helm release on this cluster"' <<<"$OPERATOR_RELEASES")"
log "flink operator: $OPERATOR_CHART"

# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

# Whether the bucket already carries the tag this script puts on everything it
# creates. A bucket with no tag set at all answers with an API error rather than
# an empty tag list, and both mean the same thing here.
bucket_is_ours() {
	local tags
	tags="$(aws s3api get-bucket-tagging --bucket "$1" \
		--query "TagSet[?Key=='$TAG_KEY'].Value" --output text 2>/dev/null)" || return 1
	[[ $tags == true ]]
}

# The bucket, created or adopted, and then configured the way a corpus wants it.
#
# A bucket that already exists and does not carry the tag is refused rather than
# adopted: the two calls below are not additive — PutBucketTagging replaces the
# whole tag set and PutBucketVersioning suspends versioning — so adopting one
# would silently reconfigure a bucket the operator keeps something else in.
# `BUCKET` defaults to a name derived from the account id, which is exactly the
# name someone may already have used.
create_bucket() {
	if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
		bucket_is_ours "$BUCKET" ||
			die "s3://$BUCKET already exists and carries no $TAG_KEY tag, so this script did not create it;
     tagging and versioning are set below and neither call is additive, so it will not adopt one.
     Name a bucket of your own with BUCKET, or tag that one $TAG_KEY=true if it is meant to be this benchmark's."
		log "s3://$BUCKET exists"
	else
		log "creating s3://$BUCKET"
		# us-east-1 is the one region whose CreateBucket refuses a location
		# constraint naming it.
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
	# Only when it is actually on. A corpus is regenerated rather than restored,
	# so versions buy nothing and every deleted object of a hundred-gigabyte
	# corpus would keep being billed; a bucket that never had versioning needs
	# no call at all, and PutBucketVersioning is the only way to turn one off.
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

# The brokers go in the cluster's own private subnets, so the pods reach them
# over the VPC and nothing about the lane is public. One subnet per availability
# zone, in zone order: MSK takes one subnet per zone and requires the broker
# count to be a multiple of the zone count, which holds because this asks for
# exactly as many zones as brokers.
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

# Every CIDR the VPC has, not only its primary: EKS clusters commonly carry a
# secondary range for pods. The rule is written against the CIDR rather than
# against the cluster's own security group because a CIDR reaches managed nodes,
# self-managed nodes and autoscaler-provisioned nodes alike, and a rule naming
# one security group reaches only the nodes that happen to carry it.
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

# The brokers' volumes raised to MSK_VOLUME_GIB, and left alone at or above it.
#
# A broker volume can be grown and never shrunk, so those are the only two
# answers. Growing it is what lets a campaign move from a smoke corpus to an
# hour run without recreating the cluster: an hour's offer sits on the brokers
# for as long as the engine is behind, and a volume that fills stops the offer
# rather than the engine — which is the run's own rate, measured against a
# broker that ran out of room.
grow_broker_volume() {
	local reported state version current update_error
	# One read for all three: the size says whether to grow, the state says
	# whether now is the time, and the version is what an update has to carry.
	reported="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" \
		--query 'ClusterInfo.[State,CurrentVersion,BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize]' \
		--output text)" ||
		die "could not read $MSK_NAME's broker storage; try: aws kafka describe-cluster --cluster-arn $MSK_ARN"
	IFS=$'\t' read -r state version current <<<"$reported"
	# `--output text` prints `None` for a field the API left out, which an
	# arithmetic comparison would read as zero and then grow a cluster whose
	# shape nobody knows.
	if [[ -z $current || $current == None ]]; then
		die "$MSK_NAME reports no broker volume size, so this cannot tell whether it holds ${MSK_VOLUME_GIB} GiB"
	fi
	if ((current >= MSK_VOLUME_GIB)); then
		log "msk broker volumes are ${current} GiB, at or above the ${MSK_VOLUME_GIB} GiB asked for"
		return 0
	fi
	# A cluster still applying an earlier update keeps reporting the old size
	# while it does, so the size alone would ask for the same growth a second
	# time and MSK would refuse it. The state is what tells those two apart.
	if [[ $state != ACTIVE ]]; then
		log "msk broker volumes are ${current} GiB and $MSK_NAME is $state, so the growth to ${MSK_VOLUME_GIB} GiB is left to the update already running"
		return 0
	fi
	log "growing the msk broker volumes from ${current} to ${MSK_VOLUME_GIB} GiB"
	# The version MSK reports rather than a guess: an update carrying the wrong
	# one is refused. The cluster leaves ACTIVE while it applies this, and
	# everything below that needs the cluster waits on ACTIVE anyway — so this
	# only has to be requested, and the wait at the end of the script covers it.
	if update_error="$(aws kafka update-broker-storage --cluster-arn "$MSK_ARN" --current-version "$version" \
		--target-broker-ebs-volume-info "KafkaBrokerNodeId=All,VolumeSizeGB=$MSK_VOLUME_GIB" 2>&1)"; then
		return 0
	fi
	case "$update_error" in
	# MSK holds a cooldown between storage updates, and refuses one on a
	# cluster that left ACTIVE between the read above and this call. Both mean
	# "not now" rather than "not ever", and neither changes what the volume
	# already is — so a re-run of this script converges instead of failing on
	# the growth a previous run asked for, which is what the header promises.
	*ACTIVE* | *UPDATING* | *ooldown* | *"6 hour"* | *"6-hour"*)
		log "$MSK_NAME will not take the growth to ${MSK_VOLUME_GIB} GiB yet: $update_error"
		;;
	*)
		die "could not grow $MSK_NAME's broker volumes to ${MSK_VOLUME_GIB} GiB: $update_error"
		;;
	esac
}

# `list-clusters --cluster-name-filter` matches on a prefix, so the exact name
# is asserted in the query as well; a longer-named cluster is not this one.
MSK_ARN="$(aws kafka list-clusters --cluster-name-filter "$MSK_NAME" \
	--query "ClusterInfoList[?ClusterName=='$MSK_NAME'].ClusterArn | [0]" --output text)"
if [[ -z $MSK_ARN || $MSK_ARN == None ]]; then
	if [[ -z ${MSK_KAFKA_VERSION:-} ]]; then
		if ! KAFKA_VERSIONS="$(aws kafka list-kafka-versions \
			--query "KafkaVersions[?Status=='ACTIVE'].Version" --output text 2>&1)"; then
			die "aws kafka list-kafka-versions failed: $KAFKA_VERSIONS — set MSK_KAFKA_VERSION to choose one yourself"
		fi
		# The newest plain 3.x, where MSK spells a line's latest patch either as
		# a number or as a trailing `x` (3.7.x); a `.tiered` variant is a
		# different storage mode and is not what an unset knob should pick. `x`
		# must sort after every numeric patch of the same minor, which a plain
		# numeric field sort cannot express, so the patch is mapped to a
		# sentinel column for ordering and the real version recovered from the
		# tab afterward.
		# `|| true` because no match is a refusal with a fix on the next line, and
		# under `pipefail` grep's exit 1 would otherwise abort before it is read.
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
	# IAM only: no SCRAM secret to store, rotate or leak into a rendered file,
	# and no unauthenticated listener at all.
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

# The topic and group ARNs share everything with the cluster ARN but the
# resource type, and the cluster's uuid is part of them — deriving them from the
# cluster ARN is what keeps the policy scoped to this cluster rather than to any
# cluster that ever carried the name.
MSK_TOPIC_ARN="${MSK_ARN/:cluster/:topic}/*"
MSK_GROUP_ARN="${MSK_ARN/:cluster/:group}/*"

# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------

# Rendered while MSK provisions, which takes tens of minutes: the ARNs the
# policy needs are known as soon as the cluster is requested, so the wait
# happens once at the end instead of blocking the rest of the setup.
#
# The documents say `${REGION}`, not `${AWS_REGION}`: that name belongs to the
# AWS CLI's own environment contract, and a policy template should not depend
# on it meaning what this script means by it.
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
# put-role-policy replaces, so this is the same call on a first and a repeat run
# and the role's permissions always match the documents in this checkout.
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

# After the namespace and not in the preflight beside the Flink operator's: the
# chart grants its controller a Role in each namespace named by
# `spark.jobNamespaces`, which is what makes the harness namespace eligible at
# all, and a Role cannot be created in a namespace that does not exist yet.
if kubectl --context "$KUBE_CONTEXT" get crd sparkapplications.sparkoperator.k8s.io >/dev/null 2>&1; then
	log "the sparkapplications CRD is present"
else
	log "installing the Kubeflow spark-operator $SPARK_OPERATOR_VERSION"
	helm repo add "$SPARK_OPERATOR_RELEASE" "$SPARK_OPERATOR_REPO" --force-update
	# The chart's own spark identity and RBAC are off: a run's driver runs as
	# $SPARK_SERVICE_ACCOUNT, because a Pod Identity association is made per
	# (namespace, service account) and that name is the one bound to the role
	# above. The chart would bind its Role to an account of its own naming
	# instead, so the namespace manifest grants ours the same rules.
	#
	# The webhook is stated rather than left to the chart's default, because it
	# is what grafts `spec.volumes` and the two `volumeMounts` onto the pods —
	# a SparkApplication carries them and the CRD alone does not apply them. An
	# install without it starts a driver with no /opt/bench/run, which dies
	# opening the run's job document.
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
# Recorded in the log because a run's engine behaviour belongs to the operator's
# version, and a cluster that had the CRD already may be running any of them.
if ! SPARK_OPERATOR_RELEASES="$(helm --kube-context "$KUBE_CONTEXT" list --all-namespaces \
	--filter "^$SPARK_OPERATOR_RELEASE\$" --output json 2>&1)"; then
	die "helm could not list the releases on $KUBE_CONTEXT: $SPARK_OPERATOR_RELEASES"
fi
log "spark operator: $(jq -r '.[0].chart // "not a helm release on this cluster"' <<<"$SPARK_OPERATOR_RELEASES")"

if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
	log "applying the schema registry"
	# `sed` and not the harness's own renderer: this script needs no Python
	# toolchain, and the markers are three. A marker the template gains and
	# this list does not would reach the API server verbatim and be refused
	# there, which is what the render test in tests/test_scripts.py pins.
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

log "waiting for $MSK_NAME to reach ACTIVE (typically 15-30 minutes on a first run)"
waited=0
while :; do
	MSK_STATE="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" --query ClusterInfo.State --output text)"
	if [[ $MSK_STATE == ACTIVE ]]; then
		break
	fi
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
done

BOOTSTRAP="$(aws kafka get-bootstrap-brokers --cluster-arn "$MSK_ARN" \
	--query BootstrapBrokerStringSaslIam --output text)"

log "setup complete. Copy site.aws.example.yaml to site.yaml and fill it in with:"
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
log "MSK bills by the hour whether or not a run is using it — deploy/aws/teardown.sh when you are done."
