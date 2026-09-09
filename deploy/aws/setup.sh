#!/usr/bin/env bash
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
# A cluster that has not reached ACTIVE in this long is reported rather than
# waited on forever; the script is idempotent, so re-running it resumes the
# wait without recreating anything.
MSK_ACTIVE_WAIT_S="${MSK_ACTIVE_WAIT_S:-3600}"

# The tag every resource created here carries, so `teardown.sh` and a cost
# report can both find them by one key.
TAG_KEY=lakehouse-ingest-bench

# 100 GiB per broker: an offer of a few hundred GB has to sit on the brokers
# for as long as the engine is behind, and MSK's smallest volume would fill.
MSK_VOLUME_GIB=100
# MSK's IAM listener. IAM decides who may connect; the security group only
# scopes the network.
MSK_IAM_PORT=9098

# Pinned rather than tracked: the operator's CRD version and the FlinkDeployment
# fields the harness renders have to agree, and `latest` would move under a run.
FLINK_OPERATOR_VERSION=1.10.0
FLINK_OPERATOR_RELEASE=flink-kubernetes-operator
FLINK_OPERATOR_NAMESPACE=flink-operator

ROLE_NAME=lakehouse-ingest-bench-harness
# One inline policy on the role rather than a managed one: it names this
# account's bucket and this cluster's MSK ARN, so it is not reusable anyway and
# an inline policy is deleted with the role.
POLICY_NAME=lakehouse-ingest-bench-harness
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink

ECR_REPOSITORIES="lakehouse-ingest-bench/harness lakehouse-ingest-bench/flink"

(($# == 0)) || die "setup.sh takes no arguments; every parameter is an environment variable (see $PREREQ_DOC)"
[[ $MSK_BROKERS =~ ^[1-9][0-9]*$ ]] || die "MSK_BROKERS must be a positive integer, got '$MSK_BROKERS'"

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
	helm repo add flink-operator-repo \
		"https://downloads.apache.org/flink/flink-kubernetes-operator-$FLINK_OPERATOR_VERSION/" --force-update
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
OPERATOR_CHART="$(helm --kube-context "$KUBE_CONTEXT" list --all-namespaces \
	--filter "^$FLINK_OPERATOR_RELEASE\$" --output json |
	jq -r '.[0].chart // "not a helm release on this cluster"')"
log "flink operator: $OPERATOR_CHART"

# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
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
# Only when it is actually on. A corpus is regenerated rather than restored, so
# versions buy nothing and every deleted object of a hundred-gigabyte corpus
# would keep being billed; a bucket that never had versioning needs no call at
# all, and PutBucketVersioning is the only way to turn one off.
if [[ "$(aws s3api get-bucket-versioning --bucket "$BUCKET" --query 'Status' --output text)" == Enabled ]]; then
	log "suspending versioning on s3://$BUCKET"
	aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Suspended
fi

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

# `list-clusters --cluster-name-filter` matches on a prefix, so the exact name
# is asserted in the query as well; a longer-named cluster is not this one.
MSK_ARN="$(aws kafka list-clusters --cluster-name-filter "$MSK_NAME" \
	--query "ClusterInfoList[?ClusterName=='$MSK_NAME'].ClusterArn | [0]" --output text)"
if [[ -z $MSK_ARN || $MSK_ARN == None ]]; then
	if [[ -z ${MSK_KAFKA_VERSION:-} ]]; then
		# The newest plain 3.x: a `.tiered` variant sorts higher and is a
		# different storage mode, which is not what an unset knob should pick.
		MSK_KAFKA_VERSION="$(aws kafka list-kafka-versions \
			--query "KafkaVersions[?Status=='ACTIVE'].Version" --output text |
			tr '\t' '\n' | grep -E '^3(\.[0-9]+)+$' | sort -t. -k1,1n -k2,2n -k3,3n | tail -1)"
		[[ -n $MSK_KAFKA_VERSION ]] ||
			die "aws kafka list-kafka-versions reported no ACTIVE 3.x version; set MSK_KAFKA_VERSION yourself"
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
for service_account in "$HARNESS_SERVICE_ACCOUNT" "$FLINK_SERVICE_ACCOUNT"; do
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
log "applying the namespace, both service accounts and the flink RBAC"
envsubst '${NAMESPACE}' <"$AWS_DIR/k8s/namespace.yaml.tmpl" |
	kubectl --context "$KUBE_CONTEXT" apply -f -

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
log "MSK bills by the hour whether or not a run is using it — deploy/aws/teardown.sh when you are done."
