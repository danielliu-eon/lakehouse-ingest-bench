#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Remove what `setup.sh` created, in the order that lets each deletion succeed:
# the workloads first, then the identity they ran as, then the broker, then the
# security group the broker's network interfaces were holding.
#
# By default the bucket, the ECR repositories and both engine operators stay:
# the corpus is the expensive thing to rebuild, the images are the slow thing to
# push, and an operator is shared with whatever else runs on the cluster.
# `--all` removes those as well, corpus included.
#
# Like `setup.sh` it never creates, deletes or reconfigures the EKS cluster, and
# it leaves the eks-pod-identity-agent add-on installed — the add-on is free and
# is a property of the cluster rather than of this benchmark.
#
# Every step describes before it deletes, so a re-run after a partial teardown
# finishes the job instead of failing on what has already gone.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
AWS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_lib.sh
source "$AWS_DIR/../../scripts/_lib.sh"

usage() {
	cat <<'USAGE'
usage: deploy/aws/teardown.sh [--all]

  --all   also delete the bucket and everything in it, the three ECR
          repositories and both engine operators' releases

Environment: AWS_REGION and CLUSTER_NAME are required. BUCKET, MSK_NAME,
NAMESPACE and KUBE_CONTEXT mean what they mean to setup.sh and must match the
run of it that created these; MSK_DELETED_WAIT_S bounds the wait below.
USAGE
}

ALL=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--all)
		ALL=1
		shift
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

export AWS_REGION="${AWS_REGION:?AWS_REGION must name the region the EKS cluster is in}"
CLUSTER_NAME="${CLUSTER_NAME:?CLUSTER_NAME must name the EKS cluster setup.sh was run against}"
KUBE_CONTEXT="${KUBE_CONTEXT:-$CLUSTER_NAME}"
MSK_NAME="${MSK_NAME:-lakehouse-ingest-bench}"
NAMESPACE="${NAMESPACE:-ingest-bench}"
# How long to wait for a deleted MSK cluster to disappear. The security group
# cannot go until it has, so giving up here leaves the group for the next run.
MSK_DELETED_WAIT_S="${MSK_DELETED_WAIT_S:-1800}"

TAG_KEY=lakehouse-ingest-bench
ROLE_NAME=lakehouse-ingest-bench-harness
POLICY_NAME=lakehouse-ingest-bench-harness
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink
SPARK_SERVICE_ACCOUNT=ingest-bench-spark
FLINK_OPERATOR_RELEASE=flink-kubernetes-operator
FLINK_OPERATOR_NAMESPACE=flink-operator
SPARK_OPERATOR_RELEASE=spark-operator
SPARK_OPERATOR_NAMESPACE=spark-operator
ECR_REPOSITORIES="lakehouse-ingest-bench/harness lakehouse-ingest-bench/flink lakehouse-ingest-bench/spark"

require_host_tools aws kubectl
if ((ALL == 1)); then
	require_host_tools helm
fi

if ! ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>&1)"; then
	die "aws sts get-caller-identity failed: $ACCOUNT — sign in first (aws configure, or aws sso login --profile ...)"
fi
BUCKET="${BUCKET:-lakehouse-ingest-bench-$ACCOUNT}"

# Everything in the cluster — the namespace, the associations, the operator — is
# gone with the cluster, so its absence is a reason to skip those steps rather
# than an error. Asked once, so that a later failure is a real failure and not
# an unreachable cluster read as an empty answer.
if aws eks describe-cluster --name "$CLUSTER_NAME" >/dev/null 2>&1; then
	CLUSTER_PRESENT=1
	if ! kubectl config get-contexts -o name 2>/dev/null | grep -qxF "$KUBE_CONTEXT"; then
		log "no kubeconfig context $KUBE_CONTEXT; writing one"
		aws eks update-kubeconfig --name "$CLUSTER_NAME" --alias "$KUBE_CONTEXT" >/dev/null
	fi
else
	CLUSTER_PRESENT=0
	log "EKS cluster $CLUSTER_NAME is not in $AWS_REGION; skipping everything that lives inside it"
fi

# ---------------------------------------------------------------------------
# The workloads
# ---------------------------------------------------------------------------

# First, and waited on: the namespace holds every engine of a run and the
# harness Jobs, and a pod still running would keep speaking to MSK and to the
# role while the rest of this deletes them.
if ((CLUSTER_PRESENT == 1)); then
	if kubectl --context "$KUBE_CONTEXT" get namespace "$NAMESPACE" >/dev/null 2>&1; then
		log "deleting namespace $NAMESPACE and everything in it"
		kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --wait
	else
		log "namespace $NAMESPACE is already gone"
	fi
fi

# ---------------------------------------------------------------------------
# The identity
# ---------------------------------------------------------------------------

if ((CLUSTER_PRESENT == 1)); then
	for service_account in "$HARNESS_SERVICE_ACCOUNT" "$FLINK_SERVICE_ACCOUNT" "$SPARK_SERVICE_ACCOUNT"; do
		association_ids="$(aws eks list-pod-identity-associations --cluster-name "$CLUSTER_NAME" \
			--namespace "$NAMESPACE" --service-account "$service_account" \
			--query 'associations[].associationId' --output text)"
		if [[ -z $association_ids ]]; then
			log "pod identity: $NAMESPACE/$service_account has no association"
			continue
		fi
		# shellcheck disable=SC2086  # a deliberate expansion: --output text tab-separates the ids
		for association_id in $association_ids; do
			log "deleting pod identity association $association_id ($NAMESPACE/$service_account)"
			aws eks delete-pod-identity-association --cluster-name "$CLUSTER_NAME" \
				--association-id "$association_id" >/dev/null
		done
	done
fi

if aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
	# The inline policy goes first: IAM refuses to delete a role that still
	# carries one.
	if aws iam get-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME" >/dev/null 2>&1; then
		log "deleting inline policy $POLICY_NAME from $ROLE_NAME"
		aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME"
	fi
	log "deleting iam role $ROLE_NAME"
	aws iam delete-role --role-name "$ROLE_NAME"
else
	log "iam role $ROLE_NAME is already gone"
fi

# ---------------------------------------------------------------------------
# The broker and its security group
# ---------------------------------------------------------------------------

MSK_ARN="$(aws kafka list-clusters --cluster-name-filter "$MSK_NAME" \
	--query "ClusterInfoList[?ClusterName=='$MSK_NAME'].ClusterArn | [0]" --output text)"
if [[ -n $MSK_ARN && $MSK_ARN != None ]]; then
	MSK_STATE="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" --query ClusterInfo.State --output text)"
	if [[ $MSK_STATE != DELETING ]]; then
		log "deleting MSK cluster $MSK_NAME"
		aws kafka delete-cluster --cluster-arn "$MSK_ARN" >/dev/null
	fi
	# Waited on because the brokers' network interfaces hold the security group,
	# and DeleteSecurityGroup fails with DependencyViolation until they are gone.
	log "waiting for $MSK_NAME to disappear (several minutes)"
	waited=0
	while aws kafka describe-cluster --cluster-arn "$MSK_ARN" >/dev/null 2>&1; do
		if ((waited >= MSK_DELETED_WAIT_S)); then
			die "$MSK_NAME is still there after ${waited}s; re-run this script once it has gone"
		fi
		if ((waited % 300 == 0)); then
			log "  ... still deleting (${waited}s)"
		fi
		sleep 30
		waited=$((waited + 30))
	done
else
	log "MSK cluster $MSK_NAME is already gone"
fi

MSK_SG_NAME="$MSK_NAME-msk"
# Matched on the tag as well as the name, so a group of the same name that this
# benchmark did not create is not the one deleted.
MSK_SG_ID="$(aws ec2 describe-security-groups \
	--filters "Name=group-name,Values=$MSK_SG_NAME" "Name=tag:$TAG_KEY,Values=true" \
	--query 'SecurityGroups[0].GroupId' --output text)"
if [[ -n $MSK_SG_ID && $MSK_SG_ID != None ]]; then
	log "deleting security group $MSK_SG_NAME ($MSK_SG_ID)"
	if ! DELETE_ERROR="$(aws ec2 delete-security-group --group-id "$MSK_SG_ID" 2>&1)"; then
		case "$DELETE_ERROR" in
		*DependencyViolation*)
			die "$MSK_SG_ID is still attached to something: $DELETE_ERROR
     MSK releases its network interfaces a few minutes after the cluster goes; re-run this script then."
			;;
		*) die "could not delete $MSK_SG_ID: $DELETE_ERROR" ;;
		esac
	fi
else
	log "security group $MSK_SG_NAME is already gone"
fi

# ---------------------------------------------------------------------------
# What --all also removes
# ---------------------------------------------------------------------------

if ((ALL == 0)); then
	log "kept: s3://$BUCKET, the ECR repositories and both engine operators. --all removes them too."
	exit 0
fi

if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
	log "emptying and deleting s3://$BUCKET — every corpus and every run's artifacts"
	aws s3 rm "s3://$BUCKET" --recursive >/dev/null
	if ! DELETE_ERROR="$(aws s3api delete-bucket --bucket "$BUCKET" 2>&1)"; then
		case "$DELETE_ERROR" in
		*BucketNotEmpty*)
			die "s3://$BUCKET still holds objects: $DELETE_ERROR
     A bucket that was versioned before keeps its noncurrent versions, which \`aws s3 rm\` does not remove.
     Delete them (aws s3api list-object-versions / delete-objects) and re-run with --all."
			;;
		*) die "could not delete s3://$BUCKET: $DELETE_ERROR" ;;
		esac
	fi
else
	log "s3://$BUCKET is already gone"
fi

# shellcheck disable=SC2086  # a deliberate expansion: the names are space separated
for repository in $ECR_REPOSITORIES; do
	if aws ecr describe-repositories --repository-names "$repository" >/dev/null 2>&1; then
		log "deleting ecr repository $repository and its images"
		aws ecr delete-repository --repository-name "$repository" --force >/dev/null
	else
		log "ecr repository $repository is already gone"
	fi
done

if ((CLUSTER_PRESENT == 1)); then
	if helm --kube-context "$KUBE_CONTEXT" status "$FLINK_OPERATOR_RELEASE" \
		--namespace "$FLINK_OPERATOR_NAMESPACE" >/dev/null 2>&1; then
		log "uninstalling the Flink Kubernetes Operator"
		helm --kube-context "$KUBE_CONTEXT" uninstall "$FLINK_OPERATOR_RELEASE" \
			--namespace "$FLINK_OPERATOR_NAMESPACE" --wait
		kubectl --context "$KUBE_CONTEXT" delete namespace "$FLINK_OPERATOR_NAMESPACE" --ignore-not-found --wait
	else
		log "the Flink Kubernetes Operator is not a helm release here; leaving it alone"
	fi
	if helm --kube-context "$KUBE_CONTEXT" status "$SPARK_OPERATOR_RELEASE" \
		--namespace "$SPARK_OPERATOR_NAMESPACE" >/dev/null 2>&1; then
		log "uninstalling the Kubeflow spark-operator"
		helm --kube-context "$KUBE_CONTEXT" uninstall "$SPARK_OPERATOR_RELEASE" \
			--namespace "$SPARK_OPERATOR_NAMESPACE" --wait
		kubectl --context "$KUBE_CONTEXT" delete namespace "$SPARK_OPERATOR_NAMESPACE" --ignore-not-found --wait
	else
		log "the Kubeflow spark-operator is not a helm release here; leaving it alone"
	fi
fi
