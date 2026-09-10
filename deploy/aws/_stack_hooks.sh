# SPDX-License-Identifier: Apache-2.0
# The AWS half of the in-cluster stack, sourced by deploy/k8s/stack/setup.sh
# and teardown.sh when CLOUD=aws: what an account has to hold for a broker and
# a catalog that live inside the cluster. Everything cloud-neutral stays in the
# stack scripts; a second cloud is a second file defining the same functions:
# stack_preflight, stack_preflight_storage, stack_bind_identity,
# stack_unbind_identity, stack_storage_class, stack_delete_storage_class,
# stack_catalog_settings, stack_storage_profile_json and
# stack_storage_credential_json.
#
# Sourced after scripts/_lib.sh, never executed: `log`, `die` and
# `require_host_tools` come from there.

_STACK_HOOKS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

# One role for the stack's four identities. Separate from the role
# deploy/aws/setup.sh makes, whose policy names a managed broker this stack
# does not have — which is what lets the stack be bound without one.
STACK_ROLE_NAME=lakehouse-ingest-bench-stack
STACK_POLICY_NAME=lakehouse-ingest-bench-stack
STACK_TAG_KEY=lakehouse-ingest-bench

KAFKA_STORAGE_CLASS="${KAFKA_STORAGE_CLASS:-ingest-bench-kafka}"
KAFKA_VOLUME_THROUGHPUT_MIBS="${KAFKA_VOLUME_THROUGHPUT_MIBS:-250}"
KAFKA_VOLUME_IOPS="${KAFKA_VOLUME_IOPS:-6000}"

# What the stack needs of the account and the cluster before it changes
# anything, each refusal naming its fix. Sets ACCOUNT. Shared by setup.sh and
# teardown.sh — what only setup needs is stack_preflight_storage below, so a
# teardown does not refuse on a bucket or a CSI add-on already removed.
stack_preflight() {
	require_host_tools aws envsubst
	[[ -n ${AWS_REGION:-} ]] || die "AWS_REGION must name the region the cluster and the bucket are in"
	export AWS_REGION
	[[ -n ${CLUSTER_NAME:-} ]] || die "CLUSTER_NAME must name the EKS cluster, for its pod identity associations"

	if ! ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>&1)"; then
		die "aws sts get-caller-identity failed: $ACCOUNT — sign in first (aws configure, or aws sso login --profile ...)"
	fi
	log "account $ACCOUNT, region $AWS_REGION"
}

# What only setup.sh needs before it provisions storage: the bucket and the
# volume knobs it is told, and the CSI driver a broker's or the database's
# claim depends on.
stack_preflight_storage() {
	[[ -n ${BUCKET:-} ]] || die "BUCKET must name the bucket whose corpus/, runs/ and warehouse/ prefixes a run uses"
	[[ $KAFKA_VOLUME_THROUGHPUT_MIBS =~ ^[1-9][0-9]*$ ]] ||
		die "KAFKA_VOLUME_THROUGHPUT_MIBS must be a positive integer, got '$KAFKA_VOLUME_THROUGHPUT_MIBS'"
	[[ $KAFKA_VOLUME_IOPS =~ ^[1-9][0-9]*$ ]] || die "KAFKA_VOLUME_IOPS must be a positive integer, got '$KAFKA_VOLUME_IOPS'"

	log "bucket s3://$BUCKET"
	aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 ||
		die "s3://$BUCKET is not reachable from this account; deploy/aws/setup.sh creates the bucket, or name one of your own with BUCKET"

	# A persistent volume on a cluster without the EBS CSI driver pends
	# forever, and the failure would surface as a broker that never starts.
	if ! kubectl --context "$KUBE_CONTEXT" get csidriver ebs.csi.aws.com >/dev/null 2>&1; then
		die "$CLUSTER_NAME has no ebs.csi.aws.com CSI driver, so no broker or database volume can be provisioned. Install the add-on with its identity, then re-run:
     eksctl create addon --cluster $CLUSTER_NAME --region $AWS_REGION --name aws-ebs-csi-driver --auto-apply-pod-identity-associations"
	fi
	log "ebs.csi.aws.com is present"
}

# stack_bind_identity <namespace> <service-account>... — the role, its policy,
# and one pod identity association per account. Before any pod exists: the
# catalog validates bucket access when its warehouse is created, and a pod
# that started before its association held has no credentials until it
# restarts.
stack_bind_identity() {
	local namespace=$1
	shift
	local trust policy role_arn service_account associate_error
	trust="$(cat "$_STACK_HOOKS_DIR/iam/trust.json")"
	export BUCKET
	policy="$(envsubst '${BUCKET}' <"$_STACK_HOOKS_DIR/iam/stack-policy.json")"
	if aws iam get-role --role-name "$STACK_ROLE_NAME" >/dev/null 2>&1; then
		log "iam role $STACK_ROLE_NAME exists; refreshing its trust policy"
		aws iam update-assume-role-policy --role-name "$STACK_ROLE_NAME" --policy-document "$trust"
	else
		log "creating iam role $STACK_ROLE_NAME"
		aws iam create-role --role-name "$STACK_ROLE_NAME" --assume-role-policy-document "$trust" \
			--tags "Key=$STACK_TAG_KEY,Value=true" >/dev/null
	fi
	# put-role-policy replaces, so this is the same call on a first and a repeat run.
	aws iam put-role-policy --role-name "$STACK_ROLE_NAME" --policy-name "$STACK_POLICY_NAME" --policy-document "$policy"
	role_arn="arn:aws:iam::$ACCOUNT:role/$STACK_ROLE_NAME"
	for service_account in "$@"; do
		if associate_error="$(aws eks create-pod-identity-association --cluster-name "$CLUSTER_NAME" \
			--namespace "$namespace" --service-account "$service_account" --role-arn "$role_arn" \
			--tags "$STACK_TAG_KEY=true" 2>&1)"; then
			log "pod identity: $namespace/$service_account now assumes $STACK_ROLE_NAME"
		else
			case "$associate_error" in
			*ResourceInUseException*) log "pod identity: $namespace/$service_account is already associated" ;;
			*) die "could not associate $namespace/$service_account with $STACK_ROLE_NAME: $associate_error" ;;
			esac
		fi
	done
}

# stack_unbind_identity <namespace> <service-account>... — the associations,
# then the role. Each step describes before it deletes, so a partial teardown
# re-run finishes rather than failing on what has already gone.
stack_unbind_identity() {
	local namespace=$1
	shift
	local service_account association_id detach_error
	for service_account in "$@"; do
		if ! association_id="$(aws eks list-pod-identity-associations --cluster-name "$CLUSTER_NAME" \
			--namespace "$namespace" --service-account "$service_account" \
			--query 'associations[0].associationId' --output text 2>&1)"; then
			die "could not list pod identity associations for $namespace/$service_account: $association_id"
		fi
		if [[ -n $association_id && $association_id != None ]]; then
			log "deleting pod identity association for $namespace/$service_account"
			aws eks delete-pod-identity-association --cluster-name "$CLUSTER_NAME" --association-id "$association_id" >/dev/null
		else
			log "no pod identity association for $namespace/$service_account"
		fi
	done
	if aws iam get-role --role-name "$STACK_ROLE_NAME" >/dev/null 2>&1; then
		log "deleting iam role $STACK_ROLE_NAME"
		if ! detach_error="$(aws iam delete-role-policy --role-name "$STACK_ROLE_NAME" --policy-name "$STACK_POLICY_NAME" 2>&1)"; then
			[[ $detach_error == *NoSuchEntity* ]] || die "could not remove policy $STACK_POLICY_NAME from $STACK_ROLE_NAME: $detach_error"
		fi
		aws iam delete-role --role-name "$STACK_ROLE_NAME"
	else
		log "iam role $STACK_ROLE_NAME is already gone"
	fi
}

# The brokers' StorageClass, applied and named in KAFKA_STORAGE_CLASS.
stack_storage_class() {
	export KAFKA_STORAGE_CLASS KAFKA_VOLUME_THROUGHPUT_MIBS KAFKA_VOLUME_IOPS
	log "applying StorageClass $KAFKA_STORAGE_CLASS (gp3, ${KAFKA_VOLUME_THROUGHPUT_MIBS} MiB/s, ${KAFKA_VOLUME_IOPS} iops)"
	envsubst '${KAFKA_STORAGE_CLASS} ${KAFKA_VOLUME_THROUGHPUT_MIBS} ${KAFKA_VOLUME_IOPS}' \
		<"$_STACK_HOOKS_DIR/k8s/kafka-storageclass.yaml.tmpl" |
		kubectl --context "$KUBE_CONTEXT" apply -f -
}

stack_delete_storage_class() {
	log "deleting StorageClass $KAFKA_STORAGE_CLASS"
	kubectl --context "$KUBE_CONTEXT" delete storageclass "$KAFKA_STORAGE_CLASS" --ignore-not-found
}

# What the catalog's chart needs of this cloud: the switches that let it read
# and write the warehouse as the pod's own identity without assuming a role,
# and the region under both names an SDK reads it as.
stack_catalog_settings() {
	STACK_CATALOG_CONFIG_JSON="$(jq -nc '{
		LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS: "true",
		LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS: "true"
	}')"
	STACK_CATALOG_ENV_JSON="$(jq -nc --arg region "$AWS_REGION" \
		'[{name: "AWS_REGION", value: $region}, {name: "AWS_DEFAULT_REGION", value: $region}]')"
}

# The warehouse's storage profile, on stdout. Vending and remote signing off:
# the harness and both engines reach the bucket as their own identity already,
# so the catalog stays off the data path.
stack_storage_profile_json() {
	jq -nc --arg bucket "$BUCKET" --arg region "$AWS_REGION" '{
		type: "s3",
		bucket: $bucket,
		"key-prefix": "warehouse",
		region: $region,
		flavor: "aws",
		"sts-enabled": false,
		"remote-signing-enabled": false
	}'
}

# stack_storage_credential_json <external-id> — how the catalog authenticates
# to the bucket: the identity the pod holds, which is the association above.
stack_storage_credential_json() {
	jq -nc --arg external_id "$1" '{type: "s3", "credential-type": "aws-system-identity", "external-id": $external_id}'
}
