#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Everything a run needs of a Kubernetes cluster that is to hold its own
# broker and catalog, and that no run creates for itself: the namespace and
# the identities the pods run as, the cloud's binding of those identities to
# the bucket, a StorageClass for the brokers, the Strimzi operator and a Kafka
# cluster under it, Lakekeeper with its Postgres, and the catalog's warehouse.
# It ends by printing the values to put in site.yaml.
#
# Every step describes before it creates, so a re-run converges rather than
# failing on what is already there. Everything cloud-specific is in one sourced
# file per cloud, deploy/<cloud>/_stack_hooks.sh, chosen by CLOUD.
#
# It never creates, deletes or reconfigures the cluster, its node groups or its
# add-ons. Those are the operator's; deploy/aws/eksctl-kafka-nodegroup.example.yaml
# makes the brokers' node group.
set -euo pipefail
PREREQ_DOC="deploy/k8s/stack/README.md"
STACK_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_lib.sh
source "$STACK_DIR/../../../scripts/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$REPO_ROOT/scripts/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: deploy/k8s/stack/setup.sh

  It takes no arguments; every parameter is an environment variable. CLOUD
  (aws) chooses the account-side hook, KUBE_CONTEXT the cluster, NAMESPACE
  where the stack and every run live, BUCKET the one bucket. The broker's
  size and placement are KAFKA_BROKERS, KAFKA_VOLUME_GI, KAFKA_CPU,
  KAFKA_MEM_GI, KAFKA_NODE_SELECTOR and KAFKA_TOLERATIONS; the catalog's
  placement is CATALOG_NODE_SELECTOR and CATALOG_TOLERATIONS.
  deploy/k8s/stack/README.md lists every variable and its default.
USAGE
}

while [[ $# -gt 0 ]]; do
	case "$1" in
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

# Before the tool check, so the answer for a cloud this has no hook for is
# that, and not a missing helmfile.
CLOUD="${CLOUD:-}"
case "$CLOUD" in
aws) ;;
gcp) die "CLOUD=gcp is not supported yet: the cloud seam is one sourced file, deploy/aws/_stack_hooks.sh, and a GCP hook is the next cloud — see $PREREQ_DOC" ;;
"") die "CLOUD must be set; aws is the one value this supports — see $PREREQ_DOC" ;;
*) die "CLOUD is '$CLOUD'; aws is the one value this supports — see $PREREQ_DOC" ;;
esac

KAFKA_BROKERS="${KAFKA_BROKERS:-3}"
KAFKA_VOLUME_GI="${KAFKA_VOLUME_GI:-500}"
KAFKA_CPU="${KAFKA_CPU:-4}"
KAFKA_MEM_GI="${KAFKA_MEM_GI:-16}"
KAFKA_JVM_HEAP="${KAFKA_JVM_HEAP:-6g}"
# One-line JSON, as the manifests take it. An empty selector is `{}`, which
# cannot be written as a `${VAR:-...}` default without escaping the brace.
KAFKA_NODE_SELECTOR="${KAFKA_NODE_SELECTOR:-}"
[[ -n $KAFKA_NODE_SELECTOR ]] || KAFKA_NODE_SELECTOR='{}'
KAFKA_TOLERATIONS="${KAFKA_TOLERATIONS:-[]}"
CATALOG_NODE_SELECTOR="${CATALOG_NODE_SELECTOR:-}"
[[ -n $CATALOG_NODE_SELECTOR ]] || CATALOG_NODE_SELECTOR='{}'
CATALOG_TOLERATIONS="${CATALOG_TOLERATIONS:-[]}"
# Pinned: the operator's CRD version and the fields the chart renders have to
# agree, and `latest` would move under a running campaign.
STRIMZI_VERSION="${STRIMZI_VERSION:-1.2.0}"
LAKEKEEPER_CHART_VERSION="${LAKEKEEPER_CHART_VERSION:-0.12.0}"
WITH_SCHEMA_REGISTRY="${WITH_SCHEMA_REGISTRY:-false}"
KAFKA_READY_WAIT_S="${KAFKA_READY_WAIT_S:-900}"
CATALOG_READY_WAIT_S="${CATALOG_READY_WAIT_S:-600}"

for knob in KAFKA_BROKERS KAFKA_VOLUME_GI KAFKA_CPU KAFKA_MEM_GI KAFKA_READY_WAIT_S CATALOG_READY_WAIT_S; do
	[[ ${!knob} =~ ^[1-9][0-9]*$ ]] || die "$knob must be a positive integer, got '${!knob}'"
done
[[ $KAFKA_JVM_HEAP =~ ^[0-9]+[gG]$ ]] || die "KAFKA_JVM_HEAP must be an integer number of gigabytes with a g suffix (e.g. 6g), got '$KAFKA_JVM_HEAP'"
[[ ${KAFKA_JVM_HEAP%[gG]} -lt $KAFKA_MEM_GI ]] ||
	die "KAFKA_JVM_HEAP $KAFKA_JVM_HEAP must be below KAFKA_MEM_GI ${KAFKA_MEM_GI}Gi; the rest of the pod's memory is page cache"

NAMESPACE="${NAMESPACE:-ingest-bench}"
KUBE_CONTEXT="${KUBE_CONTEXT:-${CLUSTER_NAME:-}}"
[[ -n $KUBE_CONTEXT ]] || die "KUBE_CONTEXT must name the kubeconfig context of the cluster (it defaults to CLUSTER_NAME)"

# Fixed names, shared with teardown.sh. The Kafka cluster's name is half of
# the bootstrap address; the ServiceAccount names are what the pod identity
# associations are made for, so a name that varied would need a new binding.
KAFKA_NAME=ingest-bench
WAREHOUSE_NAME=ingest-bench
CATALOG_SECRET=ingest-bench-catalog-keys
CATALOG_SERVICE_ACCOUNT=ingest-bench-catalog
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink
SPARK_SERVICE_ACCOUNT=ingest-bench-spark
STRIMZI_NAMESPACE=strimzi-operator
SPARK_OPERATOR_RELEASE=spark-operator
SPARK_OPERATOR_NAMESPACE=spark-operator

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

require_host_tools kubectl helm helmfile yq jq curl envsubst
# shellcheck source=deploy/aws/_stack_hooks.sh
source "$REPO_ROOT/deploy/$CLOUD/_stack_hooks.sh"

if ! NODES="$(kubectl --context "$KUBE_CONTEXT" get nodes -o name 2>&1)"; then
	die "kubectl --context $KUBE_CONTEXT cannot reach the cluster: $NODES"
fi
log "context $KUBE_CONTEXT, namespace $NAMESPACE, $(wc -l <<<"$NODES" | tr -d ' ') nodes"
stack_preflight
stack_preflight_storage

# Said rather than refused: a Flink campaign needs neither, and an operator
# shared with another namespace is not this script's to reconfigure.
if kubectl --context "$KUBE_CONTEXT" get crd flinkdeployments.flink.apache.org >/dev/null 2>&1; then
	log "the flinkdeployments CRD is present"
else
	log "warning: no flinkdeployments CRD on $KUBE_CONTEXT; a Flink run needs the operator, which deploy/aws/setup.sh installs"
fi
if SPARK_NAMESPACES="$(helm --kube-context "$KUBE_CONTEXT" get values "$SPARK_OPERATOR_RELEASE" \
	--namespace "$SPARK_OPERATOR_NAMESPACE" -o json 2>/dev/null | jq -r '.spark.jobNamespaces // [] | .[]')"; then
	if grep -qxF "$NAMESPACE" <<<"$SPARK_NAMESPACES"; then
		log "the spark-operator watches $NAMESPACE"
	else
		WATCHED="${SPARK_NAMESPACES//$'\n'/,}"
		log "warning: the spark-operator does not watch $NAMESPACE (it watches: ${WATCHED:-nothing}); a Spark run here needs:
     helm --kube-context $KUBE_CONTEXT upgrade $SPARK_OPERATOR_RELEASE spark-operator/spark-operator --namespace $SPARK_OPERATOR_NAMESPACE --reuse-values --set 'spark.jobNamespaces={${WATCHED:+$WATCHED,}$NAMESPACE}'"
	fi
else
	log "warning: no $SPARK_OPERATOR_RELEASE release on $KUBE_CONTEXT; a Spark run needs one, which deploy/aws/setup.sh installs"
fi

# ---------------------------------------------------------------------------
# The namespace and the identities
# ---------------------------------------------------------------------------

# The same manifest deploy/aws/setup.sh applies: its content is cloud-neutral,
# so it is rendered here rather than copied.
export NAMESPACE
log "applying the namespace, the three run identities and the engine RBAC"
envsubst '${NAMESPACE}' <"$REPO_ROOT/deploy/aws/k8s/namespace.yaml.tmpl" |
	kubectl --context "$KUBE_CONTEXT" apply -f -

stack_bind_identity "$NAMESPACE" "$HARNESS_SERVICE_ACCOUNT" "$FLINK_SERVICE_ACCOUNT" "$SPARK_SERVICE_ACCOUNT" "$CATALOG_SERVICE_ACCOUNT"

stack_storage_class

# ---------------------------------------------------------------------------
# The catalog's secrets
# ---------------------------------------------------------------------------

# A bounded read of urandom rather than a stream into `head`: under pipefail
# a writer cut off by its reader fails the substitution.
random_token() {
	head -c 64 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32
}

# Made once and never regenerated: the encryption key guards what the catalog
# has already stored, and the external id is part of the warehouse's
# credential. A re-run finds the Secret and leaves it.
if kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get secret "$CATALOG_SECRET" >/dev/null 2>&1; then
	log "secret $CATALOG_SECRET exists"
else
	log "creating secret $CATALOG_SECRET"
	kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" create secret generic "$CATALOG_SECRET" \
		--from-literal=encryptionKey="$(random_token)" \
		--from-literal=externalId="$(random_token)" \
		--from-literal=postgresUser=postgres \
		--from-literal=postgresPassword="$(random_token)" \
		--from-literal=pgDatabase=lakekeeper \
		--from-literal=pgUser=lakekeeper \
		--from-literal=pgPassword="$(random_token)"
	kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" label secret "$CATALOG_SECRET" lakehouse-ingest-bench=true
fi

# ---------------------------------------------------------------------------
# The releases
# ---------------------------------------------------------------------------

stack_catalog_settings
export STRIMZI_VERSION LAKEKEEPER_CHART_VERSION KAFKA_BROKERS KAFKA_VOLUME_GI KAFKA_STORAGE_CLASS KAFKA_CPU KAFKA_MEM_GI KAFKA_JVM_HEAP KAFKA_NODE_SELECTOR KAFKA_TOLERATIONS CATALOG_NODE_SELECTOR CATALOG_TOLERATIONS CATALOG_SECRET STACK_CATALOG_CONFIG_JSON STACK_CATALOG_ENV_JSON
log "helmfile sync: strimzi $STRIMZI_VERSION, kafka/$KAFKA_NAME ($KAFKA_BROKERS x ${KAFKA_VOLUME_GI}Gi on $KAFKA_STORAGE_CLASS), lakekeeper chart $LAKEKEEPER_CHART_VERSION"
# `sync` and not `apply`: apply diffs first through a helm plugin this does
# not require, and sync is the same install-or-upgrade either way.
helmfile --file "$STACK_DIR/helmfile.yaml.gotmpl" --kube-context "$KUBE_CONTEXT" sync

# Helm's wait sees the operator's Deployment and the catalog's; the Kafka CR's
# readiness is a condition only the operator sets.
log "waiting up to ${KAFKA_READY_WAIT_S}s for kafka/$KAFKA_NAME to be Ready"
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" wait "kafka/$KAFKA_NAME" \
	--for=condition=Ready --timeout="${KAFKA_READY_WAIT_S}s" ||
	die "kafka/$KAFKA_NAME is not Ready after ${KAFKA_READY_WAIT_S}s. A broker pod that is Pending usually has no volume (the StorageClass and the CSI driver) or no node (KAFKA_NODE_SELECTOR / KAFKA_TOLERATIONS against the node group); try: kubectl --context $KUBE_CONTEXT -n $NAMESPACE describe kafka $KAFKA_NAME"
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" rollout status deployment/lakekeeper --timeout "${CATALOG_READY_WAIT_S}s"

# ---------------------------------------------------------------------------
# The catalog's bootstrap and warehouse
# ---------------------------------------------------------------------------

# mgmt_post <path> <json> — one management call, refused with the catalog's
# own answer: `curl -f` would hide the body that says what was wrong.
mgmt_post() {
	local status body
	body="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-mgmt.XXXXXX")" || die "could not make a temporary file for the catalog's answer"
	status="$(curl -s -o "$body" -w '%{http_code}' -X POST "$MGMT$1" -H 'content-type: application/json' -d "$2")" ||
		die "could not reach the catalog through the tunnel for POST $1"
	if [[ $status != 2* ]]; then
		log "POST $1 answered $status: $(cat "$body")"
		rm -f "$body"
		return 1
	fi
	rm -f "$body"
}

trap k8s_port_forward_stop EXIT
k8s_port_forward svc/lakekeeper "$CATALOG_FORWARD_PORT:8181" "$NAMESPACE" /health
MGMT="http://localhost:$CATALOG_FORWARD_PORT/management/v1"

if [[ "$(curl -sf "$MGMT/info" | jq -r .bootstrapped)" == true ]]; then
	log "the catalog is bootstrapped"
else
	log "bootstrapping the catalog"
	mgmt_post /bootstrap '{"accept-terms-of-use": true}' || die "the catalog refused its bootstrap; the line above is its answer"
fi

if curl -sf "$MGMT/warehouse" | jq -e --arg name "$WAREHOUSE_NAME" '.warehouses[] | select(.name == $name)' >/dev/null; then
	log "warehouse $WAREHOUSE_NAME exists"
else
	EXTERNAL_ID="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get secret "$CATALOG_SECRET" \
		-o jsonpath='{.data.externalId}' | base64 -d)"
	REQUEST="$(jq -nc --arg name "$WAREHOUSE_NAME" \
		--argjson profile "$(stack_storage_profile_json)" \
		--argjson credential "$(stack_storage_credential_json "$EXTERNAL_ID")" '{
		"warehouse-name": $name,
		"storage-profile": $profile,
		"storage-credential": $credential,
		"delete-profile": {type: "hard"}
	}')"
	log "creating warehouse $WAREHOUSE_NAME over s3://$BUCKET/warehouse"
	mgmt_post /warehouse "$REQUEST" ||
		die "the catalog refused the warehouse; the line above is its answer. A denied bucket read or write means the catalog pod started before its identity was bound — kubectl --context $KUBE_CONTEXT -n $NAMESPACE rollout restart deployment/lakekeeper, then re-run"
fi
k8s_port_forward_stop

# ---------------------------------------------------------------------------
# The schema registry, when asked for
# ---------------------------------------------------------------------------

if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
	log "applying the schema registry"
	sed -e "s|__NAMESPACE__|$NAMESPACE|g" \
		-e "s|__NODE_SELECTOR__|$CATALOG_NODE_SELECTOR|g" \
		-e "s|__TOLERATIONS__|$CATALOG_TOLERATIONS|g" \
		<"$REPO_ROOT/deploy/k8s/schema-registry.yaml.tmpl" |
		kubectl --context "$KUBE_CONTEXT" apply --namespace "$NAMESPACE" -f -
	kubectl --context "$KUBE_CONTEXT" rollout status deployment/schema-registry --namespace "$NAMESPACE" --timeout 300s
else
	log "no schema registry (WITH_SCHEMA_REGISTRY is '$WITH_SCHEMA_REGISTRY')"
fi

# ---------------------------------------------------------------------------
# What to put in site.yaml
# ---------------------------------------------------------------------------

log "setup complete. Copy site.k8s.example.yaml to site.yaml and fill it in with:"
# The roots and the region below are AWS's shape; a second cloud extends this
# block when it lands.
cat <<SITE
  kafka.bootstrap_servers:        $KAFKA_NAME-kafka-bootstrap.$NAMESPACE.svc:9092
  kafka.security:                 {}
  corpus_root / runs_root / warehouse: s3://$BUCKET/{corpus,runs,warehouse}
  catalog.props.uri:              http://lakekeeper.$NAMESPACE.svc:8181/catalog
  catalog.props.warehouse:        $WAREHOUSE_NAME
  catalog.props.s3.region:        $AWS_REGION
  kubernetes.context:             $KUBE_CONTEXT
  kubernetes.namespace:           $NAMESPACE
  kubernetes.aws_region:          $AWS_REGION
SITE
if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
	cat <<SITE
  kafka.schema_registry.url:      http://schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7
SITE
fi
log "the brokers' nodes and volumes bill whether or not a run is using them — deploy/k8s/stack/teardown.sh when you are done"
