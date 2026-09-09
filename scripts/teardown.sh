#!/usr/bin/env bash
# Stop everything a run started and drop its topic, keeping everything a result
# is made of.
#
# The order is what each deletion needs of the one before it: the engine goes
# first so nothing is still consuming, then the producer and the scorer, then
# the topic — from inside the cluster, because a managed broker is reachable
# from its own network and not from an operator's machine.
#
# The table and the warehouse data are never deleted. A run's table is its
# result, and a teardown that dropped tables on its own would be a worse
# failure mode than an orphan; `drop-table` does it by hand.
set -euo pipefail
# The tools a missing prerequisite points at.
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Dropping a topic is one controller round trip plus the wait for every broker
# to agree, inside a pod that has to be scheduled and pull an image.
DROP_WAIT_S="${DROP_WAIT_S:-600}"

# What `table-metadata` exits when the catalog holds no such table, as
# ingest_bench.table.cli.TABLE_ABSENT. Any other non-zero exit is a catalog this
# script could not reach, which is a failure and not an absent table.
TABLE_ABSENT=3

usage() {
	cat <<'USAGE'
usage: scripts/teardown.sh <run_id> [options]

  <run_id>           a run whose directory is under $RUNS_DIR
  --site PATH        the site config naming the cluster and the broker (default: ./site.yaml)
  --image-tag TAG    the harness image tag the drop-topic Job runs (default: this checkout's commit)
  --keep-table       accepted, and already what happens: the table and the warehouse data
                     are never deleted here. Drop one with `drop-table --table <t>`

Environment: DROP_WAIT_S, RUNS_DIR.
USAGE
}

RUN_ID=""
IMAGE_TAG=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--image-tag)
		IMAGE_TAG="${2:?--image-tag needs a tag}"
		shift 2
		;;
	--keep-table)
		# Nothing to set: the table is always kept. The flag exists because a
		# reader looking for the switch that protects it should find it, and
		# find that it is already the only behaviour.
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	-*)
		printf 'unknown argument %s\n\n' "$1" >&2
		usage >&2
		exit 2
		;;
	*)
		[[ -z $RUN_ID ]] || die "this tears one run down, and was given both '$RUN_ID' and '$1'"
		RUN_ID="$1"
		shift
		;;
	esac
done

[[ -n $RUN_ID ]] || {
	printf 'a run id is required\n\n' >&2
	usage >&2
	exit 2
}

require_host_tools kubectl aws yq jq git

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; run this where stage.sh fetched it, or set RUNS_DIR"

k8s_read_site
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

BOOTSTRAP="$(jq -r .bootstrap "$FACTS")"
TABLE="$(jq -r .table "$FACTS")"

# ---------------------------------------------------------------------------
# 1. The engine
# ---------------------------------------------------------------------------

# The deployment before the ConfigMap it mounts: a JobManager that restarted
# between the two would find no volume and report that instead of stopping.
# An external run rendered neither document, and there is then nothing here of
# ours to delete.
for document in flinkdeployment.yaml flink-job-configmap.yaml; do
	if [[ -f $RUN_DIR/$document ]]; then
		log "deleting what $document declares"
		k8s_delete_file "$RUN_DIR/$document"
	fi
done

# ---------------------------------------------------------------------------
# 2. The producer and the scorer
# ---------------------------------------------------------------------------

log "deleting the producer and scorer Jobs"
k8s_delete job "$(producer_job "$RUN_ID")"
k8s_delete job "$(scorer_job "$RUN_ID")"

# ---------------------------------------------------------------------------
# 3. The topic
# ---------------------------------------------------------------------------

DROP_JOB="drop-topic-$(k8s_object_name "$RUN_ID")"
DROP_COMMAND="drop-topic --bootstrap $BOOTSTRAP --topic $RUN_ID$(site_flags '.kafka.security' --kafka-prop)"
log "dropping topic $RUN_ID as job/$DROP_JOB"
k8s_delete job "$DROP_JOB"
k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
	"NAME=$DROP_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$DROP_COMMAND" \
	"ENV=$JOB_ENV" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$DROP_JOB" "$DROP_WAIT_S"
k8s_delete job "$DROP_JOB"

# ---------------------------------------------------------------------------
# 4. The table's last metadata document
# ---------------------------------------------------------------------------

# Copied beside the run's other artifacts because it is the whole of what the
# table looked like when the run ended: its schema, its properties, its
# snapshot history and the manifests behind them. The table itself stays where
# it is, and a later campaign may drop it — the copy is what survives that.
# Assigned before it is read: a `site_pairs` that refused would fail this
# assignment, where a command substitution inside the loop's redirection below
# would read as an empty map and open a catalog with no properties.
CATALOG_PAIRS="$(site_pairs '.catalog.props')"
CATALOG_PROPS=()
while IFS= read -r pair; do
	[[ -n $pair ]] || continue
	CATALOG_PROPS+=(--catalog-prop "$pair")
done <<<"$CATALOG_PAIRS"

# A teardown converges over an absent table: a run that failed before it created
# one has no document to copy, and refusing there would leave the topic dropped
# and the teardown reported as failed. Every other failure — an unreachable
# catalog, expired credentials, a harness that is not installed — is reported,
# because it says nothing about whether the document exists.
METADATA_STATUS=0
METADATA="$(harness_local --extra aws table-metadata --table "$TABLE" ${CATALOG_PROPS[@]+"${CATALOG_PROPS[@]}"})" ||
	METADATA_STATUS=$?
if ((METADATA_STATUS == 0)); then
	FINAL="$RUNS_ROOT/$RUN_ID/table-metadata.final.json"
	log "copying $TABLE's metadata document to $FINAL"
	aws s3 cp "$METADATA" "$FINAL" >&2 || log "could not copy $METADATA to $FINAL; the table still holds it"
elif ((METADATA_STATUS == TABLE_ABSENT)); then
	log "no table $TABLE in the catalog, so there is no metadata document to copy"
else
	die "could not read $TABLE's metadata document: table-metadata exited $METADATA_STATUS; the lines above are its own error"
fi

log "torn down $RUN_ID; its table and the warehouse data are untouched"
