#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Stop the managed engine, producer and scorer, then drop the Kafka topic from a cluster
# Job. Keep the table, warehouse data and result artifacts. Use purge.sh to delete
# measured data after reviewing the run.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Allow scheduling, image pull and broker confirmation for topic deletion.
DROP_WAIT_S="${DROP_WAIT_S:-600}"

usage() {
	cat <<'USAGE'
usage: scripts/teardown.sh <run_id> [options]

  <run_id>           a run whose directory is under $RUNS_DIR
  --site PATH        site config for the cluster and the broker (default: ./site.yaml)
  --image-tag TAG    the harness image tag the drop-topic Job runs (default: this checkout's commit)
  --keep-table       accepted for compatibility; teardown always preserves table data.
                     Use `purge.sh <run_id>` to delete it

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
		# Accepted for clarity; this script always preserves the table.
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
		[[ -z $RUN_ID ]] || die "expected one run ID; got '$RUN_ID' and '$1'"
		RUN_ID="$1"
		shift
		;;
	esac
done

[[ -n $RUN_ID ]] || {
	printf 'a run ID is required\n\n' >&2
	usage >&2
	exit 2
}

require_host_tools kubectl aws yq jq git gzip

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; use the checkout where stage.sh saved it, or set RUNS_DIR"

k8s_read_site
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

BOOTSTRAP="$(jq -r .bootstrap "$FACTS")"
TABLE="$(jq -r .table "$FACTS")"
# Use the staged topic name instead of deriving it from the run ID.
TOPIC="$(jq -r .topic "$FACTS")"
[[ -n $TOPIC && $TOPIC != null ]] || die "$FACTS has no topic; cannot determine which topic to delete for $RUN_ID"

# Read the staged spec to identify the engine to stop.
SPEC="$RUN_DIR/spec.yaml"
[[ -f $SPEC ]] || die "no spec at $SPEC; cannot determine which engine to stop for $RUN_ID"
ENGINE="$(yq '.engine' "$SPEC")"
[[ -n $ENGINE && $ENGINE != null ]] || die "$SPEC has no engine; cannot determine what to stop"

# ---------------------------------------------------------------------------
# 1. The engine
# ---------------------------------------------------------------------------

if [[ $ENGINE == external ]]; then
	log "external engine: stop it manually"
else
	k8s_read_engine "$ENGINE" "$(k8s_object_name "$RUN_ID")"

	# Capture missing provenance before deleting the engine. Do not wait for a digest here;
	# staging normally records it once the fleet is running.
	if [[ ! -f $RUN_DIR/$ENGINE_IMAGE_FILE ]]; then
		k8s_write_engine_image "$RUN_DIR/$ENGINE_IMAGE_FILE" "$ENGINE_PROVENANCE_SELECTOR" 0
	fi

	# Delete engine resources before the ConfigMap their pods mount.
	for document in "$ENGINE_DOCUMENT_FILE" "$ENGINE_CONFIGMAP_FILE"; do
		if [[ -f $RUN_DIR/$document ]]; then
			log "deleting resources declared in $document"
			k8s_delete_file "$RUN_DIR/$document"
		fi
	done
fi

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
read_site_prop_flags '.kafka.security' --kafka-prop
DROP_COMMAND=(drop-topic --bootstrap "$BOOTSTRAP" --topic "$TOPIC" ${SITE_PROP_FLAGS[@]+"${SITE_PROP_FLAGS[@]}"})
log "dropping topic $TOPIC as job/$DROP_JOB"
k8s_delete job "$DROP_JOB"
k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
	"NAME=$DROP_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$(job_command_json "${DROP_COMMAND[@]}")" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$DROP_JOB" "$DROP_WAIT_S"
k8s_delete job "$DROP_JOB"

# ---------------------------------------------------------------------------
# 4. The scorer's artifacts
# ---------------------------------------------------------------------------

# Fetch scores for collection below. finish.sh fetches again to include any final uploads.
SCORES="$RUN_DIR/scores"
mkdir -p "$SCORES"
log "fetching $RUNS_ROOT/$RUN_ID/scores/ into $SCORES"
aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/" "$SCORES/" --only-show-errors >&2 ||
	log "could not fetch $RUNS_ROOT/$RUN_ID/scores/; the report below will list missing artifacts"

# ---------------------------------------------------------------------------
# 5. The table's last metadata document
# ---------------------------------------------------------------------------

# Save the final metadata document for later geometry and location reads, even if the
# catalog entry is removed. It references manifests; it does not copy them.
# Install tunnel cleanup before reading catalog properties.
trap k8s_port_forward_stop EXIT
read_catalog_prop_flags

# An absent table needs no metadata copy. Report other errors because they do not
# establish absence.
METADATA_STATUS=0
METADATA="$(harness_local --extra aws table-metadata --table "$TABLE" ${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"})" ||
	METADATA_STATUS=$?
if ((METADATA_STATUS == 0)); then
	LOCAL_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
	FINAL="$RUNS_ROOT/$RUN_ID/$METADATA_FINAL_FILE"
	# Save locally for finish and purge, then upload the same document for durable storage.
	log "copying $TABLE's metadata document to $LOCAL_FINAL and $FINAL"
	if k8s_fetch_metadata_document "$METADATA" "$LOCAL_FINAL"; then
		aws s3 cp "$LOCAL_FINAL" "$FINAL" --only-show-errors >&2 || log "could not copy $LOCAL_FINAL to $FINAL; the local copy is available"
	else
		log "could not copy $METADATA to $LOCAL_FINAL; the source metadata is unchanged"
	fi
elif ((METADATA_STATUS == TABLE_ABSENT)); then
	log "no table $TABLE in the catalog, so there is no metadata document to copy"
else
	die "could not read $TABLE's metadata document: table-metadata exited $METADATA_STATUS; see the error above"
fi

# ---------------------------------------------------------------------------
# 6. The run's document
# ---------------------------------------------------------------------------

# Collect a result even if finish is never called. Geometry and producer logs may still be
# missing. Report collection failures without treating completed resource teardown as a
# failure; collect can be retried separately.
log "collect will report scores/geometry.json and publish logs as missing until finish.sh fetches the logs and measures geometry"
log "collecting $RUN_ID"
harness_local --extra aws collect --run-dir "$(abs_path "$RUN_DIR")" --site "$(abs_path "$SITE_FILE")" ||
	log "could not collect $RUN_ID; rerun: collect --run-dir $RUN_DIR --site $SITE_FILE"

log "torn down $RUN_ID; its table and the warehouse data are untouched"
