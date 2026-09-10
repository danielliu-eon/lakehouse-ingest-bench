#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
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
# failure mode than an orphan; `purge.sh` reclaims them once it has been read.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Dropping a topic is one controller round trip plus the wait for every broker
# to agree, inside a pod that has to be scheduled and pull an image.
DROP_WAIT_S="${DROP_WAIT_S:-600}"

usage() {
	cat <<'USAGE'
usage: scripts/teardown.sh <run_id> [options]

  <run_id>           a run whose directory is under $RUNS_DIR
  --site PATH        the site config naming the cluster and the broker (default: ./site.yaml)
  --image-tag TAG    the harness image tag the drop-topic Job runs (default: this checkout's commit)
  --keep-table       accepted, and already what happens: the table and the warehouse data
                     are never deleted here. `purge.sh <run_id>` is what reclaims them

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

require_host_tools kubectl aws yq jq git gzip

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; run this where stage.sh fetched it, or set RUNS_DIR"

k8s_read_site
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

BOOTSTRAP="$(jq -r .bootstrap "$FACTS")"
TABLE="$(jq -r .table "$FACTS")"
# The topic staging created, rather than the run id it was named after: a
# teardown that rebuilt the name would drop a topic of its own the day the two
# stop being the same string, and leave this run's behind.
TOPIC="$(jq -r .topic "$FACTS")"
[[ -n $TOPIC && $TOPIC != null ]] || die "$FACTS names no topic, so nothing here knows which topic $RUN_ID published to"

# The copied spec, because which engine a run started is what says how to stop
# it — and the run directory is the record of what was asked for, so it is read
# rather than guessed from the documents that happen to be beside it.
SPEC="$RUN_DIR/spec.yaml"
[[ -f $SPEC ]] || die "no spec at $SPEC, so nothing here knows which engine $RUN_ID started"
ENGINE="$(yq '.engine' "$SPEC")"
[[ -n $ENGINE && $ENGINE != null ]] || die "$SPEC sets no engine, so nothing here knows what to stop"

# ---------------------------------------------------------------------------
# 1. The engine
# ---------------------------------------------------------------------------

if [[ $ENGINE == external ]]; then
	log "an external run's engine is yours, so there is none of ours to delete"
else
	k8s_read_engine "$ENGINE" "$(k8s_object_name "$RUN_ID")"

	# Provenance before the deletion that ends the chance of reading it.
	# `stage.sh` records this when the fleet reaches its running state and this
	# is the fallback to that: a run staged by a driver that failed after the
	# engine started still gets the digest of the image that ran. The wait is
	# nought seconds, because the answer is either already reported or gone —
	# nothing here starts a pod.
	if [[ ! -f $RUN_DIR/$ENGINE_IMAGE_FILE ]]; then
		k8s_write_engine_image "$RUN_DIR/$ENGINE_IMAGE_FILE" "$ENGINE_PROVENANCE_SELECTOR" 0
	fi

	# The engine's own document before the ConfigMap it mounts: a pod that
	# restarted between the two would find no volume and report that instead of
	# stopping.
	for document in "$ENGINE_DOCUMENT_FILE" "$ENGINE_CONFIGMAP_FILE"; do
		if [[ -f $RUN_DIR/$document ]]; then
			log "deleting what $document declares"
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
DROP_COMMAND="drop-topic --bootstrap $BOOTSTRAP --topic $TOPIC$(site_flags '.kafka.security' --kafka-prop)"
log "dropping topic $TOPIC as job/$DROP_JOB"
k8s_delete job "$DROP_JOB"
k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
	"NAME=$DROP_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$DROP_COMMAND" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$DROP_JOB" "$DROP_WAIT_S"
k8s_delete job "$DROP_JOB"

# ---------------------------------------------------------------------------
# 4. The scorer's artifacts
# ---------------------------------------------------------------------------

# Fetched now rather than left to `finish.sh`, because the document collected
# below is assembled from them and the pod that wrote them is gone. `finish.sh`
# fetches them again — the scorer's last write may land after this — and both
# reads are of the same prefix, so the second one only ever adds.
SCORES="$RUN_DIR/scores"
mkdir -p "$SCORES"
log "fetching $RUNS_ROOT/$RUN_ID/scores/ into $SCORES"
aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/" "$SCORES/" >&2 ||
	log "could not fetch $RUNS_ROOT/$RUN_ID/scores/; the document below will say which artifacts are missing"

# ---------------------------------------------------------------------------
# 5. The table's last metadata document
# ---------------------------------------------------------------------------

# Copied beside the run's other artifacts because it is the whole of what the
# table looked like when the run ended: its schema, its properties, its
# snapshot history and the manifests behind them. The table itself stays where
# it is, and a later campaign may drop it — the copy is what survives that.
read_catalog_prop_flags

# A teardown converges over an absent table: a run that failed before it created
# one has no document to copy, and refusing there would leave the topic dropped
# and the teardown reported as failed. Every other failure — an unreachable
# catalog, expired credentials, a harness that is not installed — is reported,
# because it says nothing about whether the document exists.
METADATA_STATUS=0
METADATA="$(harness_local --extra aws table-metadata --table "$TABLE" ${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"})" ||
	METADATA_STATUS=$?
if ((METADATA_STATUS == 0)); then
	LOCAL_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
	FINAL="$RUNS_ROOT/$RUN_ID/$METADATA_FINAL_FILE"
	# Both copies, and the local one first so that the second is an upload of a
	# document already in hand rather than a second read of the table's. Each
	# has its own reader: `finish.sh` and `purge.sh` open the local one, and the
	# one in the bucket is what outlives this machine's working directory.
	log "copying $TABLE's metadata document to $LOCAL_FINAL and $FINAL"
	if k8s_fetch_metadata_document "$METADATA" "$LOCAL_FINAL"; then
		aws s3 cp "$LOCAL_FINAL" "$FINAL" >&2 || log "could not copy $LOCAL_FINAL to $FINAL; it is still on this machine"
	else
		log "could not copy $METADATA to $LOCAL_FINAL; the table still holds it"
	fi
elif ((METADATA_STATUS == TABLE_ABSENT)); then
	log "no table $TABLE in the catalog, so there is no metadata document to copy"
else
	die "could not read $TABLE's metadata document: table-metadata exited $METADATA_STATUS; the lines above are its own error"
fi

# ---------------------------------------------------------------------------
# 6. The run's document
# ---------------------------------------------------------------------------

# Collected here so a run has a result document even if nothing is ever done
# with it again. It carries no geometry yet — that is `finish.sh`, which
# measures it and collects a second time over the same directory.
#
# A failure is reported and not fatal: everything destructive above has already
# happened, so exiting non-zero here would report a teardown that did not
# converge when in fact it did, and the fix is to rerun `collect` alone.
log "collecting $RUN_ID"
harness_local --extra aws collect --run-dir "$(abs_path "$RUN_DIR")" --site "$(abs_path "$SITE_FILE")" ||
	log "could not collect $RUN_ID; rerun: collect --run-dir $RUN_DIR --site $SITE_FILE"

log "torn down $RUN_ID; its table and the warehouse data are untouched"
