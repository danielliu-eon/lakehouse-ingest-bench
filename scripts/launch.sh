#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Start the scorer, wait for its baseline reading, then start producer shards. This keeps
# the first sample free of rows committed before scoring began.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Leave time for producer scheduling and image pulls before the first batch is due. A late
# start can mark the run producer_bound.
EPOCH_LEAD_S="${EPOCH_LEAD_S:-180}"
# Stop scoring after this long without a commit while rows remain outstanding.
IDLE_STOP_S="${IDLE_STOP_S:-600}"
# Timeout and polling interval for the scorer's baseline reading.
FIRST_POLL_WAIT_S="${FIRST_POLL_WAIT_S:-300}"
FIRST_POLL_S="${FIRST_POLL_S:-5}"
# Each shard buffers and decompresses a whole batch, so memory scales with
# offered_bytes_per_s * batch_interval_ms / 1000, not shard count. The default fits smoke;
# see docs/running.md for larger presets.
PRODUCER_MEMORY="${PRODUCER_MEMORY:-2Gi}"
# Leave unset to use the scorer's own read-worker default.
SCORER_READ_WORKERS="${SCORER_READ_WORKERS:-}"

usage() {
	cat <<'USAGE'
usage: scripts/launch.sh <run_id> [options]

  <run_id>           staged run with a directory under $RUNS_DIR
  --site PATH        site config for the cluster and the runs prefix (default: ./site.yaml)
  --image-tag TAG    the harness image tag the producer and the scorer run (default: this checkout's commit)

Environment: EPOCH_LEAD_S, IDLE_STOP_S, FIRST_POLL_WAIT_S, FIRST_POLL_S,
PRODUCER_MEMORY, SCORER_READ_WORKERS, RUNS_DIR.
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

require_host_tools kubectl yq jq git

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
SPEC="$RUN_DIR/spec.yaml"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; run scripts/stage.sh first, or set RUNS_DIR"
[[ -f $SPEC ]] || die "$RUN_DIR has no spec.yaml; cannot read the run configuration"

k8s_read_site
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

BOOTSTRAP="$(jq -r .bootstrap "$FACTS")"
CORPUS_URI="$(jq -r .corpus_uri "$FACTS")"
TABLE="$(jq -r .table "$FACTS")"
# Use the staged topic name instead of deriving it from the run ID.
TOPIC="$(jq -r .topic "$FACTS")"
[[ -n $TOPIC && $TOPIC != null ]] || die "$FACTS has no topic for the producer"
# Omit --key-column for unkeyed records.
KEY_COLUMN="$(jq -r '.key_column // empty' "$FACTS")"
# Use the encoding and schema ID resolved at staging. Raw Avro has no schema ID.
VALUE_ENCODING="$(jq -r '.value_encoding // empty' "$FACTS")"
SCHEMA_ID="$(jq -r '.schema_id // empty' "$FACTS")"

# Pass configured producer options and leave omitted options to the command defaults.
SHARDS="$(yq '.producer.shards' "$SPEC")"
if [[ $SHARDS == null ]]; then
	SHARDS=1
fi
[[ $SHARDS =~ ^[1-9][0-9]*$ ]] || die "$SPEC producer.shards must be a positive integer; got '$SHARDS'"
SPEED="$(yq '.producer.speed' "$SPEC")"
REPLAY_SECONDS="$(yq '.producer.seconds' "$SPEC")"
BEHIND_MAX_MS="$(yq '.producer.behind_max_ms' "$SPEC")"
COMPRESSION="$(yq '.producer.compression' "$SPEC")"

EPOCH=$(($(date +%s) + EPOCH_LEAD_S))

# ---------------------------------------------------------------------------
# Whether this cluster has room for the run's own pods
# ---------------------------------------------------------------------------

# Must match CPU requests in the producer and scorer templates; tests check this.
POD_CPU_MILLICORES=2000

# Warn before launch if current CPU requests leave too little room. This is best-effort:
# autoscaling may add nodes, and namespace-scoped credentials may not permit listing them.
warn_if_the_pods_will_not_fit() {
	local scratch="" nodes="" pods="" free="" needed=$((1 + SHARDS))
	scratch="$(mktemp -d "${TMPDIR:-/tmp}/ingest-bench-launch.XXXXXX")" || return 0
	nodes="$scratch/nodes.json"
	pods="$scratch/pods.json"
	if kubectl --context "$KUBE_CONTEXT" get nodes -o json >"$nodes" 2>/dev/null &&
		kubectl --context "$KUBE_CONTEXT" get pods --all-namespaces -o json >"$pods" 2>/dev/null; then
		free="$(k8s_nodes_with_free_cpu "$POD_CPU_MILLICORES" "$nodes" "$pods")" || free=""
	fi
	rm -rf "$scratch"
	[[ -n $free ]] || return 0
	((free < needed)) || return 0
	log "$free of this cluster's nodes have ${POD_CPU_MILLICORES}m of CPU free, and this run needs $needed: the scorer and $SHARDS producer shard(s)"
	log "each of these pods requests ${POD_CPU_MILLICORES}m, so a node fits one unless it has twice that free; a pod nothing can schedule stays Pending with no log of its own — see $PREREQ_DOC §Sizing the cluster"
}

warn_if_the_pods_will_not_fit

# ---------------------------------------------------------------------------
# The scorer
# ---------------------------------------------------------------------------

SCORER_JOB="$(scorer_job "$RUN_ID")"
SCORE=(score --corpus "$CORPUS_URI" --table "$TABLE"
	--publish-logs "$RUNS_ROOT/$RUN_ID/producer" --epoch "$EPOCH"
	--out /work/scores --upload-prefix "$RUNS_ROOT/$RUN_ID/scores"
	--idle-stop-s "$IDLE_STOP_S" --publish-shards "$SHARDS")
# An engine-managed table may not exist before the first record.
MANAGED_BY="$(yq '.table.managed_by' "$SPEC")"
[[ $MANAGED_BY == null ]] || SCORE+=(--table-managed-by "$MANAGED_BY")
read_site_prop_flags '.catalog.props' --catalog-prop
SCORE+=(${SITE_PROP_FLAGS[@]+"${SITE_PROP_FLAGS[@]}"})
for key in warmup_s freshness_bound_s; do
	value="$(yq ".scoring.$key" "$SPEC")"
	[[ $value == null ]] || SCORE+=("--${key//_/-}" "$value")
done
[[ $SPEED == null ]] || SCORE+=(--speed "$SPEED")
[[ $BEHIND_MAX_MS == null ]] || SCORE+=(--behind-max-ms "$BEHIND_MAX_MS")
[[ -z $SCORER_READ_WORKERS ]] || SCORE+=(--read-workers "$SCORER_READ_WORKERS")

log "starting the scorer as job/$SCORER_JOB (epoch $EPOCH, idle stop ${IDLE_STOP_S}s)"
k8s_delete job "$SCORER_JOB"
k8s_render_apply deploy/k8s/scorer-job.yaml.tmpl \
	"NAME=$SCORER_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$(job_command_json "${SCORE[@]}")" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"

log "waiting up to ${FIRST_POLL_WAIT_S}s for job/$SCORER_JOB to take its first reading"
deadline=$((SECONDS + FIRST_POLL_WAIT_S))
while :; do
	# Capture before grep -q: an early pipe close would otherwise cause SIGPIPE under
	# pipefail.
	logs="$(k8s_job_logs "$SCORER_JOB" 2>/dev/null || true)"
	if grep -q '^POLL ' <<<"$logs"; then
		log "the scorer is reading the table"
		break
	fi
	if ((SECONDS >= deadline)); then
		k8s_job_tail "$SCORER_JOB"
		die "job/$SCORER_JOB published no reading within ${FIRST_POLL_WAIT_S}s; see the logs and pod events above"
	fi
	remaining=$((deadline - SECONDS))
	sleep "$((remaining < FIRST_POLL_S ? remaining : FIRST_POLL_S))"
done

# ---------------------------------------------------------------------------
# The offer
# ---------------------------------------------------------------------------

# The baseline wait may consume the epoch lead. Refuse if the producer no longer has time
# to start before its first batch is due.
(($(date +%s) + 30 <= EPOCH)) || die "epoch $EPOCH is under 30s away; raise EPOCH_LEAD_S (currently $EPOCH_LEAD_S) and relaunch"

PRODUCER_JOB="$(producer_job "$RUN_ID")"
# Only the shard index needs shell expansion; all configured values remain argv.
PRODUCE=(/bin/sh -c 'exec produce --shard "$JOB_COMPLETION_INDEX" --publish-log "/work/publish_log-$JOB_COMPLETION_INDEX.jsonl" "$@"' --
	--corpus "$CORPUS_URI" --bootstrap "$BOOTSTRAP" --topic "$TOPIC" --epoch "$EPOCH"
	--shards "$SHARDS" --upload-prefix "$RUNS_ROOT/$RUN_ID")
read_site_prop_flags '.kafka.security' --kafka-prop
PRODUCE+=(${SITE_PROP_FLAGS[@]+"${SITE_PROP_FLAGS[@]}"})
[[ -z $KEY_COLUMN ]] || PRODUCE+=(--key-column "$KEY_COLUMN")
[[ -z $VALUE_ENCODING ]] || PRODUCE+=(--value-encoding "$VALUE_ENCODING")
[[ -z $SCHEMA_ID ]] || PRODUCE+=(--schema-id "$SCHEMA_ID")
[[ $SPEED == null ]] || PRODUCE+=(--speed "$SPEED")
[[ $REPLAY_SECONDS == null ]] || PRODUCE+=(--seconds "$REPLAY_SECONDS")
[[ $BEHIND_MAX_MS == null ]] || PRODUCE+=(--behind-max-ms "$BEHIND_MAX_MS")
[[ $COMPRESSION == null ]] || PRODUCE+=(--compression "$COMPRESSION")

log "offering the corpus from $SHARDS shard(s) as job/$PRODUCER_JOB"
k8s_delete job "$PRODUCER_JOB"
k8s_render_apply deploy/k8s/producer-job.yaml.tmpl \
	"NAME=$PRODUCER_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$(job_command_json "${PRODUCE[@]}")" \
	"COUNT=$SHARDS" \
	"MEMORY=$PRODUCER_MEMORY" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"

# Record the launch epoch for later readers.
write_launch_epoch "$FACTS" "$EPOCH"
printf '%s launched epoch=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$EPOCH" >>"$RUN_DIR/timeline.log"

log "launched $RUN_ID at epoch $EPOCH; judge it with scripts/gate.sh $RUN_ID"
