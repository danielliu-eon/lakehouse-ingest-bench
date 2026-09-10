#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Start the offer and the reader of a staged run: the scorer first, the producer
# shards once the scorer has taken a reading.
#
# In that order because the scorer's first reading is the run's baseline. A
# producer that began publishing before the table was read would have rows
# already committed by the first sample, and the offered-against-committed
# curve would start part way up.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# How far in the future the run's time origin is put. Six times the local
# stack's lead, because on a cluster the first batch is due after a pod has been
# scheduled onto a node that may have to be provisioned and has to pull an
# image. A first batch that was already due when the producer opened its first
# connection is acked late, and the scorer reads a late ack as producer_bound —
# which voids the run rather than measuring the engine.
EPOCH_LEAD_S="${EPOCH_LEAD_S:-180}"
# How long the scorer waits for a commit before it gives up on a table with
# rows still outstanding.
IDLE_STOP_S="${IDLE_STOP_S:-600}"
# How long the scorer may take to publish its first reading, and how often that
# is looked for.
FIRST_POLL_WAIT_S="${FIRST_POLL_WAIT_S:-300}"
FIRST_POLL_S="${FIRST_POLL_S:-5}"
# What one producer shard's pod asks for. A shard reads one whole batch object
# into memory and decompresses it whole, so its peak follows the preset's batch
# bytes — `offered_bytes_per_s x batch_interval_ms / 1000` — and not the shard
# count. The default fits the smoke preset; see "Generating a corpus" in
# docs/running.md for what the larger ones need.
PRODUCER_MEMORY="${PRODUCER_MEMORY:-2Gi}"

usage() {
	cat <<'USAGE'
usage: scripts/launch.sh <run_id> [options]

  <run_id>           a run stage.sh has staged, whose directory is under $RUNS_DIR
  --site PATH        the site config naming the cluster and the runs prefix (default: ./site.yaml)
  --image-tag TAG    the harness image tag the producer and the scorer run (default: this checkout's commit)

Environment: EPOCH_LEAD_S, IDLE_STOP_S, FIRST_POLL_WAIT_S, FIRST_POLL_S,
PRODUCER_MEMORY, RUNS_DIR.
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
		[[ -z $RUN_ID ]] || die "this launches one run, and was given both '$RUN_ID' and '$1'"
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

require_host_tools kubectl yq jq git

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
SPEC="$RUN_DIR/spec.yaml"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; run scripts/stage.sh first, or set RUNS_DIR"
[[ -f $SPEC ]] || die "$RUN_DIR holds no spec.yaml, so the run it asks for cannot be read"

k8s_read_site
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

BOOTSTRAP="$(jq -r .bootstrap "$FACTS")"
CORPUS_URI="$(jq -r .corpus_uri "$FACTS")"
TABLE="$(jq -r .table "$FACTS")"
# The topic staging created, rather than the run id it was named after: the two
# are the same string today, and a producer that rebuilt the name would publish
# to a topic of its own the day they stop being.
TOPIC="$(jq -r .topic "$FACTS")"
[[ -n $TOPIC && $TOPIC != null ]] || die "$FACTS names no topic, so there is nothing for the producer to publish to"
# `key_column` is null when the spec asked for unkeyed records, and the flag is
# then left off rather than passed empty.
KEY_COLUMN="$(jq -r '.key_column // empty' "$FACTS")"
# What the producer frames each value as, and the id its header names, both
# settled at stage time. `schema_id` is null for a raw-Avro run and the flag is
# then left off rather than passed empty.
VALUE_ENCODING="$(jq -r '.value_encoding // empty' "$FACTS")"
SCHEMA_ID="$(jq -r '.schema_id // empty' "$FACTS")"

# Every knob the spec sets about the offer, so the run that happens is the run
# the copied spec claims. A key the spec leaves out is left out here too, and
# the producer and the scorer apply their own defaults rather than ones this
# script would have to keep in step with theirs.
SHARDS="$(yq '.producer.shards' "$SPEC")"
if [[ $SHARDS == null ]]; then
	SHARDS=1
fi
[[ $SHARDS =~ ^[1-9][0-9]*$ ]] || die "$SPEC asks for producer.shards '$SHARDS', which is not a pod count"
SPEED="$(yq '.producer.speed' "$SPEC")"
REPLAY_SECONDS="$(yq '.producer.seconds' "$SPEC")"
BEHIND_MAX_MS="$(yq '.producer.behind_max_ms' "$SPEC")"
COMPRESSION="$(yq '.producer.compression' "$SPEC")"

EPOCH=$(($(date +%s) + EPOCH_LEAD_S))

# ---------------------------------------------------------------------------
# The scorer
# ---------------------------------------------------------------------------

SCORER_JOB="$(scorer_job "$RUN_ID")"
SCORE="score --corpus $CORPUS_URI --table $TABLE"
SCORE="$SCORE --publish-logs $RUNS_ROOT/$RUN_ID/producer --epoch $EPOCH"
# Written to the pod's own disk and mirrored to the runs prefix on every poll,
# because gate.sh and finish.sh read the artifacts from there and this pod's
# filesystem goes with the pod.
SCORE="$SCORE --out /work/scores --upload-prefix $RUNS_ROOT/$RUN_ID/scores"
SCORE="$SCORE --idle-stop-s $IDLE_STOP_S --publish-shards $SHARDS"
# Who created the table, because it decides what an absent one means: an engine
# that creates its own has none until its first record, and this scorer starts
# before the producer does. Left off where the spec says nothing, so the scorer
# applies its own default rather than one this script would keep in step.
MANAGED_BY="$(yq '.table.managed_by' "$SPEC")"
[[ $MANAGED_BY == null ]] || SCORE="$SCORE --table-managed-by $MANAGED_BY"
SCORE="$SCORE$(site_flags '.catalog.props' --catalog-prop)"
# A scoring key the spec leaves out is left out here too.
for key in warmup_s freshness_bound_s; do
	value="$(yq ".scoring.$key" "$SPEC")"
	[[ $value == null ]] || SCORE="$SCORE --${key//_/-} $value"
done
[[ $SPEED == null ]] || SCORE="$SCORE --speed $SPEED"
# The scorer decides whether the producer, rather than the engine, set the rate,
# so the spec's tolerance has to reach it and not only the producer.
[[ $BEHIND_MAX_MS == null ]] || SCORE="$SCORE --behind-max-ms $BEHIND_MAX_MS"

log "starting the scorer as job/$SCORER_JOB (epoch $EPOCH, idle stop ${IDLE_STOP_S}s)"
k8s_delete job "$SCORER_JOB"
k8s_render_apply deploy/k8s/scorer-job.yaml.tmpl \
	"NAME=$SCORER_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$SCORE" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"

log "waiting up to ${FIRST_POLL_WAIT_S}s for job/$SCORER_JOB to take its first reading"
waited=0
while :; do
	# Read into a variable rather than piped into `grep`: a `grep -q` that
	# matched closes the pipe, and the `kubectl` behind it then dies of SIGPIPE
	# — which under `pipefail` reads as a failure to find the line.
	logs="$(k8s_job_logs "$SCORER_JOB" 2>/dev/null || true)"
	if grep -q '^POLL ' <<<"$logs"; then
		log "the scorer is reading the table"
		break
	fi
	if ((waited >= FIRST_POLL_WAIT_S)); then
		k8s_job_tail "$SCORER_JOB"
		die "job/$SCORER_JOB published no reading within ${FIRST_POLL_WAIT_S}s; the lines above are its own log"
	fi
	sleep "$FIRST_POLL_S"
	waited=$((waited + FIRST_POLL_S))
done

# ---------------------------------------------------------------------------
# The offer
# ---------------------------------------------------------------------------

# The scorer's first-reading wait above can eat most or all of EPOCH_LEAD_S; a
# producer applied with the epoch no longer safely ahead has its first batch
# due before its first connection opens, and the run voids as producer_bound
# rather than measuring the engine.
(($(date +%s) + 30 <= EPOCH)) || die "epoch $EPOCH is under 30s away; raise EPOCH_LEAD_S (currently $EPOCH_LEAD_S) and relaunch"

PRODUCER_JOB="$(producer_job "$RUN_ID")"
PRODUCE="produce --corpus $CORPUS_URI --bootstrap $BOOTSTRAP --topic $TOPIC --epoch $EPOCH"
# `$JOB_COMPLETION_INDEX` is escaped here and expanded by the shell that is the
# image's entrypoint, so one rendered command serves every shard.
PRODUCE="$PRODUCE --shard \$JOB_COMPLETION_INDEX --shards $SHARDS"
PRODUCE="$PRODUCE --publish-log /work/publish_log-\$JOB_COMPLETION_INDEX.jsonl"
PRODUCE="$PRODUCE --upload-prefix $RUNS_ROOT/$RUN_ID"
PRODUCE="$PRODUCE$(site_flags '.kafka.security' --kafka-prop)"
[[ -z $KEY_COLUMN ]] || PRODUCE="$PRODUCE --key-column $KEY_COLUMN"
[[ -z $VALUE_ENCODING ]] || PRODUCE="$PRODUCE --value-encoding $VALUE_ENCODING"
[[ -z $SCHEMA_ID ]] || PRODUCE="$PRODUCE --schema-id $SCHEMA_ID"
[[ $SPEED == null ]] || PRODUCE="$PRODUCE --speed $SPEED"
[[ $REPLAY_SECONDS == null ]] || PRODUCE="$PRODUCE --seconds $REPLAY_SECONDS"
[[ $BEHIND_MAX_MS == null ]] || PRODUCE="$PRODUCE --behind-max-ms $BEHIND_MAX_MS"
[[ $COMPRESSION == null ]] || PRODUCE="$PRODUCE --compression $COMPRESSION"

log "offering the corpus from $SHARDS shard(s) as job/$PRODUCER_JOB"
k8s_delete job "$PRODUCER_JOB"
k8s_render_apply deploy/k8s/producer-job.yaml.tmpl \
	"NAME=$PRODUCER_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$PRODUCE" \
	"COUNT=$SHARDS" \
	"MEMORY=$PRODUCER_MEMORY" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"

# The epoch is the one fact staging could not know, and every later reader of
# the run directory needs it. `jq` cannot edit in place, so the document is
# rewritten through a temporary file beside it.
EPOCH_TMP="$(mktemp "$RUN_DIR/facts.json.XXXXXX")"
jq --argjson epoch "$EPOCH" '.epoch = $epoch' "$FACTS" >"$EPOCH_TMP"
mv "$EPOCH_TMP" "$FACTS"
printf '%s launched epoch=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$EPOCH" >>"$RUN_DIR/timeline.log"

log "launched $RUN_ID at epoch $EPOCH; judge it with scripts/gate.sh $RUN_ID"
