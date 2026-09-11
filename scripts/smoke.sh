#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run the full benchmark locally: generate, stage, start the engine, produce and score.
# Exit 0 only when run_valid is true.
# This checks integration, not engine performance: the stack shares one machine.
set -euo pipefail
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"

# Set a shared future epoch so producer and scorer can start before the first batch.
EPOCH_LEAD_S="${EPOCH_LEAD_S:-30}"
# Allow a cold first commit, then stop if outstanding rows make no progress.
IDLE_STOP_S="${IDLE_STOP_S:-120}"
# Timeout for --external-ready-file.
EXTERNAL_READY_WAIT_S="${EXTERNAL_READY_WAIT_S:-900}"
# Must match ingest_bench.scorer.cli.NO_GEOMETRY. A table without commits has no geometry;
# other nonzero statuses are read failures.
NO_GEOMETRY=4

usage() {
	cat <<'USAGE'
usage: scripts/smoke.sh [options]

  --engine NAME|external      which run spec to stage. NAME is a directory under
                              engines/ whose compose.sh starts the engine here;
                              `external` waits for you to start yours
                              (default: flink)
  --spec PATH                 a run spec under runs/ to stage instead of
                              runs/smoke-<engine>.yaml
  --set KEY=VALUE             override a corpus preset key, repeatable
                              (e.g. --set duration_s=30 for a 30 s corpus)
  --keep                      leave the stack up afterwards
  --external-ready-file PATH  with --engine external, wait for PATH to appear
                              instead of reading a newline from stdin

Environment: EPOCH_LEAD_S, IDLE_STOP_S, EXTERNAL_READY_WAIT_S, plus readiness
timeouts and endpoints defined in engines/<engine>/compose.sh.
USAGE
}

# Load engine operations from engines/<name>/compose.sh.
ENGINE=flink
KEEP=0
READY_FILE=""
SPEC_FILE=""
# These options enter the image's shell as part of one command string.
SETS=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--engine)
		ENGINE="${2:?--engine needs an engines/ directory name, or external}"
		shift 2
		;;
	--spec)
		SPEC_FILE="${2:?--spec needs a path}"
		shift 2
		;;
	--set)
		SETS="$SETS --set ${2:?--set needs KEY=VALUE}"
		shift 2
		;;
	--keep)
		KEEP=1
		shift
		;;
	--external-ready-file)
		READY_FILE="${2:?--external-ready-file needs a path}"
		shift 2
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

require_host_tools docker jq yq curl

[[ -n $SPEC_FILE ]] || SPEC_FILE="$REPO_ROOT/runs/smoke-$ENGINE.yaml"
[[ -f $SPEC_FILE ]] ||
	die "no run spec at $SPEC_FILE; --engine takes external or a directory under $REPO_ROOT/engines, or name a spec with --spec"
# Specs must be under runs/, which the harness container mounts at /runs.
[[ "$(cd -- "$(dirname -- "$SPEC_FILE")" && pwd)" == "$REPO_ROOT/runs" ]] ||
	die "--spec must name a file under $REPO_ROOT/runs, which is what the harness container mounts as /runs"
[[ $ENGINE == external || -z $READY_FILE ]] || die "--external-ready-file only applies to --engine external"

# Managed engines supply their own Compose hooks; external engines are started by the
# operator.
if [[ $ENGINE != external ]]; then
	ENGINE_COMPOSE="$REPO_ROOT/engines/$ENGINE/compose.sh"
	[[ -f $ENGINE_COMPOSE ]] ||
		die "no $ENGINE_COMPOSE, so this stack does not know how to start '$ENGINE'; --engine takes external or a directory under $REPO_ROOT/engines"
	# shellcheck source=/dev/null
	source "$ENGINE_COMPOSE"
	for hook in engine_compose_build engine_compose_start engine_compose_ready engine_compose_logs; do
		declare -F "$hook" >/dev/null ||
			die "$ENGINE_COMPOSE declares no $hook, and this script calls all four; see docs/adding-an-engine.md"
	done
fi

STAGE_OUT=""
RUN_ID=""
cleanup() {
	local status=$?
	if ((status != 0)); then
		# Print engine and scorer logs before teardown removes their containers.
		if [[ -n $RUN_ID ]] && docker container inspect "scorer-$RUN_ID" >/dev/null 2>&1; then
			log "--- last 30 lines of scorer-$RUN_ID ---"
			docker logs --tail 30 "scorer-$RUN_ID" >&2 || true
		fi
		# Delegate log selection to the engine's Compose hooks.
		if declare -F engine_compose_logs >/dev/null; then
			engine_compose_logs
		fi
	fi
	[[ -z $STAGE_OUT ]] || rm -f "$STAGE_OUT"
	if ((KEEP == 1)); then
		log "--keep: the stack is still up. Tear it down with:"
		log "  RUN_DIR=/tmp docker compose -f $COMPOSE_FILE --profile '*' down -v"
	else
		log "tearing the stack down"
		compose down -v --remove-orphans >/dev/null 2>&1 || true
	fi
	exit "$status"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# The site
# ---------------------------------------------------------------------------

log "building the harness image"
compose build harness
if [[ $ENGINE != external ]]; then
	engine_compose_build
fi

log "starting the broker, the object store and the catalog"
compose up -d kafka minio minio-init iceberg-rest

log "generating the corpus"
harness "gen-corpus --preset smoke$SETS --out s3://corpus --seed 1"

# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

# Use an explicit XXXXXX template for both BSD and GNU mktemp.
STAGE_OUT="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-stage.XXXXXX")"
log "staging $(basename "$SPEC_FILE")"
harness "stage --spec /runs/$(basename "$SPEC_FILE") --site /site.yaml --runs-dir /runs" | tee "$STAGE_OUT"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' "$STAGE_OUT")"
[[ -n $RUN_ID ]] || die "stage printed no run_id line; see the output above"
# Compose reads the submitter's mount path from the environment.
export RUN_DIR="$REPO_ROOT/runs/$RUN_ID"
[[ -d $RUN_DIR ]] || die "stage reported run_id $RUN_ID but wrote no $RUN_DIR"
log "run $RUN_ID staged in $RUN_DIR"

if [[ $ENGINE != external ]]; then
	# Start the engine, then verify its effective settings against the staged spec.
	engine_compose_start
	engine_compose_ready
else
	log "start your engine now against these facts:"
	cat "$RUN_DIR/facts.json"
	if [[ -n $READY_FILE ]]; then
		log "waiting up to ${EXTERNAL_READY_WAIT_S}s for $READY_FILE to appear"
		waited=0
		while [[ ! -e $READY_FILE ]]; do
			((waited < EXTERNAL_READY_WAIT_S)) || die "$READY_FILE did not appear within ${EXTERNAL_READY_WAIT_S}s"
			sleep 2
			waited=$((waited + 2))
		done
	else
		log "press enter once it is consuming $RUN_ID"
		read -r
	fi
fi

# ---------------------------------------------------------------------------
# The offer, and the score of it
# ---------------------------------------------------------------------------

BOOTSTRAP="$(jq -r .bootstrap "$RUN_DIR/facts.json")"
CORPUS_URI="$(jq -r .corpus_uri "$RUN_DIR/facts.json")"
TABLE="$(jq -r .table "$RUN_DIR/facts.json")"
# Omit --key-column for unkeyed records.
KEY_COLUMN="$(jq -r '.key_column // empty' "$RUN_DIR/facts.json")"
# Use the staged encoding and schema ID. Raw Avro has no schema ID.
VALUE_ENCODING="$(jq -r '.value_encoding // empty' "$RUN_DIR/facts.json")"
SCHEMA_ID="$(jq -r '.schema_id // empty' "$RUN_DIR/facts.json")"

SHARDS="$(yq '.producer.shards' "$SPEC_FILE")"
[[ $SHARDS == null || $SHARDS == 1 ]] ||
	die "this script supports one producer shard; $(basename "$SPEC_FILE") requests $SHARDS"

# Pass configured options and preserve command defaults for omitted ones.
SPEED="$(yq '.producer.speed' "$SPEC_FILE")"
REPLAY_SECONDS="$(yq '.producer.seconds' "$SPEC_FILE")"
BEHIND_MAX_MS="$(yq '.producer.behind_max_ms' "$SPEC_FILE")"
COMPRESSION="$(yq '.producer.compression' "$SPEC_FILE")"

EPOCH=$(($(date +%s) + EPOCH_LEAD_S))
write_launch_epoch "$RUN_DIR/facts.json" "$EPOCH"
SCORE="score --corpus $CORPUS_URI --table $TABLE --catalog-prop-file /catalog.props"
SCORE="$SCORE --publish-logs s3://runs/$RUN_ID/producer --epoch $EPOCH --out /runs/$RUN_ID/scores"
SCORE="$SCORE --idle-stop-s $IDLE_STOP_S"
# An engine-managed table may not exist until the first record.
MANAGED_BY="$(yq '.table.managed_by' "$SPEC_FILE")"
[[ $MANAGED_BY == null ]] || SCORE="$SCORE --table-managed-by $MANAGED_BY"
# Preserve scorer defaults for omitted options.
for key in warmup_s freshness_bound_s; do
	value="$(yq ".scoring.$key" "$SPEC_FILE")"
	[[ $value == null ]] || SCORE="$SCORE --${key//_/-} $value"
done
# Pass lateness tolerance to the scorer so it can determine producer_bound.
[[ $BEHIND_MAX_MS == null ]] || SCORE="$SCORE --behind-max-ms $BEHIND_MAX_MS"

log "starting the scorer (epoch $EPOCH, idle stop ${IDLE_STOP_S}s)"
compose run -d --name "scorer-$RUN_ID" harness "$SCORE" >/dev/null

PRODUCE="produce --corpus $CORPUS_URI --bootstrap $BOOTSTRAP --topic $RUN_ID --epoch $EPOCH"
PRODUCE="$PRODUCE --publish-log /runs/$RUN_ID/publish_log-0.jsonl --upload-prefix s3://runs/$RUN_ID"
[[ -z $KEY_COLUMN ]] || PRODUCE="$PRODUCE --key-column $KEY_COLUMN"
[[ -z $VALUE_ENCODING ]] || PRODUCE="$PRODUCE --value-encoding $VALUE_ENCODING"
[[ -z $SCHEMA_ID ]] || PRODUCE="$PRODUCE --schema-id $SCHEMA_ID"
[[ $SPEED == null ]] || PRODUCE="$PRODUCE --speed $SPEED"
[[ $REPLAY_SECONDS == null ]] || PRODUCE="$PRODUCE --seconds $REPLAY_SECONDS"
[[ $BEHIND_MAX_MS == null ]] || PRODUCE="$PRODUCE --behind-max-ms $BEHIND_MAX_MS"
[[ $COMPRESSION == null ]] || PRODUCE="$PRODUCE --compression $COMPRESSION"
log "offering the corpus"
harness "$PRODUCE"

log "waiting for the scorer to see the table drain"
SCORER_STATUS="$(docker wait "scorer-$RUN_ID")"
docker logs "scorer-$RUN_ID" 2>&1 | tail -n 20 >&2
docker rm "scorer-$RUN_ID" >/dev/null
log "the scorer exited $SCORER_STATUS (0 means drained, 2 means it stopped idle)"

# Measure before cleanup removes the catalog. Preserve file-sizes' default offsets
# when the spec does not supply them.
OFFSETS="$(yq '[.scoring.geometry_offsets_s // [] | .[] | tostring] | join(",")' "$SPEC_FILE")" ||
	die "could not read scoring.geometry_offsets_s out of $SPEC_FILE"
OFFSET_FLAGS=""
[[ -z $OFFSETS ]] || OFFSET_FLAGS="--offsets $OFFSETS"

log "measuring file geometry for $RUN_ID"
GEOMETRY_STATUS=0
harness "file-sizes --table $TABLE --catalog-prop-file /catalog.props --epoch $EPOCH --out /runs/$RUN_ID/scores $OFFSET_FLAGS" ||
	GEOMETRY_STATUS=$?
if ((GEOMETRY_STATUS == NO_GEOMETRY)); then
	log "no geometry: the table never committed"
elif ((GEOMETRY_STATUS != 0)); then
	die "could not measure the geometry: file-sizes exited $GEOMETRY_STATUS; see the error above"
fi

print_verdict "$RUN_DIR/scores/summary.json" "$RUN_DIR/scores/geometry.json"
log "run_valid: true — $RUN_ID, artifacts in $RUN_DIR"
