#!/usr/bin/env bash
# The whole benchmark on one machine: build a corpus, stage a run, start an
# engine on it, offer the corpus and score what reaches the table. Exits 0 only
# when the scorer published `run_valid: true`.
#
# Nothing measured here is a result — the stack shares one machine with the
# engine, and on arm64 the engine image is emulated. What it proves is that the
# harness, the engine and the scorer agree about a run: the same topic, the same
# table, the same epoch, and figures that come out the far end.
set -euo pipefail
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"

# The producer and the scorer are both handed the run's time origin, and it is
# a moment in the near future so that neither is still starting when the first
# batch is due.
EPOCH_LEAD_S="${EPOCH_LEAD_S:-30}"
# How long the scorer waits for a commit before it gives up on a table with
# rows still outstanding. Long enough to cover a cold first checkpoint on an
# emulated engine, short enough that a stuck run ends in minutes.
IDLE_STOP_S="${IDLE_STOP_S:-120}"
# How long `--external-ready-file` is waited on.
EXTERNAL_READY_WAIT_S="${EXTERNAL_READY_WAIT_S:-900}"

usage() {
	cat <<'USAGE'
usage: scripts/smoke.sh [options]

  --engine flink|spark|external
                              which run spec to stage; `flink` and `spark` also
                              start the engine, `external` waits for you to
                              start yours (default: flink)
  --spec PATH                 a run spec under runs/ to stage instead of
                              runs/smoke-<engine>.yaml
  --set KEY=VALUE             override a corpus preset key, repeatable
                              (e.g. --set duration_s=30 for a 30 s corpus)
  --keep                      leave the stack up afterwards
  --external-ready-file PATH  with --engine external, wait for PATH to appear
                              instead of reading a newline from stdin

Environment: EPOCH_LEAD_S, IDLE_STOP_S, EXTERNAL_READY_WAIT_S,
FLINK_REST, FLINK_SLOT_WAIT_S, FLINK_JOB_WAIT_S, SPARK_UI, SPARK_APP_WAIT_S,
SPARK_QUERY_WAIT_S.
USAGE
}

ENGINE=flink
KEEP=0
READY_FILE=""
SPEC_FILE=""
# A string rather than an array: these are passed inside the single command
# string the harness image's entrypoint splits, so they are already subject to
# one round of word splitting and an array would buy nothing.
SETS=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--engine)
		ENGINE="${2:?--engine needs flink, spark or external}"
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
[[ -f $SPEC_FILE ]] || die "no run spec at $SPEC_FILE; --engine takes flink, spark or external, or name one with --spec"
# The harness container mounts this checkout's `runs/` as `/runs` and the stage
# command names the spec inside it, so a spec anywhere else is not a file that
# container can open.
[[ "$(cd -- "$(dirname -- "$SPEC_FILE")" && pwd)" == "$REPO_ROOT/runs" ]] ||
	die "--spec must name a file under $REPO_ROOT/runs, which is what the harness container mounts as /runs"
[[ $ENGINE == external || -z $READY_FILE ]] || die "--external-ready-file only applies to --engine external"

STAGE_OUT=""
RUN_ID=""
cleanup() {
	local status=$?
	if ((status != 0)); then
		# The two logs that name why a run stopped: the engine's, and the
		# reader's. Printed before teardown, because teardown removes both.
		if [[ -n $RUN_ID ]] && docker container inspect "scorer-$RUN_ID" >/dev/null 2>&1; then
			log "--- last 30 lines of scorer-$RUN_ID ---"
			docker logs --tail 30 "scorer-$RUN_ID" >&2 || true
		fi
		case "$ENGINE" in
		flink)
			log "--- last 40 lines of flink-jobmanager ---"
			compose logs --tail 40 --no-log-prefix flink-jobmanager >&2 || true
			;;
		spark)
			log "--- last 40 lines of spark-job ---"
			compose logs --tail 40 --no-log-prefix spark-job >&2 || true
			;;
		esac
	fi
	[[ -z $STAGE_OUT ]] || rm -f "$STAGE_OUT"
	if ((KEEP == 1)); then
		log "--keep: the stack is still up. Tear it down with:"
		log "  docker compose -f $COMPOSE_FILE --profile flink --profile flink-job --profile spark --profile tools down -v"
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
if [[ $ENGINE == flink ]]; then
	log "building the engine image (amd64; emulated on an arm64 machine)"
	compose build flink-jobmanager
elif [[ $ENGINE == spark ]]; then
	log "building the engine image"
	compose build spark-job
fi

log "starting the broker, the object store and the catalog"
compose up -d kafka minio minio-init iceberg-rest

log "generating the corpus"
harness "gen-corpus --preset smoke$SETS --out s3://corpus --seed 1"

# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

# A template with X's and an explicit directory, because `mktemp -t <prefix>`
# is a BSD spelling: GNU coreutils refuses a template with no X's in it, so the
# BSD form works on a developer's Mac and fails in CI.
STAGE_OUT="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-stage.XXXXXX")"
log "staging $(basename "$SPEC_FILE")"
harness "stage --spec /runs/$(basename "$SPEC_FILE") --site /site.yaml --runs-dir /runs" | tee "$STAGE_OUT"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' "$STAGE_OUT")"
[[ -n $RUN_ID ]] || die "stage printed no run_id line; see the output above"
# Exported because the engine's submitter mounts it, and compose reads it from
# the environment rather than from an argument.
export RUN_DIR="$REPO_ROOT/runs/$RUN_ID"
[[ -d $RUN_DIR ]] || die "stage reported run_id $RUN_ID but wrote no $RUN_DIR"
log "run $RUN_ID staged in $RUN_DIR"

if [[ $ENGINE == flink ]]; then
	# The cluster's shape, as the engine's renderer wrote it. Exported so
	# compose interpolates the taskmanager's slots and both memory sizes.
	set -a
	# shellcheck source=/dev/null
	source "$RUN_DIR/flink.env"
	set +a
	log "starting flink: $TASKMANAGERS taskmanager(s) of $SLOTS slot(s)"
	compose up -d --scale "flink-taskmanager=$TASKMANAGERS" flink-jobmanager flink-taskmanager
	wait_for_flink_slots "$((TASKMANAGERS * SLOTS))"
	log "submitting the job"
	compose run --rm -T flink-job
	wait_for_flink_job_running "$RUN_ID"
	# A job that is RUNNING is not yet a job running what the spec asked for:
	# Flink drops a setting it does not know and sizes a vertex from whatever
	# configuration reached it, neither of which fails a submission. The
	# jobmanager is addressed by its service name because this runs inside the
	# stack's own network, where `localhost` is the harness container.
	log "checking the job against the spec it was staged from"
	harness "verify-flink --spec /runs/$RUN_ID/spec.yaml --run-id $RUN_ID --rest http://flink-jobmanager:8081" ||
		die "the flink job is not running what $(basename "$SPEC_FILE") asked for; the lines above name every setting it dropped"
elif [[ $ENGINE == spark ]]; then
	# The submission line's shape, as the engine's renderer wrote it. Exported
	# so compose interpolates the driver's cores and its heap. There is no
	# separate submitter: under `--master local[N]` this one container is the
	# driver, its executors and the job.
	set -a
	# shellcheck source=/dev/null
	source "$RUN_DIR/job.env"
	set +a
	log "starting spark: local[$LOCAL_CORES], ${DRIVER_MEM_MB}m driver"
	compose up -d spark-job
	wait_for_spark_query "$RUN_ID"
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
# `key_column` is null when the spec asked for unkeyed records, and the flag is
# then left off rather than passed empty.
KEY_COLUMN="$(jq -r '.key_column // empty' "$RUN_DIR/facts.json")"
# What the producer frames each value as, and the id its header names. The
# schema is registered at stage time, so this is the id every reader of the run
# resolves the writer schema by; it is null for a raw-Avro run, and the flag is
# then left off rather than passed empty.
VALUE_ENCODING="$(jq -r '.value_encoding // empty' "$RUN_DIR/facts.json")"
SCHEMA_ID="$(jq -r '.schema_id // empty' "$RUN_DIR/facts.json")"

SHARDS="$(yq '.producer.shards' "$SPEC_FILE")"
[[ $SHARDS == null || $SHARDS == 1 ]] ||
	die "this script offers one producer shard and $(basename "$SPEC_FILE") asks for $SHARDS"

# Every knob the spec sets about the offer, so the run that happens is the run
# the copied spec claims. A key the spec leaves out is left out here too, and
# the producer and the scorer apply their own defaults rather than ones this
# script would have to keep in step with theirs.
SPEED="$(yq '.producer.speed' "$SPEC_FILE")"
REPLAY_SECONDS="$(yq '.producer.seconds' "$SPEC_FILE")"
BEHIND_MAX_MS="$(yq '.producer.behind_max_ms' "$SPEC_FILE")"

EPOCH=$(($(date +%s) + EPOCH_LEAD_S))
SCORE="score --corpus $CORPUS_URI --table $TABLE --catalog-prop-file /catalog.props"
SCORE="$SCORE --publish-logs s3://runs/$RUN_ID/producer --epoch $EPOCH --out /runs/$RUN_ID/scores"
SCORE="$SCORE --idle-stop-s $IDLE_STOP_S"
# A scoring key the spec leaves out is left out here too, so the scorer applies
# its own default rather than one this script would have to keep in step.
for key in warmup_s freshness_bound_s; do
	value="$(yq ".scoring.$key" "$SPEC_FILE")"
	[[ $value == null ]] || SCORE="$SCORE --${key//_/-} $value"
done
# The scorer decides whether the producer, rather than the engine, set the rate,
# so the spec's tolerance has to reach it and not only the producer.
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
log "offering the corpus"
harness "$PRODUCE"

log "waiting for the scorer to see the table drain"
SCORER_STATUS="$(docker wait "scorer-$RUN_ID")"
docker logs "scorer-$RUN_ID" 2>&1 | tail -n 20 >&2
docker rm "scorer-$RUN_ID" >/dev/null
log "the scorer exited $SCORER_STATUS (0 drained, 2 stopped idle)"

print_verdict "$RUN_DIR/scores/summary.json"
log "run_valid: true — $RUN_ID, artifacts in $RUN_DIR"
