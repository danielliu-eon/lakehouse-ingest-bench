# SPDX-License-Identifier: Apache-2.0
# How a Flink run is started, made ready and read on the local Compose stack.
#
# The other half of the seam `specs/kubernetes.py` is: that file says how a run
# of this engine is addressed on a cluster, and this one says how it is
# addressed on one machine — the services to build and raise, the env file the
# renderer wrote the fleet's shape into, and a readiness probe nobody outside
# the engine can write. Together they are what keeps `scripts/` free of an
# engine's names.
#
# Sourced by scripts/smoke.sh, never executed, and only for a run whose spec
# names this engine. The four `engine_compose_*` hooks are the whole contract
# and are called in the order they are declared. `compose`, `harness`, `log`
# and `die` come from scripts/_lib.sh; `RUN_ID`, `RUN_DIR`, `SPEC_FILE` and
# `REPO_ROOT` are set before the first call.

# The jobmanager's REST endpoint, as the compose file publishes it.
FLINK_REST="${FLINK_REST:-http://localhost:8081}"

# How long the fleet may take to register its slots, and a job to reach RUNNING
# once submitted. Both are generous because the engine image is amd64: on an
# arm64 machine every second of this is emulated.
FLINK_SLOT_WAIT_S="${FLINK_SLOT_WAIT_S:-180}"
FLINK_JOB_WAIT_S="${FLINK_JOB_WAIT_S:-180}"

# A non-numeric or absent REST answer reads as zero rather than as an error: the
# endpoint is polled precisely because it is not up yet, and `curl` failing is
# the normal first answer.
_rest_number() {
	local value
	value="$(curl -sf --max-time 5 "$1" 2>/dev/null | jq -r "$2" 2>/dev/null || true)"
	case "$value" in '' | *[!0-9]*) value=0 ;; esac
	printf '%s' "$value"
}

# Slots, not taskmanager containers: a job asks the scheduler for slots, and a
# fleet whose containers are up but whose slots have not registered fails
# submission with a resource timeout minutes later instead of at once.
wait_for_flink_slots() {
	local wanted=$1 waited=0 total=0
	while ((waited < FLINK_SLOT_WAIT_S)); do
		total="$(_rest_number "$FLINK_REST/overview" '."slots-total" // 0')"
		if ((total >= wanted)); then
			log "flink fleet has $total slot(s)"
			return 0
		fi
		sleep 2
		waited=$((waited + 2))
	done
	die "flink reported $total of $wanted slot(s) after ${FLINK_SLOT_WAIT_S}s; check: compose logs flink-taskmanager"
}

# The job is named after the run — `pipeline.name` is the run id — so this is
# also the check that the job on the cluster is the one just submitted.
wait_for_flink_job_running() {
	local name=$1 waited=0 state=""
	while ((waited < FLINK_JOB_WAIT_S)); do
		state="$(curl -sf --max-time 5 "$FLINK_REST/jobs/overview" 2>/dev/null |
			jq -r --arg name "$name" '.jobs[]? | select(.name == $name) | .state' 2>/dev/null || true)"
		case "$state" in
		RUNNING)
			log "flink job $name is RUNNING"
			return 0
			;;
		FAILED | CANCELED | FINISHED)
			die "flink job $name went to $state before it ran; check: compose logs flink-jobmanager"
			;;
		esac
		sleep 2
		waited=$((waited + 2))
	done
	die "flink job $name did not reach RUNNING within ${FLINK_JOB_WAIT_S}s (last state: ${state:-none reported})"
}

engine_compose_build() {
	log "building the engine image (amd64; emulated on an arm64 machine)"
	compose build flink-jobmanager
}

engine_compose_start() {
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
}

engine_compose_ready() {
	wait_for_flink_job_running "$RUN_ID"
	# A job that is RUNNING is not yet a job running what the spec asked for:
	# Flink drops a setting it does not know and sizes a vertex from whatever
	# configuration reached it, neither of which fails a submission. The
	# jobmanager is addressed by its service name because this runs inside the
	# stack's own network, where `localhost` is the harness container.
	log "checking the job against the spec it was staged from"
	harness "verify-flink --spec /runs/$RUN_ID/spec.yaml --run-id $RUN_ID --rest http://flink-jobmanager:8081" ||
		die "the flink job is not running what $(basename "$SPEC_FILE") asked for; the lines above name every setting it dropped"
}

engine_compose_logs() {
	log "--- last 40 lines of flink-jobmanager ---"
	compose logs --tail 40 --no-log-prefix flink-jobmanager >&2 || true
}
