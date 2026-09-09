# Shared shell for the run scripts: where the stack is, how to speak to it, and
# how to wait for the two things in it that announce readiness to nobody.
#
# Sourced, never executed. Shell options belong to the caller — nothing here
# sets or clears one, so a script that runs without `set -e` still does.

# Resolved from this file rather than from the caller's working directory. Every
# path the stack mounts is relative to the compose file, so a script invoked
# from elsewhere would otherwise mount a tree that is not this checkout.
_LIB_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$_LIB_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/deploy/compose/local/docker-compose.yml"

# Where a caller's prerequisites are written down, for the one message that has
# to point at them. Set before sourcing this file: the cloud setup scripts under
# deploy/ share these functions and have their own list of tools.
PREREQ_DOC="${PREREQ_DOC:-docs/running.md}"

# The jobmanager's REST endpoint, as the compose file publishes it.
FLINK_REST="${FLINK_REST:-http://localhost:8081}"

# How long the fleet may take to register its slots, and a job to reach RUNNING
# once submitted. Both are generous because the engine image is amd64: on an
# arm64 machine every second of this is emulated.
FLINK_SLOT_WAIT_S="${FLINK_SLOT_WAIT_S:-180}"
FLINK_JOB_WAIT_S="${FLINK_JOB_WAIT_S:-180}"

# stderr, so a caller can still parse a command's stdout through a pipe while
# the narration stays on screen.
log() {
	printf '%s  %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

die() {
	log "$*"
	exit 1
}

# Every profile is activated on every call. Compose interpolates the whole model
# before it filters by profile, so naming them all costs nothing and removes the
# class of failure where a service is invisible to the one command that needs
# it. What actually starts is always named explicitly.
compose() {
	docker compose -f "$COMPOSE_FILE" --profile flink --profile flink-job --profile tools "$@"
}

# One harness command, as the single string the image's shell entrypoint splits.
# `-T` because some of these are parsed, and a TTY carriage-returns every line.
harness() {
	compose run --rm -T harness "$1"
}

# The verdict block, and a refusal unless the run is publishable. Shared by the
# local smoke and the cloud drivers so that both read the same fields in the
# same order — a second copy of this filter would drift, and the copy that lost
# would be the one nobody reread.
print_verdict() {
	local summary=$1
	[[ -f $summary ]] || die "the scorer published no $summary"
	jq '{
  run_valid, state, reason, producer_bound,
  prefix, last_batch, committed_rows, offered_rows,
  freshness: .freshness.window,
  exactness: {exact: .exactness.exact, loss_rows: .exactness.loss_rows, duplicate_rows: .exactness.duplicate_rows},
  keepup
}' "$summary"
	[[ "$(jq -r .run_valid "$summary")" == true ]] ||
		die "run_valid is false; the block above says why, in full in $summary"
}

# Refuse up front rather than half way through a run. A missing `yq` surfaces
# otherwise as a scorer given an empty `--warmup-s`, minutes after the corpus
# was generated.
require_host_tools() {
	local missing="" tool
	for tool in "$@"; do
		command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
	done
	[[ -z $missing ]] || die "missing host tool(s):$missing — see $PREREQ_DOC for what this needs"
}

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
