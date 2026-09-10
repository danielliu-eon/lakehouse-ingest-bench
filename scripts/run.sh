#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Run stage, launch, gate, teardown and finish in order. Print the run ID immediately
# after staging so an interrupted session can resume with the individual drivers in
# docs/running.md.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Limit the polling wait after launch. The default allows an hour-long offer plus drain
# time.
RUN_MAX_S="${RUN_MAX_S:-7200}"
# Allow time to start an external engine before its ready file appears.
EXTERNAL_READY_WAIT_S="${EXTERNAL_READY_WAIT_S:-1800}"

# Use a distinct exit code when teardown fails and the fleet may still be running.
TEARDOWN_FAILED=6

usage() {
	cat <<'USAGE'
usage: scripts/run.sh <spec> [options]

  <spec>                      a run spec on this machine, staged as it stands
  --site PATH                 the site config every driver below reads (default: ./site.yaml)
  --image-tag TAG             the harness and engine image tag to run (default: this checkout's commit)
  --publish DIR               passed to finish.sh: also write the result under DIR/<engine>/
  --variant NAME              passed to finish.sh: the tuning this run stands for
  --external-ready-file PATH  for `engine: external`, wait for PATH to appear once the run is staged
                              instead of reading a newline from stdin
  --gate-interval-s N         how often the run is judged while it goes (default: 60)
  --breaches N                passed to gate.sh: consecutive non-PASS verdicts before it tears down

Environment: RUN_MAX_S, EXTERNAL_READY_WAIT_S, RUNS_DIR, and every wait the
drivers below take — each is in that driver's own --help.

It prints `run_id: <id>` as soon as staging returns.

Exit codes: finish.sh's own, 0 only on `run_valid: true`; 6 when the teardown
did not converge, so the fleet may still be running; 2 for an argument error;
1 for a refusal, an overrun among them.
USAGE
}

SPEC=""
IMAGE_TAG=""
PUBLISH_DIR=""
VARIANT=""
READY_FILE=""
BREACHES=""
# Poll about once a minute to avoid repeatedly reading the same scorer sample.
GATE_INTERVAL_S=60
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
	--publish)
		PUBLISH_DIR="${2:?--publish needs a directory}"
		shift 2
		;;
	--variant)
		VARIANT="${2:?--variant needs a name}"
		shift 2
		;;
	--external-ready-file)
		READY_FILE="${2:?--external-ready-file needs a path}"
		shift 2
		;;
	--gate-interval-s)
		GATE_INTERVAL_S="${2:?--gate-interval-s needs a number of seconds}"
		shift 2
		;;
	--breaches)
		BREACHES="${2:?--breaches needs a count}"
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
		[[ -z $SPEC ]] || die "this runs one spec, and was given both '$SPEC' and '$1'"
		SPEC="$1"
		shift
		;;
	esac
done

[[ -n $SPEC ]] || {
	printf 'a run spec is required\n\n' >&2
	usage >&2
	exit 2
}

# Validate polling arguments before starting a fleet.
[[ $GATE_INTERVAL_S =~ ^[1-9][0-9]*$ ]] ||
	die "--gate-interval-s takes a number of seconds, and was given '$GATE_INTERVAL_S'"
[[ -z $BREACHES || $BREACHES =~ ^[1-9][0-9]*$ ]] ||
	die "--breaches takes a count of consecutive verdicts, and was given '$BREACHES'"
# Pass one breach threshold to the gate and use it below. Tests check it matches gate.sh's
# default.
BREACHES="${BREACHES:-3}"

# Check all drivers' prerequisites before staging.
require_host_tools kubectl aws yq jq git curl gzip
[[ -f $SPEC ]] || die "no run spec at $SPEC"
require_site_file
RUNS_ROOT="$(site_root '.runs_root')"

# Validate external-only options before staging can start a managed engine.
ENGINE="$(yq '.engine' "$SPEC")"
[[ $ENGINE == external || -z $READY_FILE ]] ||
	die "--external-ready-file applies to an external run, and $SPEC asks for engine '$ENGINE'"

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/ingest-bench-run.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

# Pass the selected site explicitly to every driver.
COMMON=(--site "$SITE_FILE")
[[ -z $IMAGE_TAG ]] || COMMON+=(--image-tag "$IMAGE_TAG")

# ---------------------------------------------------------------------------
# 1. Stage
# ---------------------------------------------------------------------------

log "staging $SPEC"
# Capture stage's result while leaving its stderr diagnostics visible.
STAGE_OUT="$("$REPO_ROOT/scripts/stage.sh" "$SPEC" "${COMMON[@]}")"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' <<<"$STAGE_OUT")"
[[ -n $RUN_ID ]] || die "scripts/stage.sh printed no run_id line, so there is no run to launch"
# Print the run ID before later steps can fail, allowing manual recovery.
printf 'run_id: %s\n' "$RUN_ID"

RUN_DIR="$RUNS_DIR/$RUN_ID"
SUMMARY_URI="$RUNS_ROOT/$RUN_ID/scores/summary.json"

if [[ $ENGINE == external ]]; then

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
		read -r ||
			die "nothing answered, and $RUN_ID is staged but not launched; --external-ready-file is the unattended form"
	fi
fi

# ---------------------------------------------------------------------------
# 2. Launch
# ---------------------------------------------------------------------------

LAUNCH_STATUS=0
"$REPO_ROOT/scripts/launch.sh" "$RUN_ID" "${COMMON[@]}" || LAUNCH_STATUS=$?
if ((LAUNCH_STATUS != 0)); then
	# Leave pods available for inspecting launch failures; print the teardown command.
	log "$RUN_ID is staged and its fleet is billing; stop it with scripts/teardown.sh $RUN_ID once the lines above have been read"
	exit "$LAUNCH_STATUS"
fi

# ---------------------------------------------------------------------------
# 3. Judge it while it goes
# ---------------------------------------------------------------------------

GATE_ARGS=("$RUN_ID" "${COMMON[@]}" --teardown --breaches "$BREACHES")
# Read the gate's persisted count of consecutive non-PASS verdicts.
BREACH_FILE="$RUN_DIR/gate-breaches"

TORN_DOWN=0
OVERRAN=0
waited=0
# Report an unreadable breach file only once.
BREACH_FILE_REPORTED=""
while :; do
	# Enforce the timeout even when no summary can be read.
	if ((waited >= RUN_MAX_S)); then
		log "$RUN_ID was still running after ${RUN_MAX_S}s, so it is being torn down unfinished"
		OVERRAN=1
		break
	fi
	sleep "$GATE_INTERVAL_S"
	waited=$((waited + GATE_INTERVAL_S))

	# Let gate --teardown apply the consecutive-breach rule; do not stop on one verdict.
	GATE_STATUS=0
	"$REPO_ROOT/scripts/gate.sh" "${GATE_ARGS[@]}" || GATE_STATUS=$?
	log "the gate exited $GATE_STATUS (0 PASS, 3 UNDERSIZED, 5 VOID, 1 nothing published to judge yet)"

	# Saved metadata indicates teardown reached the table-copy step.
	if [[ -f $RUN_DIR/$METADATA_FINAL_FILE ]]; then
		log "the gate tore $RUN_ID down"
		TORN_DOWN=1
		break
	fi

	# Teardown may leave no metadata if the table is absent or copying fails. Exit this loop
	# at the breach threshold, then retry the idempotent teardown below.
	if ((GATE_STATUS != 0)) && [[ -f $BREACH_FILE ]]; then
		# Validate as text before arithmetic: Bash treats barewords as variable names, which can
		# abort under set -u before cleanup.
		BREACH_COUNT="$(cat "$BREACH_FILE")"
		if [[ ! $BREACH_COUNT =~ ^[0-9]+$ ]]; then
			[[ -n $BREACH_FILE_REPORTED ]] ||
				log "$BREACH_FILE holds '$BREACH_COUNT' rather than a count of verdicts, so whether the gate tore $RUN_ID down cannot be read from it; remove it"
			BREACH_FILE_REPORTED=yes
		elif ((BREACH_COUNT >= BREACHES)); then
			log "the gate has not passed $RUN_ID for $BREACHES ticks, so it has torn it down"
			break
		fi
	fi

	if ! aws s3 cp "$SUMMARY_URI" "$SCRATCH/summary.json" --only-show-errors; then
		log "the scorer has published nothing at $SUMMARY_URI yet"
		continue
	fi
	# Retry an unreadable summary; it may be an incomplete update.
	STATE="$(jq -r '.state // empty' "$SCRATCH/summary.json")" || STATE=""
	if [[ -z $STATE ]]; then
		log "$SUMMARY_URI names no state yet"
		continue
	fi
	if [[ $STATE != running ]]; then
		log "$RUN_ID is $STATE"
		break
	fi
done

# ---------------------------------------------------------------------------
# 4. Tear down, and read it
# ---------------------------------------------------------------------------

# Initialize even when the gate already completed teardown.
TEARDOWN_STATUS=0
if ((TORN_DOWN == 0)); then
	# Read and publish the verdict even if teardown failed.
	"$REPO_ROOT/scripts/teardown.sh" "$RUN_ID" "${COMMON[@]}" || TEARDOWN_STATUS=$?
	((TEARDOWN_STATUS == 0)) ||
		log "tearing $RUN_ID down exited $TEARDOWN_STATUS, so its fleet may still be running: scripts/teardown.sh $RUN_ID"
fi

FINISH_ARGS=("$RUN_ID" --site "$SITE_FILE")
[[ -z $VARIANT ]] || FINISH_ARGS+=(--variant "$VARIANT")
[[ -z $PUBLISH_DIR ]] || FINISH_ARGS+=(--publish "$PUBLISH_DIR")
FINISH_STATUS=0
"$REPO_ROOT/scripts/finish.sh" "${FINISH_ARGS[@]}" || FINISH_STATUS=$?

# Report failures after collecting the verdict. Prioritize failed teardown over an overrun
# because the fleet may still incur charges.
if ((TEARDOWN_STATUS != 0)); then
	log "exiting $TEARDOWN_FAILED: the teardown above did not converge, so check the cluster"
	exit "$TEARDOWN_FAILED"
fi
((OVERRAN == 0)) ||
	die "$RUN_ID was still running after ${RUN_MAX_S}s, so it was torn down unfinished; raise RUN_MAX_S for a longer offer"
exit "$FINISH_STATUS"
