#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# One measured run end to end: stage it, launch it, judge it while it goes,
# tear it down when it ends, and read the verdict.
#
# Every step is the driver an operator would run by hand, in the order
# docs/running.md gives them and with nothing of its own in between. So a run
# this loses — a closed shell, a laptop that slept — is resumed with those same
# drivers, which is why the run id is printed the moment staging returns.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# How long the whole run may take before this stops waiting on it. Two hours:
# the shipped runs offer for an hour, and the staging, placement and drain
# around one add tens of minutes.
RUN_MAX_S="${RUN_MAX_S:-7200}"
# How long `--external-ready-file` is waited on. Half an hour, three times the
# smoke's, because standing a fleet up by hand on a cluster is slower than
# starting a container on this machine.
EXTERNAL_READY_WAIT_S="${EXTERNAL_READY_WAIT_S:-1800}"

# What this exits when a run was read but its teardown did not converge, so the
# fleet may still be running. Distinct from every code the drivers it runs
# define — gate.sh's 0, 3 and 5, a refusal's 1, an argument error's 2, the
# harness's 4 — so a caller can tell a fleet to go and check from a verdict.
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
did not converge, so the fleet may still be running; 2 for an argument error.
USAGE
}

SPEC=""
IMAGE_TAG=""
PUBLISH_DIR=""
VARIANT=""
READY_FILE=""
BREACHES=""
# How often the gate is asked, which is what docs/running.md tells an operator
# to do by hand. A minute, because the scorer publishes on its own poll cadence
# and a tighter loop reads the same summary twice.
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

# Both counts are checked here rather than where they are used: the interval is
# a `sleep` argument and the breach count is gate.sh's, so a typo in either
# would otherwise surface a minute after a fleet was already running.
[[ $GATE_INTERVAL_S =~ ^[1-9][0-9]*$ ]] ||
	die "--gate-interval-s takes a number of seconds, and was given '$GATE_INTERVAL_S'"
[[ -z $BREACHES || $BREACHES =~ ^[1-9][0-9]*$ ]] ||
	die "--breaches takes a count of consecutive verdicts, and was given '$BREACHES'"

# The union of what the five drivers need, so a missing one is refused before
# anything is created.
require_host_tools kubectl aws yq jq git curl gzip
[[ -f $SPEC ]] || die "no run spec at $SPEC"
require_site_file
RUNS_ROOT="$(site_root '.runs_root')"

# Whether `--external-ready-file` means anything, refused before staging
# because staging starts a managed engine and a refusal after it would leave a
# fleet running. The local spec rather than the copied one for that reason;
# staging copies this file verbatim, so they are the same document.
ENGINE="$(yq '.engine' "$SPEC")"
[[ $ENGINE == external || -z $READY_FILE ]] ||
	die "--external-ready-file applies to an external run, and $SPEC asks for engine '$ENGINE'"

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/ingest-bench-run.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

# The site reaches every driver explicitly rather than through the environment,
# so a run reads the file this one was given even where `SITE_FILE` is unset.
COMMON=(--site "$SITE_FILE")
[[ -z $IMAGE_TAG ]] || COMMON+=(--image-tag "$IMAGE_TAG")

# ---------------------------------------------------------------------------
# 1. Stage
# ---------------------------------------------------------------------------

log "staging $SPEC"
# stage.sh's stdout is the run id and its narration is stderr, so the capture
# takes the one and the operator still watches the other.
STAGE_OUT="$("$REPO_ROOT/scripts/stage.sh" "$SPEC" "${COMMON[@]}")"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' <<<"$STAGE_OUT")"
[[ -n $RUN_ID ]] || die "scripts/stage.sh printed no run_id line, so there is no run to launch"
# On stdout, and before anything else can fail: it is the only handle on the
# run, and an operator who loses this shell needs it to reach the drivers.
printf 'run_id: %s\n' "$RUN_ID"

RUN_DIR="$RUNS_DIR/$RUN_ID"
SUMMARY_URI="$RUNS_ROOT/$RUN_ID/scores/summary.json"

if [[ $ENGINE == external ]]; then
	# stage.sh has already printed the facts to start against.
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

"$REPO_ROOT/scripts/launch.sh" "$RUN_ID" "${COMMON[@]}"

# ---------------------------------------------------------------------------
# 3. Judge it while it goes
# ---------------------------------------------------------------------------

GATE_ARGS=("$RUN_ID" "${COMMON[@]}" --teardown)
[[ -z $BREACHES ]] || GATE_ARGS+=(--breaches "$BREACHES")

TORN_DOWN=0
OVERRAN=0
waited=0
while :; do
	# At the top of the loop, so a tick that could read no summary is still
	# bounded by it.
	if ((waited >= RUN_MAX_S)); then
		log "$RUN_ID was still running after ${RUN_MAX_S}s, so it is being torn down unfinished"
		OVERRAN=1
		break
	fi
	sleep "$GATE_INTERVAL_S"
	waited=$((waited + GATE_INTERVAL_S))

	# The verdict is logged and not acted on: `--teardown` is already what ends
	# a run that is not worth paying for, and it waits for the verdict to
	# repeat — a driver that exited on one tick would undo that.
	GATE_STATUS=0
	"$REPO_ROOT/scripts/gate.sh" "${GATE_ARGS[@]}" || GATE_STATUS=$?
	log "the gate exited $GATE_STATUS (0 PASS, 3 UNDERSIZED, 5 VOID)"

	# Only teardown.sh writes this document, so it is the one local sign that
	# the gate has torn the run down — and tearing it down twice would fail on
	# a topic that is already dropped.
	if [[ -f $RUN_DIR/$METADATA_FINAL_FILE ]]; then
		log "the gate tore $RUN_ID down"
		TORN_DOWN=1
		break
	fi

	if ! aws s3 cp "$SUMMARY_URI" "$SCRATCH/summary.json" --only-show-errors; then
		log "the scorer has published nothing at $SUMMARY_URI yet"
		continue
	fi
	# A summary read while the scorer was rewriting it parses as nothing, which
	# is a tick to wait out rather than a run to end.
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

# Declared outside the branch: a gate that already tore the run down converged,
# and the refusal at the end reads this either way.
TEARDOWN_STATUS=0
if ((TORN_DOWN == 0)); then
	# Not fatal here, so the verdict below is still read and published: a run
	# whose fleet outlived its teardown is as measured as one whose did not.
	"$REPO_ROOT/scripts/teardown.sh" "$RUN_ID" "${COMMON[@]}" || TEARDOWN_STATUS=$?
	((TEARDOWN_STATUS == 0)) ||
		log "tearing $RUN_ID down exited $TEARDOWN_STATUS, so its fleet may still be running: scripts/teardown.sh $RUN_ID"
fi

FINISH_ARGS=("$RUN_ID" --site "$SITE_FILE")
[[ -z $VARIANT ]] || FINISH_ARGS+=(--variant "$VARIANT")
[[ -z $PUBLISH_DIR ]] || FINISH_ARGS+=(--publish "$PUBLISH_DIR")
FINISH_STATUS=0
"$REPO_ROOT/scripts/finish.sh" "${FINISH_ARGS[@]}" || FINISH_STATUS=$?

# Both refusals are after the verdict block, because a run this stopped waiting
# on, or could not stop, has its artifacts as the only account of what it did.
#
# The teardown first: an overrun is a run to rerun at a longer `RUN_MAX_S`, and
# a fleet still running is money being spent right now.
if ((TEARDOWN_STATUS != 0)); then
	log "exiting $TEARDOWN_FAILED: the teardown above did not converge, so check the cluster"
	exit "$TEARDOWN_FAILED"
fi
((OVERRAN == 0)) ||
	die "$RUN_ID was still running after ${RUN_MAX_S}s, so it was torn down unfinished; raise RUN_MAX_S for a longer offer"
exit "$FINISH_STATUS"
