#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Judge a run that is still going, from the artifacts the scorer publishes.
#
# The scorer's two published files rather than the table: the scorer has
# already paid for that read, and two readers of one table would disagree about
# when a commit became visible — so the run's freshness figures would depend on
# which of them was asked.
#
# It exits with the gate's own code: 0 PASS, 3 UNDERSIZED, 5 VOID.
#
# `--teardown` destroys a fleet, so it waits for the verdict to repeat. The
# gate judges the lag as of the newest sample, and a fleet still working
# through a cold start, a checkpoint that took a moment or a poll that read a
# stale prefix each produce one breaching tick the next one contradicts.
set -euo pipefail
# The tools a missing prerequisite points at.
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: scripts/gate.sh <run_id> [options]

  <run_id>           a launched run, whose scorer is publishing under the runs prefix
  --site PATH        the site config naming the runs prefix (default: ./site.yaml)
  --image-tag TAG    passed to teardown.sh, which runs one harness Job (default: this checkout's commit)
  --teardown         tear the run down once the verdict has not been PASS this many ticks running
  --breaches N       how many consecutive non-PASS verdicts --teardown waits for (default 3; 1 acts at once)

Environment: RUNS_DIR.

Exit codes: 0 PASS, 3 UNDERSIZED, 5 VOID.
USAGE
}

RUN_ID=""
TEARDOWN=0
IMAGE_TAG=""
# How many consecutive non-PASS verdicts `--teardown` waits for. Three, because
# the gate is polled about once a minute: two ticks is inside the noise a cold
# start or one slow checkpoint produces, and three minutes of a fleet that is
# not passing is minutes rather than hours of a run nobody can publish.
BREACHES_REQUIRED=3
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
	--teardown)
		TEARDOWN=1
		shift
		;;
	--breaches)
		BREACHES_REQUIRED="${2:?--breaches needs a count}"
		[[ $BREACHES_REQUIRED =~ ^[1-9][0-9]*$ ]] ||
			die "--breaches takes a count of consecutive verdicts, and was given '$BREACHES_REQUIRED'"
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
		[[ -z $RUN_ID ]] || die "this judges one run, and was given both '$RUN_ID' and '$1'"
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

require_host_tools aws yq
require_site_file
RUNS_ROOT="$(site_required '.runs_root')"

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/ingest-bench-gate.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

# The two files `gate` reads, and no others: the rest of the scorer's output is
# the record of the run, and downloading it every minute would pay for the whole
# artifact set to answer one question.
for name in summary.json keepup_samples.jsonl; do
	aws s3 cp "$RUNS_ROOT/$RUN_ID/scores/$name" "$SCRATCH/$name" >&2 ||
		die "the scorer has published no $name under $RUNS_ROOT/$RUN_ID/scores/; it may not have read the table yet"
done

GATE=(gate --out "$SCRATCH")
# The gate's own windows are its defaults, and a run overrides them only where
# it said so — see spec.scoring in the copied spec.
SPEC="$RUNS_DIR/$RUN_ID/spec.yaml"
if [[ -f $SPEC ]]; then
	ADAPTATION_S="$(yq '.scoring.gate_adaptation_s' "$SPEC")"
	[[ $ADAPTATION_S == null ]] || GATE+=(--adaptation-s "$ADAPTATION_S")
	WINDOW_S="$(yq '.scoring.gate_window_s' "$SPEC")"
	[[ $WINDOW_S == null ]] || GATE+=(--window-s "$WINDOW_S")
fi

VERDICT_STATUS=0
harness_local "${GATE[@]}" || VERDICT_STATUS=$?

# How many ticks in a row have not been PASS, kept beside the run because each
# tick is its own process: an operator runs this every minute or so, and a
# count held in memory would be one tick long.
BREACH_FILE="$RUNS_DIR/$RUN_ID/gate-breaches"
BREACHES=0
if ((VERDICT_STATUS == 0)); then
	# Consecutive means consecutive: a passing tick starts the count again, so
	# two breaches hours apart cannot be joined by an unrelated third.
	rm -f "$BREACH_FILE"
else
	if [[ -f $BREACH_FILE ]]; then
		BREACHES="$(cat "$BREACH_FILE")"
		[[ $BREACHES =~ ^[0-9]+$ ]] ||
			die "$BREACH_FILE holds '$BREACHES' rather than a count of consecutive verdicts; remove it"
	fi
	BREACHES=$((BREACHES + 1))
	mkdir -p "$RUNS_DIR/$RUN_ID"
	printf '%s\n' "$BREACHES" >"$BREACH_FILE"
fi

if ((VERDICT_STATUS != 0)) && ((TEARDOWN == 1)) && ((BREACHES < BREACHES_REQUIRED)); then
	log "the verdict is not PASS (breach $BREACHES of $BREACHES_REQUIRED), so $RUN_ID is left running"
fi

if ((VERDICT_STATUS != 0)) && ((TEARDOWN == 1)) && ((BREACHES >= BREACHES_REQUIRED)); then
	# Any non-zero answer, including a gate that found no measurement to judge:
	# each of them says the run is not worth paying for another minute of.
	log "the verdict has not been PASS for $BREACHES consecutive ticks, so tearing $RUN_ID down"
	# The tag reaches teardown.sh, whose drop-topic Job would otherwise default
	# to this checkout's commit — which need not be the commit that was pushed.
	TEARDOWN_ARGS=("$RUN_ID" --site "$SITE_FILE")
	[[ -z $IMAGE_TAG ]] || TEARDOWN_ARGS+=(--image-tag "$IMAGE_TAG")
	# The verdict is what this script exits with, so a teardown that failed is
	# reported rather than left to replace it: the exit codes here are the
	# gate's own, and a caller reading one this script never defines would have
	# to guess whether the run passed. The fleet outliving its verdict is the
	# operator's to act on, which is what the line below is for.
	TEARDOWN_STATUS=0
	"$REPO_ROOT/scripts/teardown.sh" "${TEARDOWN_ARGS[@]}" || TEARDOWN_STATUS=$?
	((TEARDOWN_STATUS == 0)) ||
		log "tearing $RUN_ID down exited $TEARDOWN_STATUS, so its fleet may still be running: scripts/teardown.sh $RUN_ID"
fi
exit "$VERDICT_STATUS"
