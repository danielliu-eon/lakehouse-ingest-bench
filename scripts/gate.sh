#!/usr/bin/env bash
# Judge a run that is still going, from the artifacts the scorer publishes.
#
# The scorer's two published files rather than the table: the scorer has
# already paid for that read, and two readers of one table would disagree about
# when a commit became visible — so the run's freshness figures would depend on
# which of them was asked.
#
# It exits with the gate's own code: 0 PASS, 3 UNDERSIZED, 5 VOID.
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
  --teardown         tear the run down when the verdict is not PASS

Environment: RUNS_DIR.

Exit codes: 0 PASS, 3 UNDERSIZED, 5 VOID.
USAGE
}

RUN_ID=""
TEARDOWN=0
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--teardown)
		TEARDOWN=1
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

if ((VERDICT_STATUS != 0)) && ((TEARDOWN == 1)); then
	# Any non-zero answer, including a gate that found no measurement to judge:
	# each of them says the run is not worth paying for another minute of.
	log "the verdict is not PASS, so tearing $RUN_ID down"
	"$REPO_ROOT/scripts/teardown.sh" "$RUN_ID" --site "$SITE_FILE"
fi
exit "$VERDICT_STATUS"
