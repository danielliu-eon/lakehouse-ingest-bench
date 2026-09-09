#!/usr/bin/env bash
# Fetch a finished run's score and read its verdict.
#
# The scorer publishes its artifacts as it goes and its pod is gone by the time
# a run ends, so the artifacts are fetched from the runs prefix rather than from
# anything still running. It exits 0 only when the scorer published
# `run_valid: true`.
set -euo pipefail
# The tools a missing prerequisite points at.
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: scripts/finish.sh <run_id> [options]

  <run_id>           a run whose scorer has published under the runs prefix
  --site PATH        the site config naming the runs prefix (default: ./site.yaml)

Environment: RUNS_DIR.

It prints the verdict block and exits 0 only on `run_valid: true`.
USAGE
}

RUN_ID=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
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
		[[ -z $RUN_ID ]] || die "this reads one run, and was given both '$RUN_ID' and '$1'"
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

require_host_tools aws jq yq
require_site_file
RUNS_ROOT="$(site_required '.runs_root')"

SCORES="$RUNS_DIR/$RUN_ID/scores"
mkdir -p "$SCORES"
log "fetching $RUNS_ROOT/$RUN_ID/scores/ into $SCORES"
aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/" "$SCORES/" >&2 ||
	die "could not fetch $RUNS_ROOT/$RUN_ID/scores/; check that the run was launched and that you can read the bucket"

print_verdict "$SCORES/summary.json"
log "run_valid: true — $RUN_ID, artifacts in $RUNS_DIR/$RUN_ID"
