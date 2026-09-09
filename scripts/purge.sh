#!/usr/bin/env bash
# Reclaim what a collected run is still paying for: its table, the warehouse
# data behind it and, on request, its artifacts.
#
# Separate from `teardown.sh` on purpose. A run's table is its result, and a
# teardown that dropped tables on its own would be a worse failure mode than an
# orphan — a campaign's evidence gone because a driver ran twice. So this is the
# one script that deletes measured data, it is never called by another, and
# every deletion is named before it happens and confirmed.
#
# The table's location comes out of the metadata document a teardown copied,
# never from the table's name: a catalog is free to place a table anywhere under
# its warehouse, and a prefix guessed from the name is a prefix that may belong
# to something else.
set -euo pipefail
# The tools a missing prerequisite points at.
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: scripts/purge.sh <run_id> [options]

  <run_id>       a run torn down under $RUNS_DIR, whose table and data are to be removed
  --site PATH    the site config naming the cluster, the catalog and the runs prefix (default: ./site.yaml)
  --artifacts    also remove the run's own prefix under the runs root — its scores, its publish
                 logs and the metadata document this script read the location out of
  --yes          do not ask; for a purge run from a script

Environment: RUNS_DIR.

It prints everything it is about to remove and asks before removing any of it.
USAGE
}

RUN_ID=""
ARTIFACTS=no
ASSUME_YES=no
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--artifacts)
		ARTIFACTS=yes
		shift
		;;
	--yes)
		ASSUME_YES=yes
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
		[[ -z $RUN_ID ]] || die "this purges one run, and was given both '$RUN_ID' and '$1'"
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

require_host_tools kubectl aws yq jq

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; run this where stage.sh fetched it, or set RUNS_DIR"

METADATA_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
[[ -f $METADATA_FINAL ]] ||
	die "no $METADATA_FINAL, so nothing here knows where $RUN_ID's data is; run scripts/teardown.sh first"

k8s_read_site

TABLE="$(jq -r .table "$FACTS")"
[[ -n $TABLE && $TABLE != null ]] || die "$FACTS names no table"
# The document's own statement of where its table lives, which is what makes
# this a deletion of that table's files and not of a prefix that reads like it.
LOCATION="$(jq -r '.location // empty' "$METADATA_FINAL")"
[[ -n $LOCATION ]] || die "$METADATA_FINAL carries no location, so it does not say which prefix holds $TABLE's files"

# A table's location has to look like one: under the site's warehouse root, and
# naming something below it. `aws s3 rm --recursive` takes a prefix and asks
# nothing, and every prefix at or above this one is other data — the warehouse
# root is every table the site has ever held, and a bucket root is the corpus
# and every run's artifacts besides. A document that named either would
# otherwise pass the non-empty check above and be removed whole.
WAREHOUSE="$(site_required '.warehouse')"
WAREHOUSE="${WAREHOUSE%/}"
[[ $LOCATION == "$WAREHOUSE"/* ]] ||
	die "$METADATA_FINAL puts $TABLE at '$LOCATION', which is not under this site's warehouse $WAREHOUSE; nothing is removed"
# Every trailing separator, so neither the root itself nor the root with a
# separator or two after it reads as a prefix of its own.
[[ ${LOCATION#"$WAREHOUSE"/} == *[!/]* ]] ||
	die "$METADATA_FINAL puts $TABLE at the warehouse root $WAREHOUSE itself, which holds every table this site has; nothing is removed"

# ---------------------------------------------------------------------------
# Refuse while the run is still being read
# ---------------------------------------------------------------------------

# The scorer opens the table on every poll and writes the artifacts a result is
# assembled from. Deleting either underneath it would not stop it — it would
# make it report loss and corruption against a table it can no longer read, so
# the run's last artifacts would be a lie about the engine.
SCORER="$(scorer_job "$RUN_ID")"
STILL_RUNNING="$(k8s_object_present job "$SCORER")" ||
	die "cannot tell whether $RUN_ID is still being scored, so nothing is removed; the line above says why"
[[ -z $STILL_RUNNING ]] ||
	die "job/$SCORER is still in $SITE_NAMESPACE, so $RUN_ID is still being scored; run scripts/teardown.sh first"

# ---------------------------------------------------------------------------
# Say what goes, then ask
# ---------------------------------------------------------------------------

printf 'purging %s removes, permanently:\n' "$RUN_ID"
printf '  the table    %s\n' "$TABLE"
printf '  its files    %s\n' "$LOCATION"
if [[ $ARTIFACTS == yes ]]; then
	printf '  its run      %s/%s/\n' "$RUNS_ROOT" "$RUN_ID"
fi

if [[ $ASSUME_YES == no ]]; then
	# stdin rather than the terminal device, so a caller can answer through a
	# pipe. A caller with no stdin at all is refused rather than defaulted:
	# "nothing answered" is not consent to delete measured data.
	[[ -t 0 ]] || die "nothing is attached to answer, and this does not assume one; pass --yes to purge unattended"
	printf 'remove all of the above? [y/N] '
	ANSWER=""
	read -r ANSWER || true
	case "$ANSWER" in
	y | Y) ;;
	*) die "answered '${ANSWER:-nothing}', so nothing was removed" ;;
	esac
fi

# ---------------------------------------------------------------------------
# Remove it
# ---------------------------------------------------------------------------

# The catalog entry first, so nothing can load a table whose files are on their
# way out. `drop-table` accepts a table that is already gone, which is what
# makes a second purge after a partial one converge.
read_catalog_prop_flags
log "dropping $TABLE"
harness_local --extra aws drop-table --table "$TABLE" ${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"}

log "removing $LOCATION"
aws s3 rm --recursive "$LOCATION" >&2 || die "could not remove $LOCATION; $TABLE is already out of the catalog"

if [[ $ARTIFACTS == yes ]]; then
	log "removing $RUNS_ROOT/$RUN_ID/"
	aws s3 rm --recursive "$RUNS_ROOT/$RUN_ID/" >&2 || die "could not remove $RUNS_ROOT/$RUN_ID/"
fi

log "purged $RUN_ID; $RUN_DIR on this machine is untouched"
