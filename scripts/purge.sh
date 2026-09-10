#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Delete a collected run's table and data, and optionally its stored artifacts. Keep this
# separate from teardown so stopping a fleet preserves evidence. List deletions and
# require confirmation unless --yes is supplied.
# Read the table location from metadata; never infer a deletion prefix from its name.
set -euo pipefail
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
                 logs and the metadata document this script read the location out of. For a run
                 whose table the catalog no longer holds it is the only thing there is to remove
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

require_host_tools kubectl aws yq jq gzip

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; run this where stage.sh fetched it, or set RUNS_DIR"

k8s_read_site

TABLE="$(jq -r .table "$FACTS")"
[[ -n $TABLE && $TABLE != null ]] || die "$FACTS names no table"

# Reuse catalog properties for metadata lookup and table deletion.
# Install tunnel cleanup before reading catalog properties.
trap k8s_port_forward_stop EXIT
read_catalog_prop_flags

# Use saved metadata when available; otherwise ask the catalog. Missing local metadata
# does not mean the table is absent. Distinguish TABLE_ABSENT from connection or
# credential failures before allowing deletion.
METADATA_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
# Read saved metadata or a temporary copy fetched from the catalog.
METADATA_DOCUMENT="$METADATA_FINAL"
HAVE_TABLE=yes
if [[ ! -f $METADATA_FINAL ]]; then
	log "no $METADATA_FINAL, so the catalog is asked whether it still holds $TABLE"
	CURRENT_STATUS=0
	CURRENT="$(harness_local --extra aws table-metadata --table "$TABLE" \
		${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"})" || CURRENT_STATUS=$?
	if ((CURRENT_STATUS == 0)); then
		log "the catalog puts $TABLE's metadata at $CURRENT"
		# Use a temporary file so declining confirmation leaves the run directory unchanged.
		METADATA_DOCUMENT="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-metadata.XXXXXX")" ||
			die "could not make a temporary file to fetch $TABLE's metadata document into"
		trap 'k8s_port_forward_stop; rm -f "$METADATA_DOCUMENT"' EXIT
		# Normalize compressed metadata through the same reader as teardown.
		k8s_fetch_metadata_document "$CURRENT" "$METADATA_DOCUMENT" ||
			die "could not fetch $CURRENT, which is where the catalog says $TABLE's metadata is; nothing is removed"
	elif ((CURRENT_STATUS == TABLE_ABSENT)); then
		HAVE_TABLE=no
	else
		die "could not ask the catalog whether it holds $TABLE: table-metadata exited $CURRENT_STATUS; nothing is removed. The lines above are its own error — run scripts/teardown.sh once it is reachable, or fix the catalog properties in $SITE_FILE"
	fi
fi

LOCATION=""
if [[ $HAVE_TABLE == yes ]]; then
	# Delete the location declared by the table's metadata.
	LOCATION="$(jq -r '.location // empty' "$METADATA_DOCUMENT")"
	[[ -n $LOCATION ]] || die "$METADATA_DOCUMENT carries no location, so it does not say which prefix holds $TABLE's files"

	# Require a location strictly below the warehouse root. Recursive deletion of the root or
	# a parent prefix would remove other tables or run data.
	WAREHOUSE="$(site_root '.warehouse')"
	WAREHOUSE="${WAREHOUSE%/}"
	[[ $LOCATION == "$WAREHOUSE"/* ]] ||
		die "$METADATA_DOCUMENT puts $TABLE at '$LOCATION', which is not under this site's warehouse $WAREHOUSE; nothing is removed"
	# Strip all trailing separators before comparing roots.
	[[ ${LOCATION#"$WAREHOUSE"/} == *[!/]* ]] ||
		die "$METADATA_DOCUMENT puts $TABLE at the warehouse root $WAREHOUSE itself, which holds every table this site has; nothing is removed"
fi

# ---------------------------------------------------------------------------
# Refuse while the run is still being read
# ---------------------------------------------------------------------------

# Refuse while the scorer can still read the table or write artifacts; deleting either
# would invalidate its measurements.
SCORER="$(scorer_job "$RUN_ID")"
STILL_RUNNING="$(k8s_object_present job "$SCORER")" ||
	die "cannot tell whether $RUN_ID is still being scored, so nothing is removed; the line above says why"
[[ -z $STILL_RUNNING ]] ||
	die "job/$SCORER is still in $SITE_NAMESPACE, so $RUN_ID is still being scored; run scripts/teardown.sh first"

# ---------------------------------------------------------------------------
# Say what goes, then ask
# ---------------------------------------------------------------------------

# Return without prompting when there is nothing to delete.
if [[ $HAVE_TABLE == no && $ARTIFACTS == no ]]; then
	printf 'purging %s removes nothing: this catalog holds no %s, and its own prefix goes only with --artifacts.\n' \
		"$RUN_ID" "$TABLE"
	exit 0
fi

printf 'purging %s removes, permanently:\n' "$RUN_ID"
if [[ $HAVE_TABLE == yes ]]; then
	printf '  the table    %s\n' "$TABLE"
	printf '  its files    %s\n' "$LOCATION"
else
	printf '  no table:    this catalog holds no %s, so only the prefix below goes\n' "$TABLE"
fi
if [[ $ARTIFACTS == yes ]]; then
	printf '  its run      %s/%s/\n' "$RUNS_ROOT" "$RUN_ID"
fi

if [[ $ASSUME_YES == no ]]; then
	confirm "remove all of the above?"
fi

# ---------------------------------------------------------------------------
# Remove it
# ---------------------------------------------------------------------------

if [[ $HAVE_TABLE == yes ]]; then
	# Drop the catalog entry before its files. drop-table tolerates absence so a partial
	# purge can be retried.
	log "dropping $TABLE"
	harness_local --extra aws drop-table --table "$TABLE" ${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"}

	log "removing $LOCATION"
	aws s3 rm --recursive "$LOCATION" >&2 || die "could not remove $LOCATION; $TABLE is already out of the catalog"
fi

if [[ $ARTIFACTS == yes ]]; then
	log "removing $RUNS_ROOT/$RUN_ID/"
	aws s3 rm --recursive "$RUNS_ROOT/$RUN_ID/" >&2 || die "could not remove $RUNS_ROOT/$RUN_ID/"
fi

log "purged $RUN_ID; $RUN_DIR on this machine is untouched"
