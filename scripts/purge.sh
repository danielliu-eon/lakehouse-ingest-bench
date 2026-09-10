#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
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

# Read once, because both of the harness commands below take them: the one that
# asks the catalog whether it still holds the table, and the one that drops it.
read_catalog_prop_flags

# A teardown copies the table's last metadata document beside the run, and that
# copy is what says where the table's files are. A run torn down by hand, or
# never torn down at all, has no such copy — and its absence says nothing about
# whether the table is there. Reading it as "no table" is what would leave a
# table holding every byte the run wrote while reporting a purge that
# succeeded, so the catalog is asked instead.
#
# `table-metadata` answers all three cases: the document's URI for a table the
# catalog holds, TABLE_ABSENT for one it does not, and any other code for a
# catalog it could not reach — which has to stay a refusal, because "I could
# not ask" and "there is nothing there" are different answers and only one of
# them makes removing the run's artifacts safe.
METADATA_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
# The document this script actually reads the location out of: the copy beside
# the run where a teardown left one, and a temporary file where the catalog
# answered instead.
METADATA_DOCUMENT="$METADATA_FINAL"
HAVE_TABLE=yes
if [[ ! -f $METADATA_FINAL ]]; then
	log "no $METADATA_FINAL, so the catalog is asked whether it still holds $TABLE"
	CURRENT_STATUS=0
	CURRENT="$(harness_local --extra aws table-metadata --table "$TABLE" \
		${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"})" || CURRENT_STATUS=$?
	if ((CURRENT_STATUS == 0)); then
		log "the catalog puts $TABLE's metadata at $CURRENT"
		# Into a temporary file rather than into the run directory: nothing is
		# confirmed at this point, and a purge that is declined has to leave
		# that directory as it found it — which is what the closing line says.
		METADATA_DOCUMENT="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-metadata.XXXXXX")" ||
			die "could not make a temporary file to fetch $TABLE's metadata document into"
		trap 'rm -f "$METADATA_DOCUMENT"' EXIT
		# Fetched through the same reader a teardown uses, so a compressed
		# document reaches the `jq` below as the JSON it parses.
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
	# The document's own statement of where its table lives, which is what makes
	# this a deletion of that table's files and not of a prefix that reads like it.
	LOCATION="$(jq -r '.location // empty' "$METADATA_DOCUMENT")"
	[[ -n $LOCATION ]] || die "$METADATA_DOCUMENT carries no location, so it does not say which prefix holds $TABLE's files"

	# A table's location has to look like one: under the site's warehouse root,
	# and naming something below it. `aws s3 rm --recursive` takes a prefix and
	# asks nothing, and every prefix at or above this one is other data — the
	# warehouse root is every table the site has ever held, and a bucket root is
	# the corpus and every run's artifacts besides. A document that named either
	# would otherwise pass the non-empty check above and be removed whole.
	WAREHOUSE="$(site_root '.warehouse')"
	WAREHOUSE="${WAREHOUSE%/}"
	[[ $LOCATION == "$WAREHOUSE"/* ]] ||
		die "$METADATA_DOCUMENT puts $TABLE at '$LOCATION', which is not under this site's warehouse $WAREHOUSE; nothing is removed"
	# Every trailing separator, so neither the root itself nor the root with a
	# separator or two after it reads as a prefix of its own.
	[[ ${LOCATION#"$WAREHOUSE"/} == *[!/]* ]] ||
		die "$METADATA_DOCUMENT puts $TABLE at the warehouse root $WAREHOUSE itself, which holds every table this site has; nothing is removed"
fi

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

# Nothing to reclaim and nothing asked for: no table in the catalog and no
# `--artifacts`. Said and left, rather than prompting over an empty list and
# then reporting a purge — this is the one script that deletes measured data,
# and a success line it did not earn is worse than a refusal.
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
	# The catalog entry first, so nothing can load a table whose files are on
	# their way out. `drop-table` accepts a table that is already gone, which is
	# what makes a second purge after a partial one converge.
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
