#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Collect a completed run's verdict and optionally publish it. Fetch artifacts from object
# storage after the pods stop. Measure geometry here to keep manifest walks out of the
# scoring poll loop.
# Exit 0 only when run_valid is true.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Must match ingest_bench.scorer.cli.NO_GEOMETRY. A table without commits has no geometry;
# other nonzero statuses are read failures.
NO_GEOMETRY=4

usage() {
	cat <<'USAGE'
usage: scripts/finish.sh <run_id> [options]

  <run_id>            run with scorer output under the runs prefix
  --site PATH         site config for the runs prefix and the catalog (default: ./site.yaml)
  --variant NAME      tuning variant recorded in the report and result filename (default: hash)
  --publish DIR       publish the report under DIR/<engine>/ and refresh DIR/RESULTS.md
  --publish-invalid   publish even though `run_valid` is false, as a result labelled by its state

Environment: RUNS_DIR.

Prints the verdict and exits 0 only when `run_valid: true`.
USAGE
}

RUN_ID=""
VARIANT=""
PUBLISH_DIR=""
PUBLISH_INVALID=no
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--variant)
		VARIANT="${2:?--variant needs a name}"
		shift 2
		;;
	--publish)
		PUBLISH_DIR="${2:?--publish needs a directory}"
		shift 2
		;;
	--publish-invalid)
		PUBLISH_INVALID=yes
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
		[[ -z $RUN_ID ]] || die "expected one run ID; got '$RUN_ID' and '$1'"
		RUN_ID="$1"
		shift
		;;
	esac
done

[[ -n $RUN_ID ]] || {
	printf 'a run ID is required\n\n' >&2
	usage >&2
	exit 2
}
[[ $PUBLISH_INVALID == no || -n $PUBLISH_DIR ]] ||
	die "--publish-invalid requires --publish <dir>"

require_host_tools aws jq yq kubectl
require_site_file
RUNS_ROOT="$(site_root '.runs_root')"
# Validate the Kubernetes context before a background catalog tunnel may need it.
KUBE_CONTEXT="$(site_required '.kubernetes.context')"

RUN_DIR="$RUNS_DIR/$RUN_ID"
SCORES="$RUN_DIR/scores"
SUMMARY="$SCORES/summary.json"
GEOMETRY="$SCORES/geometry.json"

# ---------------------------------------------------------------------------
# 1. The artifacts the pods wrote
# ---------------------------------------------------------------------------

mkdir -p "$SCORES"
if [[ ! -f $RUN_DIR/engine-pods.json ]]; then
	aws s3 cp "$RUNS_ROOT/$RUN_ID/stage/engine-pods.json" "$RUN_DIR/engine-pods.json" --only-show-errors >&2 ||
		log "no captured engine pod requests; managed engine cost will be unavailable"
fi
log "fetching $RUNS_ROOT/$RUN_ID/scores/ into $SCORES"
aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/" "$SCORES/" --only-show-errors >&2 ||
	die "could not fetch $RUNS_ROOT/$RUN_ID/scores/; check that the run was launched and that you can read the bucket"

# Fetch per-shard publish logs to derive producer pacing and producer_bound. Missing logs
# are recorded in the result.
PRODUCER="$RUN_DIR/producer"
mkdir -p "$PRODUCER"
log "fetching $RUNS_ROOT/$RUN_ID/producer/ into $PRODUCER"
aws s3 sync "$RUNS_ROOT/$RUN_ID/producer/" "$PRODUCER/" --only-show-errors >&2 ||
	log "could not fetch $RUNS_ROOT/$RUN_ID/producer/; the report will list publish logs as missing"

# ---------------------------------------------------------------------------
# 2. The geometry
# ---------------------------------------------------------------------------

# Use saved metadata so geometry does not require a live catalog entry. Pass catalog
# properties for the object-store settings needed to read manifests.
METADATA_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
if [[ -f $METADATA_FINAL ]]; then
	# Install cleanup before catalog properties can open a tunnel.
	trap k8s_port_forward_stop EXIT
	read_catalog_prop_flags
	# Omit an unspecified ladder to preserve file-sizes defaults.
	OFFSETS="$(yq '[.scoring.geometry_offsets_s // [] | .[] | tostring] | join(",")' "$RUN_DIR/spec.yaml")" ||
		die "could not read scoring.geometry_offsets_s out of $RUN_DIR/spec.yaml"
	OFFSET_FLAGS=()
	[[ -z $OFFSETS ]] || OFFSET_FLAGS=(--offsets "$OFFSETS")
	# Geometry offsets are relative to the launch epoch.
	EPOCH="$(jq -r '.epoch // empty' "$RUN_DIR/facts.json")"
	[[ -n $EPOCH ]] || die "$RUN_DIR/facts.json has no epoch for geometry offsets; was this run launched?"

	log "measuring file geometry for $RUN_ID"
	GEOMETRY_STATUS=0
	harness_local --extra aws file-sizes \
		--metadata "$(abs_path "$METADATA_FINAL")" \
		--epoch "$EPOCH" \
		--out "$(abs_path "$SCORES")" \
		${OFFSET_FLAGS[@]+"${OFFSET_FLAGS[@]}"} \
		${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"} || GEOMETRY_STATUS=$?
	if ((GEOMETRY_STATUS == NO_GEOMETRY)); then
		log "no geometry: the table never committed"
	elif ((GEOMETRY_STATUS != 0)); then
		die "could not measure the geometry: file-sizes exited $GEOMETRY_STATUS; see the error above"
	fi
else
	log "$METADATA_FINAL is missing; run scripts/teardown.sh to save metadata before measuring geometry"
fi

# ---------------------------------------------------------------------------
# 3. The run's document
# ---------------------------------------------------------------------------

VARIANT_FLAGS=()
[[ -z $VARIANT ]] || VARIANT_FLAGS=(--variant "$VARIANT")
RUN_DIR_ABS="$(abs_path "$RUN_DIR")"
SITE_ABS="$(abs_path "$SITE_FILE")"

log "collecting $RUN_ID"
harness_local --extra aws collect --run-dir "$RUN_DIR_ABS" --site "$SITE_ABS" \
	${VARIANT_FLAGS[@]+"${VARIANT_FLAGS[@]}"}

# ---------------------------------------------------------------------------
# 4. The published result
# ---------------------------------------------------------------------------

# Publish before print_verdict exits on invalid runs, allowing --publish-invalid to take
# effect.
if [[ -n $PUBLISH_DIR ]]; then
	[[ -f $SUMMARY ]] || die "the scorer published no $SUMMARY, so there is no verdict to publish against"
	if [[ "$(jq -r .run_valid "$SUMMARY")" != true && $PUBLISH_INVALID == no ]]; then
		die "run_valid is false, so $RUN_ID is not a headline result; --publish-invalid publishes it labelled by its state"
	fi
	# Let collect derive the published filename. The trailing slash identifies a directory.
	# Reject a missing engine instead of publishing into a literal null directory.
	ENGINE="$(jq -r '.run.engine // empty' "$RUN_DIR/run.json")" ||
		die "could not read $RUN_DIR/run.json; the line above is jq's own error"
	[[ -n $ENGINE ]] || die "$RUN_DIR/run.json has no engine; cannot choose the results directory"
	# Match collect.validate's missing-machine rules before writing a result.
	MISSING_MACHINE_ROLES="$(jq -r '
		.run.fleet | if length == 0 then "empty fleet" else
			.[] | (.machine_type // "") as $machine_type |
			select($machine_type == "" or $machine_type == "unspecified" or
				($machine_type | startswith("YOUR_"))) | .role
		end
	' "$RUN_DIR/run.json")" || die "could not read fleet machine types from $RUN_DIR/run.json"
	[[ -z $MISSING_MACHINE_ROLES ]] ||
		die "cannot publish: set a real machine_type in the run spec for: $MISSING_MACHINE_ROLES"
	# Create the directory before resolving its absolute path.
	mkdir -p "$PUBLISH_DIR"
	PUBLISH_ABS="$(abs_path "$PUBLISH_DIR")"
	log "publishing $RUN_ID under $PUBLISH_DIR/$ENGINE/"
	harness_local --extra aws collect --run-dir "$RUN_DIR_ABS" --site "$SITE_ABS" \
		--out "$PUBLISH_ABS/$ENGINE/" ${VARIANT_FLAGS[@]+"${VARIANT_FLAGS[@]}"}

	# Regenerate RESULTS.md when the renderer is available. Use the same aws extra as other
	# calls to avoid uv resyncing between dependency sets.
	if harness_available results-table; then
		harness_local --extra aws results-table "$PUBLISH_ABS" --out "$PUBLISH_ABS/RESULTS.md"
	else
		log "no results-table command in this checkout, so $PUBLISH_DIR/RESULTS.md is left as it is"
	fi
fi

# ---------------------------------------------------------------------------
# 5. The verdict
# ---------------------------------------------------------------------------

print_verdict "$SUMMARY" "$GEOMETRY"
log "run_valid: true — $RUN_ID, artifacts in $RUN_DIR"
