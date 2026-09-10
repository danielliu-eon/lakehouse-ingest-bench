#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Turn a torn-down run into a read verdict and, on request, a published result.
#
# The scorer publishes its artifacts as it goes and its pod is gone by the time
# a run ends, so the artifacts are fetched from the runs prefix rather than from
# anything still running. Geometry is measured here rather than during the run:
# it is a read of the metadata document, so it costs nothing to leave until the
# fleet is gone, and leaving it keeps a manifest walk off the poll loop that is
# timing commits.
#
# It exits 0 only when the scorer published `run_valid: true`.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# What `file-sizes` exits when the table it was pointed at holds no commit, as
# ingest_bench.scorer.cli.NO_GEOMETRY. A run that committed nothing has no
# geometry, which is a fact about the run rather than a failure to read it;
# every other non-zero exit is the read itself failing.
NO_GEOMETRY=4

usage() {
	cat <<'USAGE'
usage: scripts/finish.sh <run_id> [options]

  <run_id>            a run whose scorer has published under the runs prefix
  --site PATH         the site config naming the runs prefix and the catalog (default: ./site.yaml)
  --variant NAME      the tuning this run stands for, recorded in the document and in its published
                      name (default: collect's own, which is `hash`)
  --publish DIR       also write the document under DIR/<engine>/ as a published result, and
                      re-render DIR/RESULTS.md
  --publish-invalid   publish even though `run_valid` is false, as a result labelled by its state

Environment: RUNS_DIR.

It prints the verdict block and exits 0 only on `run_valid: true`.
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
[[ $PUBLISH_INVALID == no || -n $PUBLISH_DIR ]] ||
	die "--publish-invalid says how to publish, so it needs --publish <dir> to say where"

require_host_tools aws jq yq
require_site_file
RUNS_ROOT="$(site_root '.runs_root')"

RUN_DIR="$RUNS_DIR/$RUN_ID"
SCORES="$RUN_DIR/scores"
SUMMARY="$SCORES/summary.json"
GEOMETRY="$SCORES/geometry.json"

# ---------------------------------------------------------------------------
# 1. The artifacts the pods wrote
# ---------------------------------------------------------------------------

mkdir -p "$SCORES"
log "fetching $RUNS_ROOT/$RUN_ID/scores/ into $SCORES"
aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/" "$SCORES/" --only-show-errors >&2 ||
	die "could not fetch $RUNS_ROOT/$RUN_ID/scores/; check that the run was launched and that you can read the bucket"

# The publish logs as well, because they are the offered side of the run: every
# figure the document derives about the producer — how far behind its schedule
# it fell, and so whether the offer rather than the engine set the rate — is
# read from them. Each shard uploads its own as it goes, for the same reason the
# scorer does, and a run whose logs never arrived is still a run worth reading.
PRODUCER="$RUN_DIR/producer"
mkdir -p "$PRODUCER"
log "fetching $RUNS_ROOT/$RUN_ID/producer/ into $PRODUCER"
aws s3 sync "$RUNS_ROOT/$RUN_ID/producer/" "$PRODUCER/" --only-show-errors >&2 ||
	log "could not fetch $RUNS_ROOT/$RUN_ID/producer/; the document will name the publish logs as missing"

# ---------------------------------------------------------------------------
# 2. The geometry
# ---------------------------------------------------------------------------

# From the document a teardown copied rather than through the catalog: these
# figures are about files, and asking a catalog for them would make them depend
# on a service that holds none — one a finished campaign may already have
# dropped the table from. The catalog properties still reach the command, for
# the object-store settings among them: the manifests every figure is read from
# are in the bucket, and a client with no region resolves the wrong endpoint.
METADATA_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
if [[ -f $METADATA_FINAL ]]; then
	read_catalog_prop_flags
	# Empty when the spec sets no ladder, and the flag is then left off so that
	# `file-sizes` applies its own default rather than one restated here.
	OFFSETS="$(yq '[.scoring.geometry_offsets_s // [] | .[] | tostring] | join(",")' "$RUN_DIR/spec.yaml")" ||
		die "could not read scoring.geometry_offsets_s out of $RUN_DIR/spec.yaml"
	OFFSET_FLAGS=()
	[[ -z $OFFSETS ]] || OFFSET_FLAGS=(--offsets "$OFFSETS")
	# The epoch is the offsets' origin, and only the launch knew it.
	EPOCH="$(jq -r '.epoch // empty' "$RUN_DIR/facts.json")"
	[[ -n $EPOCH ]] || die "$RUN_DIR/facts.json records no epoch, so the ladder has no origin; was this run launched?"

	log "measuring the geometry $RUN_ID left behind"
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
		die "could not measure the geometry: file-sizes exited $GEOMETRY_STATUS; the lines above are its own error"
	fi
else
	log "no $METADATA_FINAL, so this run has no geometry; scripts/teardown.sh is what copies it"
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

# Before the verdict block, because that block ends the script on an invalid run
# and `--publish-invalid` exists precisely to publish one of those — labelled by
# its validity state rather than dropped.
if [[ -n $PUBLISH_DIR ]]; then
	[[ -f $SUMMARY ]] || die "the scorer published no $SUMMARY, so there is no verdict to publish against"
	if [[ "$(jq -r .run_valid "$SUMMARY")" != true && $PUBLISH_INVALID == no ]]; then
		die "run_valid is false, so $RUN_ID is not a headline result; --publish-invalid publishes it labelled by its state"
	fi
	# `collect` again rather than a copy of the document just written, because
	# the name a published result takes is the one `collect` derives from the
	# engine, the corpus and the variant: deriving it a second time here is how
	# the two spellings come apart. The trailing separator is what says the path
	# is a directory to be filled rather than a file to be written.
	#
	# `// empty` and a refusal, not `jq -r` alone: a document without the field
	# prints the string `null`, and the engine names the directory the result is
	# filed under — so the absence would publish into `<dir>/null/` rather than
	# say that the document is not one this can publish.
	ENGINE="$(jq -r '.run.engine // empty' "$RUN_DIR/run.json")" ||
		die "could not read $RUN_DIR/run.json; the line above is jq's own error"
	[[ -n $ENGINE ]] || die "$RUN_DIR/run.json names no engine, so there is no results directory to file it under"
	# Created before it is resolved, because an absolute path is taken by
	# walking to the directory: publishing into somewhere that does not exist
	# yet is a first result, not a mistake.
	mkdir -p "$PUBLISH_DIR"
	PUBLISH_ABS="$(abs_path "$PUBLISH_DIR")"
	log "publishing $RUN_ID under $PUBLISH_DIR/$ENGINE/"
	harness_local --extra aws collect --run-dir "$RUN_DIR_ABS" --site "$SITE_ABS" \
		--out "$PUBLISH_ABS/$ENGINE/" ${VARIANT_FLAGS[@]+"${VARIANT_FLAGS[@]}"}

	# The table is generated from the published documents and never edited by
	# hand, so it is re-rendered here rather than left to whoever remembers.
	# Guarded because a checkout may predate the renderer, and a driver that
	# refused on its absence would make publishing impossible in exactly the
	# tree where the documents themselves are fine.
	#
	# `--extra aws` although rendering a table needs no cloud SDK: it is the
	# extra every other call in this script asks for, and a checkout fallback
	# given two different extra sets re-syncs its environment between them.
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
