#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Build a corpus on the cluster: one Job of N shards, then a merge if there was
# more than one shard.
#
# On the cluster and not on a laptop because a corpus is tens to hundreds of
# gigabytes written into the same bucket the run reads it from, and the pods
# already hold the identity that may write there.
set -euo pipefail
# The tools a missing prerequisite points at.
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

GEN_JOB=corpus-gen
MERGE_JOB=corpus-merge

# Generation is hours for the large presets and minutes for the smoke; the merge
# reads every shard's metadata and writes one document, so it is minutes either
# way. Both are waits, not budgets — see k8s_wait_job.
GEN_WAIT_S="${GEN_WAIT_S:-14400}"
MERGE_WAIT_S="${MERGE_WAIT_S:-1800}"

usage() {
	cat <<'USAGE'
usage: scripts/gen-corpus.sh <preset> [options]

  <preset>           a shipped preset name, or a path to one inside the image
  --shards N         generate in N pods, then merge them (default: 1)
  --seed S           the generator's seed, which is part of the corpus's name (default: 1)
  --site PATH        the site config naming the cluster and the corpus root (default: ./site.yaml)
  --image-tag TAG    the harness image tag to run (default: this checkout's commit)

Environment: GEN_WAIT_S, MERGE_WAIT_S.

It prints the corpus URI, and nothing else, on stdout.
USAGE
}

PRESET=""
SHARDS=1
SEED=1
IMAGE_TAG=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--shards)
		SHARDS="${2:?--shards needs a count}"
		shift 2
		;;
	--seed)
		SEED="${2:?--seed needs an integer}"
		shift 2
		;;
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--image-tag)
		IMAGE_TAG="${2:?--image-tag needs a tag}"
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
		[[ -z $PRESET ]] || die "this takes one preset, and was given both '$PRESET' and '$1'"
		PRESET="$1"
		shift
		;;
	esac
done

[[ -n $PRESET ]] || {
	printf 'a preset is required\n\n' >&2
	usage >&2
	exit 2
}
[[ $SHARDS =~ ^[1-9][0-9]*$ ]] || die "--shards must be a positive integer, got '$SHARDS'"
[[ $SEED =~ ^[0-9]+$ ]] || die "--seed must be a non-negative integer, got '$SEED'"

require_host_tools kubectl yq git
# The shard directory is resolved by listing the bucket, and only a sharded
# corpus has one to resolve.
((SHARDS == 1)) || require_host_tools aws

k8s_read_site
CORPUS_ROOT="$(site_required '.corpus_root')"
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

# The harness command that wrote a corpus reports the URI it wrote, so nothing
# here has to rediscover it by listing the bucket.
#
# The log is read into a variable and parsed from there rather than piped into
# `awk`: under `pipefail` an `awk` that stops at the line it wanted would fail
# the pipeline through `kubectl`, and the script would end with no message.
wrote_uri() {
	local logs reported
	logs="$(k8s_job_logs "$1")" || die "could not read job/$1's log; try: kubectl logs job/$1"
	reported="$(awk '/^wrote /{print $2; exit}' <<<"$logs")"
	[[ -n $reported ]] || die "job/$1 printed no 'wrote' line; read its log with: kubectl logs job/$1"
	# The generator's line ends the URI with a colon before its figures.
	printf '%s' "${reported%:}"
}

# ---------------------------------------------------------------------------
# Generate
# ---------------------------------------------------------------------------

if ((SHARDS == 1)); then
	GEN_COMMAND="gen-corpus --preset $PRESET --out $CORPUS_ROOT --seed $SEED"
else
	# Each shard writes under its own prefix. The generator names a corpus
	# directory after its preset and hash, so shards sharing one `--out` would
	# all write into that one directory and each publish metadata describing
	# its own batches alone — which is what the merge below exists to combine.
	# `$JOB_COMPLETION_INDEX` is escaped here and expanded by the shell that is
	# the image's entrypoint, so one rendered command serves every shard.
	GEN_COMMAND="gen-corpus --preset $PRESET --out $CORPUS_ROOT/shards/\$JOB_COMPLETION_INDEX"
	GEN_COMMAND="$GEN_COMMAND --shard-index \$JOB_COMPLETION_INDEX --shard-count $SHARDS --seed $SEED"
fi

log "generating $PRESET in $SHARDS shard(s) with $IMAGE"
k8s_delete job "$GEN_JOB"
k8s_render_apply deploy/k8s/corpus-gen-job.yaml.tmpl \
	"NAME=$GEN_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$GEN_COMMAND" \
	"COUNT=$SHARDS" \
	"ENV=$JOB_ENV" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$GEN_JOB" "$GEN_WAIT_S"

if ((SHARDS == 1)); then
	# Assigned before it is printed: `wrote_uri` refuses by calling `die`, which
	# inside a command substitution ends only that subshell — so a failure has
	# to reach `set -e` as a failed assignment rather than as an empty argument
	# to `printf`, which would print a blank line and exit 0.
	CORPUS_URI="$(wrote_uri "$GEN_JOB")"
	log "corpus generated"
	printf '%s\n' "$CORPUS_URI"
	exit 0
fi

# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

# Shard 0's directory names all of them: every shard generated the same preset
# at the same seed, so each wrote a directory of this one name. Naming them
# rather than listing every shard's prefix is also what makes a shard that
# wrote nothing fail the merge by name instead of being quietly left out.
LISTING="$(aws s3 ls "$CORPUS_ROOT/shards/0/")" ||
	die "could not list $CORPUS_ROOT/shards/0/; read job/$GEN_JOB's log with: kubectl logs job/$GEN_JOB"
SHARD_DIRS="$(awk '/ PRE /{ sub(/\/$/, "", $2); print $2 }' <<<"$LISTING")"
[[ -n $SHARD_DIRS ]] || die "no corpus directory under $CORPUS_ROOT/shards/0/; read job/$GEN_JOB's log"
DIR_COUNT="$(awk 'NF{count++} END{print count + 0}' <<<"$SHARD_DIRS")"
((DIR_COUNT == 1)) ||
	die "expected one corpus directory under $CORPUS_ROOT/shards/0/, found: $(tr '\n' ' ' <<<"$SHARD_DIRS")"

# The merge writes one metadata document beside the shard prefixes and leaves
# every batch where its shard wrote it, so the shard directories are part of
# the corpus and must not be cleaned up afterwards.
MERGE_COMMAND="merge-corpus"
shard=0
while ((shard < SHARDS)); do
	MERGE_COMMAND="$MERGE_COMMAND $CORPUS_ROOT/shards/$shard/$SHARD_DIRS"
	shard=$((shard + 1))
done
MERGE_COMMAND="$MERGE_COMMAND --out $CORPUS_ROOT"

log "merging $SHARDS shards of $SHARD_DIRS"
k8s_delete job "$MERGE_JOB"
k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
	"NAME=$MERGE_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$MERGE_COMMAND" \
	"ENV=$JOB_ENV" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$MERGE_JOB" "$MERGE_WAIT_S"

CORPUS_URI="$(wrote_uri "$MERGE_JOB")"
log "corpus merged"
printf '%s\n' "$CORPUS_URI"
