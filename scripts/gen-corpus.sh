#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Build a corpus on the cluster: one Job of N shards, then a merge if there was
# more than one shard.
#
# On the cluster and not on a laptop because a corpus is tens to hundreds of
# gigabytes written into the same bucket the run reads it from, and the pods
# already hold the identity that may write there.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Generation is hours for the large presets and minutes for the smoke; the merge
# reads every shard's metadata and writes one document, so it is minutes either
# way. Both are waits, not budgets — see k8s_wait_job.
GEN_WAIT_S="${GEN_WAIT_S:-14400}"
MERGE_WAIT_S="${MERGE_WAIT_S:-1800}"

# What one generator pod asks for. The generator holds a whole batch in memory
# while it encodes one, so its peak follows the preset's batch bytes —
# `offered_bytes_per_s x batch_interval_ms / 1000`, and roughly ten times that
# resident — and not the shard count. The default fits the smoke preset; see
# "Generating a corpus" in docs/running.md for what the larger ones need.
GEN_MEMORY="${GEN_MEMORY:-2Gi}"

usage() {
	cat <<'USAGE'
usage: scripts/gen-corpus.sh <preset> [options]

  <preset>           a shipped preset name, or a path to one inside the image
  --shards N         generate in N pods, then merge them (default: 1)
  --seed S           the generator's seed, which is part of the corpus's name (default: 1)
  --site PATH        the site config naming the cluster and the corpus root (default: ./site.yaml)
  --image-tag TAG    the harness image tag to run (default: this checkout's commit)

Environment: GEN_WAIT_S, MERGE_WAIT_S, GEN_MEMORY.

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
# Only a sharded generation reads the bucket, and then to check that every
# shard published its batches before the merge is launched over them.
((SHARDS == 1)) || require_host_tools aws

k8s_read_site
CORPUS_ROOT="$(site_root '.corpus_root')"
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

# Named after the preset, because `k8s_delete job` below removes whatever holds
# the name: two generations of different presets would otherwise be one Job, and
# the second would delete the first hours into it.
GEN_JOB="corpus-gen-$(k8s_object_name "$(basename -- "$PRESET")")"
MERGE_JOB="corpus-merge-$(k8s_object_name "$(basename -- "$PRESET")")"

log "generating $PRESET in $SHARDS shard(s) with $IMAGE"
k8s_delete job "$GEN_JOB"
k8s_render_apply deploy/k8s/corpus-gen-job.yaml.tmpl \
	"NAME=$GEN_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$GEN_COMMAND" \
	"COUNT=$SHARDS" \
	"MEMORY=$GEN_MEMORY" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
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

# The directory this generation wrote, by name. A corpus directory is its
# preset's name and the hash of that preset, so one generation writes the same
# name under every `shards/<i>/` prefix — and a prefix holds one such directory
# per sharded generation the bucket has ever seen, because the merge leaves
# every batch where its shard wrote it: the shard directories are part of each
# merged corpus and are never cleaned up. Which is why the name is read from
# what this generation reported writing rather than from what the prefix holds,
# where the second preset generated into a bucket would find two.
#
# `kubectl logs` over an indexed Job answers with one of its pods, and nothing
# here depends on which: the shard index is in the prefix, and the last segment
# — the only part read below — is the same for all of them.
#
# Assigned before it is read, so a `wrote_uri` that refused ends this script as
# a failed assignment rather than as an empty name; see the single-shard path.
SHARD_DIR="$(wrote_uri "$GEN_JOB")"
SHARD_DIR="${SHARD_DIR##*/}"

# Each shard's own metadata document, read before a merge pod is paid for. The
# merge refuses a shard it cannot read too, but only once a pod has been
# scheduled and an image pulled — and after a generation of hours that answer
# is wanted at once. `aws s3 ls` exits non-zero over a path that matches
# nothing, which is what makes this a check and not a listing.
MERGE_COMMAND="merge-corpus"
shard=0
while ((shard < SHARDS)); do
	SHARD_CORPUS="$CORPUS_ROOT/shards/$shard/$SHARD_DIR"
	aws s3 ls "$SHARD_CORPUS/corpus.json" >/dev/null ||
		die "$SHARD_CORPUS holds no corpus.json, so shard $shard of $SHARDS published none of its batches; read job/$GEN_JOB's log with: kubectl logs job/$GEN_JOB"
	MERGE_COMMAND="$MERGE_COMMAND $SHARD_CORPUS"
	shard=$((shard + 1))
done
MERGE_COMMAND="$MERGE_COMMAND --out $CORPUS_ROOT"

log "merging $SHARDS shards of $SHARD_DIR"
k8s_delete job "$MERGE_JOB"
k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
	"NAME=$MERGE_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$MERGE_COMMAND" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$MERGE_JOB" "$MERGE_WAIT_S"

CORPUS_URI="$(wrote_uri "$MERGE_JOB")"
log "corpus merged"
printf '%s\n' "$CORPUS_URI"
