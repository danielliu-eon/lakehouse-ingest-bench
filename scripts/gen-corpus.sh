#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Generate a corpus in cluster Jobs, then merge shard metadata when needed. Pods write
# directly to the run's object store using their configured identity.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Generation and merge wait timeouts; these do not limit the Jobs themselves.
GEN_WAIT_S="${GEN_WAIT_S:-14400}"
MERGE_WAIT_S="${MERGE_WAIT_S:-1800}"

# Each generator buffers a whole batch. Allow roughly ten times offered_bytes_per_s *
# batch_interval_ms / 1000 in memory per process, regardless of shard count. The default
# fits smoke; see docs/running.md for larger presets.
GEN_MEMORY="${GEN_MEMORY:-2Gi}"

usage() {
	cat <<'USAGE'
usage: scripts/gen-corpus.sh <preset> [options]

  <preset>           a shipped preset name, or a path to one inside the image
  --shards N         generate in N pods, then merge them (default: 1)
  --seed S           generator seed, included in the corpus name (default: 1)
  --site PATH        site config for the cluster and the corpus root (default: ./site.yaml)
  --image-tag TAG    the harness image tag to run (default: this checkout's commit)

Environment: GEN_WAIT_S, MERGE_WAIT_S, GEN_MEMORY.

Prints only the corpus URI to stdout.
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
		[[ -z $PRESET ]] || die "expected one preset; got '$PRESET' and '$1'"
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
# Sharded generation checks uploaded metadata before starting the merge.
((SHARDS == 1)) || require_host_tools aws

k8s_read_site
CORPUS_ROOT="$(site_root '.corpus_root')"
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

# Read the generated URI from the harness log. Capture before awk to avoid an early pipe
# close causing SIGPIPE under pipefail.
wrote_uri() {
	local logs reported
	logs="$(k8s_job_logs "$1")" || die "could not read job/$1's log; try: kubectl logs job/$1"
	reported="$(awk '/^wrote /{print $2; exit}' <<<"$logs")"
	[[ -n $reported ]] || die "job/$1 printed no 'wrote' line; read its log with: kubectl logs job/$1"
	# The report separates the URI from its figures with a colon.
	printf '%s' "${reported%:}"
}

# ---------------------------------------------------------------------------
# Generate
# ---------------------------------------------------------------------------

if ((SHARDS == 1)); then
	GEN_COMMAND="gen-corpus --preset $PRESET --out $CORPUS_ROOT --seed $SEED"
else
	# Give each shard a separate output prefix so its metadata cannot overwrite another
	# shard's. Expand JOB_COMPLETION_INDEX in the pod's shell.
	GEN_COMMAND="gen-corpus --preset $PRESET --out $CORPUS_ROOT/shards/\$JOB_COMPLETION_INDEX"
	GEN_COMMAND="$GEN_COMMAND --shard-index \$JOB_COMPLETION_INDEX --shard-count $SHARDS --seed $SEED"
fi

# Include the preset in the Job name so different presets can generate concurrently.
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
	# Assign first so a failed command substitution reaches set -e instead of becoming a
	# successful printf of an empty string.
	CORPUS_URI="$(wrote_uri "$GEN_JOB")"
	log "corpus generated"
	printf '%s\n' "$CORPUS_URI"
	exit 0
fi

# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

# Read the corpus directory name from this generation's log; shard prefixes may also
# contain older corpora. Any indexed Job pod reports the same final directory name. Keep
# shard directories because merged metadata references their batches.
SHARD_DIR="$(wrote_uri "$GEN_JOB")"
SHARD_DIR="${SHARD_DIR##*/}"

# Check each shard's metadata before launching a merge pod. aws s3 ls fails when the path
# is absent.
MERGE_COMMAND="merge-corpus"
shard=0
while ((shard < SHARDS)); do
	SHARD_CORPUS="$CORPUS_ROOT/shards/$shard/$SHARD_DIR"
	aws s3 ls "$SHARD_CORPUS/corpus.json" >/dev/null ||
		die "could not find corpus.json at $SHARD_CORPUS for shard $shard of $SHARDS; check storage access and read the job log: kubectl logs job/$GEN_JOB"
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
