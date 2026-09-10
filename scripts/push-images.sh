#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Build and push harness and engine images tagged with the checkout's commit. Refuse
# uncommitted changes unless --allow-dirty is set, since the tag would otherwise
# misidentify the image's source.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: scripts/push-images.sh [options]

  --site PATH        the site config naming the registry (default: ./site.yaml)
  --platform PLAT    what to build the harness and Spark images for (default:
                     linux/amd64). Two, comma-separated, builds a manifest list
                     with buildx. The Flink image is amd64 whatever this says
  --allow-dirty      push from a tree with uncommitted changes, whose tag then
                     names a commit that is not what is in the image

It prints the image references it pushed, and nothing else, on stdout.
USAGE
}

ALLOW_DIRTY=0
# Default to amd64 for cluster nodes; the Flink image requires it.
PLATFORM=linux/amd64
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--platform)
		PLATFORM="${2:?--platform needs a platform, e.g. linux/arm64}"
		shift 2
		;;
	--allow-dirty)
		ALLOW_DIRTY=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		printf 'unknown argument %s\n\n' "$1" >&2
		usage >&2
		exit 2
		;;
	esac
done

require_host_tools aws docker git yq

[[ -f $SITE_FILE ]] || die "no site config at $SITE_FILE; copy site.aws.example.yaml and fill it in"
REGISTRY="$(site_required '.kubernetes.registry')"
REGION="$(site_required '.kubernetes.aws_region')"

TAG="$(git -C "$REPO_ROOT" rev-parse --short HEAD)" ||
	die "could not read this checkout's commit to tag the images with"
if ((ALLOW_DIRTY == 0)); then
	[[ -z "$(git -C "$REPO_ROOT" status --porcelain)" ]] ||
		die "the working tree has uncommitted changes, so tag $TAG would not name what is in the image; commit them, or pass --allow-dirty"
fi

HARNESS_REF="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"
FLINK_REF="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/flink:$TAG"
SPARK_REF="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/spark:$TAG"

log "signing in to $REGISTRY"
# Report login pipeline failures explicitly under pipefail.
if ! aws ecr get-login-password --region "$REGION" |
	docker login --username AWS --password-stdin "$REGISTRY" >&2; then
	die "could not sign in to $REGISTRY; check that your credentials reach that account in $REGION"
fi

# build_and_push <dockerfile> <reference> <platform>
build_and_push() {
	log "building $2 for $3"
	if [[ $3 == *,* ]]; then
		# Push multi-platform builds directly; the local daemon cannot load a manifest list.
		docker buildx build --platform "$3" -f "$1" -t "$2" --push "$REPO_ROOT" >&2
	else
		docker build --platform "$3" -f "$1" -t "$2" "$REPO_ROOT" >&2
		docker push "$2" >&2
	fi
}

build_and_push "$REPO_ROOT/Dockerfile" "$HARNESS_REF" "$PLATFORM"
# The pinned PyFlink dependency requires amd64.
build_and_push "$REPO_ROOT/engines/flink/Dockerfile" "$FLINK_REF" linux/amd64
# Build Spark for the same target platform as the harness.
build_and_push "$REPO_ROOT/engines/spark/Dockerfile" "$SPARK_REF" "$PLATFORM"

log "pushed every image at tag $TAG"
printf '%s\n%s\n%s\n' "$HARNESS_REF" "$FLINK_REF" "$SPARK_REF"
