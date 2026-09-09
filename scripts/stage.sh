#!/usr/bin/env bash
# Stage a run on the cluster, and for a managed engine start it.
#
# Staging runs as a Job because it creates the topic, and a managed broker is
# reachable from inside its own network rather than from an operator's machine.
# The Job publishes its run directory to the runs prefix; everything after that
# — fetching the directory, applying the engine's two documents, waiting for the
# job to run — is `kubectl` and `aws` work, and stays here.
#
# It prints the run id, and nothing else, on stdout.
set -euo pipefail
# The tools a missing prerequisite points at.
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Staging resolves a corpus, reads its metadata, creates a topic and creates a
# table. Minutes at most, but on a cold node behind an image pull.
STAGE_WAIT_S="${STAGE_WAIT_S:-600}"
# How long the engine's job may take to reach RUNNING. Long: the operator has
# to schedule a JobManager and its TaskManagers, each of which pulls an image
# of a few hundred megabytes onto a node that may not exist yet.
ENGINE_RUNNING_WAIT_S="${ENGINE_RUNNING_WAIT_S:-600}"
ENGINE_POLL_S="${ENGINE_POLL_S:-10}"

usage() {
	cat <<'USAGE'
usage: scripts/stage.sh <spec> [options]

  <spec>             a run spec on this machine, copied into the Job's ConfigMap
  --site PATH        the site config naming the cluster, the broker and the catalog (default: ./site.yaml)
  --image-tag TAG    the harness and engine image tag to run (default: this checkout's commit)

Environment: STAGE_WAIT_S, ENGINE_RUNNING_WAIT_S, ENGINE_POLL_S, RUNS_DIR.

It prints `run_id: <id>` as its last stdout line, which launch.sh takes.
USAGE
}

SPEC=""
IMAGE_TAG=""
while [[ $# -gt 0 ]]; do
	case "$1" in
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
		[[ -z $SPEC ]] || die "this stages one spec, and was given both '$SPEC' and '$1'"
		SPEC="$1"
		shift
		;;
	esac
done

[[ -n $SPEC ]] || {
	printf 'a run spec is required\n\n' >&2
	usage >&2
	exit 2
}

require_host_tools kubectl aws yq jq git
[[ -f $SPEC ]] || die "no run spec at $SPEC"

k8s_read_site
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

# The spec's own name, which is already a DNS label because a run id is built
# from it. Naming the Job after the spec rather than after the run keeps it
# addressable before the run has an id.
SPEC_NAME="$(yq '.name' "$SPEC")"
[[ -n $SPEC_NAME && $SPEC_NAME != null ]] || die "$SPEC sets no name, so this run has nothing to be called"
ENGINE="$(yq '.engine' "$SPEC")"

STAGE_JOB="stage-$SPEC_NAME"
SPEC_CONFIGMAP="$STAGE_JOB-spec"
SITE_CONFIGMAP="$STAGE_JOB-site"

# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------

log "staging $SPEC as job/$STAGE_JOB with $IMAGE"
# A Job's spec is immutable, so the previous Job of this name goes first.
k8s_delete job "$STAGE_JOB"
k8s_configmap_from_file "$SPEC_CONFIGMAP" "$(basename -- "$SPEC")=$SPEC"
k8s_configmap_from_file "$SITE_CONFIGMAP" "site.yaml=$SITE_FILE"

STAGE_COMMAND="stage --spec /runs/$(basename -- "$SPEC") --site /site/site.yaml --runs-dir /work/runs"
STAGE_COMMAND="$STAGE_COMMAND --image-tag $TAG --upload-prefix $RUNS_ROOT"
k8s_render_apply deploy/k8s/stage-job.yaml.tmpl \
	"NAME=$STAGE_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$STAGE_COMMAND" \
	"ENV=$JOB_ENV" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS" \
	"SPEC_CONFIGMAP=$SPEC_CONFIGMAP" \
	"SITE_CONFIGMAP=$SITE_CONFIGMAP"
k8s_wait_job "$STAGE_JOB" "$STAGE_WAIT_S"

# The run id comes off the Job's own log rather than from a second derivation
# of it here: the stamp in it is the moment staging ran, and only staging knows
# that. Read into a variable first, because under `pipefail` an `awk` that stops
# at the line it wanted would fail the pipeline through `kubectl`.
LOGS="$(k8s_job_logs "$STAGE_JOB")" || die "could not read job/$STAGE_JOB's log; try: kubectl logs job/$STAGE_JOB"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' <<<"$LOGS")"
[[ -n $RUN_ID ]] || die "job/$STAGE_JOB printed no run_id line; read its log with: kubectl logs job/$STAGE_JOB"

RUN_DIR="$RUNS_DIR/$RUN_ID"
# What the engine's two documents named their objects, which the polling and
# the tailing below address.
RUN_OBJECT="$(k8s_object_name "$RUN_ID")"
mkdir -p "$RUN_DIR"
log "fetching the run directory into $RUN_DIR"
aws s3 sync "$RUNS_ROOT/$RUN_ID/stage/" "$RUN_DIR/" >&2 ||
	die "could not fetch $RUNS_ROOT/$RUN_ID/stage/; job/$STAGE_JOB published it, so check your own credentials"
[[ -f $RUN_DIR/facts.json ]] || die "$RUNS_ROOT/$RUN_ID/stage/ holds no facts.json"

k8s_delete job "$STAGE_JOB"
k8s_delete configmap "$SPEC_CONFIGMAP"
k8s_delete configmap "$SITE_CONFIGMAP"

# ---------------------------------------------------------------------------
# The engine
# ---------------------------------------------------------------------------

if [[ $ENGINE == external ]]; then
	# On stderr, because this script's stdout is the run id. The same document
	# is in the run directory, which the line below names.
	log "start your engine against these facts, in $RUN_DIR/facts.json:"
	cat "$RUN_DIR/facts.json" >&2
	log "then: scripts/launch.sh $RUN_ID"
else
	# The ConfigMap first: the deployment mounts it, and a JobManager scheduled
	# before it exists waits on a volume rather than starting.
	log "starting the engine"
	k8s_apply_file "$RUN_DIR/flink-job-configmap.yaml"
	k8s_apply_file "$RUN_DIR/flinkdeployment.yaml"

	# The deployment is named after the run, so this also confirms that the job
	# reaching RUNNING is the one just applied.
	log "waiting up to ${ENGINE_RUNNING_WAIT_S}s for flinkdeployment/$RUN_OBJECT to reach RUNNING"
	waited=0
	while :; do
		state="$(k8s_flinkdeployment_state "$RUN_OBJECT")"
		case "$state" in
		RUNNING)
			log "flinkdeployment/$RUN_OBJECT is RUNNING"
			break
			;;
		FAILED | CANCELED | FINISHED)
			k8s_deployment_tail "$RUN_OBJECT"
			die "flinkdeployment/$RUN_OBJECT went to $state before it ran; the lines above are the jobmanager's own log"
			;;
		esac
		if ((waited >= ENGINE_RUNNING_WAIT_S)); then
			k8s_deployment_tail "$RUN_OBJECT"
			die "flinkdeployment/$RUN_OBJECT did not reach RUNNING within ${ENGINE_RUNNING_WAIT_S}s (last state: ${state:-none reported})"
		fi
		sleep "$ENGINE_POLL_S"
		waited=$((waited + ENGINE_POLL_S))
	done
fi

printf 'run_id: %s\n' "$RUN_ID"
