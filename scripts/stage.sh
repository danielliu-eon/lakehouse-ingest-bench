#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Stage a run on the cluster, and for a managed engine start it.
#
# Staging runs as a Job because it creates the topic, and a managed broker is
# reachable from inside its own network rather than from an operator's machine.
# The Job publishes its run directory to the runs prefix; everything after that
# — fetching the directory, applying the engine's two documents, waiting for it
# to run — is `kubectl` and `aws` work, and stays here.
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
# How long the engine may take to reach its running state. Long: the operator
# has to schedule the whole fleet, each pod of which pulls an image of a few
# hundred megabytes onto a node that may not exist yet.
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

require_host_tools kubectl aws yq jq git curl
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

# The two ConfigMaps belong to this Job alone, and one of them is the
# operator's own site config — so every exit takes them with it rather than
# only the one that reaches the deletion below. A failure inside the trap is
# tolerated: it must not become this script's exit status, which is the
# refusal that caused the exit.
delete_stage_configmaps() {
	k8s_delete configmap "$SPEC_CONFIGMAP" || true
	k8s_delete configmap "$SITE_CONFIGMAP" || true
}

log "staging $SPEC as job/$STAGE_JOB with $IMAGE"
# A Job's spec is immutable, so the previous Job of this name goes first.
k8s_delete job "$STAGE_JOB"
trap delete_stage_configmaps EXIT
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
	"ENV_FROM=$JOB_ENV_FROM" \
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
delete_stage_configmaps
trap - EXIT

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
	# Every name below — the kind of object a run is, where its state sits, the
	# Service that carries its API — comes from the engine's own module, so a
	# third engine adds no line to this script.
	k8s_read_engine "$ENGINE" "$RUN_OBJECT"

	# The ConfigMap first: the engine's pods mount it, and one scheduled before
	# it exists waits on a volume rather than starting.
	log "starting the engine"
	k8s_apply_file "$RUN_DIR/$ENGINE_CONFIGMAP_FILE"
	k8s_apply_file "$RUN_DIR/$ENGINE_DOCUMENT_FILE"

	# The object is named after the run, so this also confirms that what
	# reaches the running state is what was just applied.
	log "waiting up to ${ENGINE_RUNNING_WAIT_S}s for $ENGINE_KIND/$RUN_OBJECT to reach $ENGINE_RUNNING_STATE"
	waited=0
	while :; do
		state="$(k8s_engine_state "$ENGINE_KIND" "$RUN_OBJECT" "$ENGINE_STATE_JSONPATH")"
		if [[ $state == "$ENGINE_RUNNING_STATE" ]]; then
			log "$ENGINE_KIND/$RUN_OBJECT is $state"
			break
		fi
		# Comma-delimited on both sides of the match, so a state whose name is
		# another's prefix cannot pass for it.
		if [[ -n $state && ",$ENGINE_FAILED_STATES," == *",$state,"* ]]; then
			k8s_engine_tail "$ENGINE_LOG_TARGET"
			die "$ENGINE_KIND/$RUN_OBJECT went to $state before it ran; the lines above are the engine's own log"
		fi
		if ((waited >= ENGINE_RUNNING_WAIT_S)); then
			k8s_engine_tail "$ENGINE_LOG_TARGET"
			die "$ENGINE_KIND/$RUN_OBJECT did not reach $ENGINE_RUNNING_STATE within ${ENGINE_RUNNING_WAIT_S}s (last state: ${state:-none reported})"
		fi
		sleep "$ENGINE_POLL_S"
		waited=$((waited + ENGINE_POLL_S))
	done

	# A running state says the operator started something, not that what it
	# started is the run this spec asked for: an engine drops a configuration
	# key it does not know, a connector ignores a hint it does not implement,
	# and each half of a fleet is sized by whatever configuration reached it —
	# none of which fails a submission. So the settings a result would be
	# attributed to are read back off the engine before the run is ever offered
	# a corpus.
	#
	# A high local port for the tunnel, so it cannot collide with an engine an
	# operator is already running on this machine.
	VERIFY_PORT=18081
	# What a check exits with having found drift, as against having been unable
	# to read the endpoint — which is worth another look, since a tunnel and an
	# engine can both be a moment behind the state that reported it running.
	VERIFY_DRIFT_STATUS=3
	VERIFY_PENDING_STATUS=4
	VERIFY_TRIES=3
	# Absolute, because `harness_local`'s checkout fallback runs from the
	# repository root and not from the operator's working directory.
	VERIFY_SPEC="$(cd -- "$RUN_DIR" && pwd)/spec.yaml" || die "could not resolve $RUN_DIR to check the engine against"
	[[ -f $VERIFY_SPEC ]] ||
		die "$RUNS_ROOT/$RUN_ID/stage/ holds no spec.yaml, so the engine has nothing to be checked against"
	# A reading and not an artifact, so it goes to a temporary file the trap
	# below removes along with the tunnel. Made only for an engine whose check
	# reads the fleet's shape: one that names no selector never has a file to
	# be handed.
	VERIFY_PODS=""
	if [[ -n $ENGINE_PODS_SELECTOR ]]; then
		VERIFY_PODS="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-pods.XXXXXX")" ||
			die "could not make a temporary file to read the run's pods into"
	fi

	# Trapped before the tunnel is opened, so no path out of the readings below
	# — `die` included — leaves one behind.
	trap 'k8s_port_forward_stop; [[ -z $VERIFY_PODS ]] || rm -f "$VERIFY_PODS"' EXIT
	k8s_port_forward "svc/$RUN_OBJECT$ENGINE_REST_SERVICE_SUFFIX" "$VERIFY_PORT:$ENGINE_REST_PORT"

	log "checking $ENGINE_KIND/$RUN_OBJECT against $VERIFY_SPEC"
	verify_tries=0
	placement_waited=0
	while :; do
		verify_args=(--spec "$VERIFY_SPEC" --run-id "$RUN_ID" --rest "http://localhost:$VERIFY_PORT")
		# Re-read on every try, because an executor still being scheduled is
		# one of the things a retry is waiting for. An engine that names no
		# selector is one whose check reads nothing off the pods.
		if [[ -n $ENGINE_PODS_SELECTOR ]]; then
			k8s_write_pods "$VERIFY_PODS" "$ENGINE_PODS_SELECTOR"
			verify_args+=(--pods "$VERIFY_PODS")
		fi
		# On stderr: this script's stdout is the run id, and the drift lines are
		# for the operator reading the refusal below.
		verify_status=0
		harness_local "verify-$ENGINE" "${verify_args[@]}" >&2 || verify_status=$?
		if ((verify_status == 0)); then
			break
		fi
		if ((verify_status == VERIFY_DRIFT_STATUS)); then
			die "$ENGINE_KIND/$RUN_OBJECT is not running what $SPEC asked for; the lines above name every setting it dropped. It is left running, so the engine can be read before it is torn down"
		fi
		# A fleet still being placed gets the engine's own running wait, not
		# the endpoint's tries: the object was RUNNING before its last pod
		# had an image to start from.
		if ((verify_status == VERIFY_PENDING_STATUS)); then
			placement_waited=$((placement_waited + ENGINE_POLL_S))
			if ((placement_waited > ENGINE_RUNNING_WAIT_S)); then
				die "$ENGINE_KIND/$RUN_OBJECT's fleet was not fully placed within ${ENGINE_RUNNING_WAIT_S}s; the lines above name what is still missing. It is left running, so the pods can be read before it is torn down"
			fi
			sleep "$ENGINE_POLL_S"
			continue
		fi
		verify_tries=$((verify_tries + 1))
		if ((verify_tries >= VERIFY_TRIES)); then
			die "could not read $ENGINE_KIND/$RUN_OBJECT's own endpoint in $VERIFY_TRIES tries (last exit $verify_status), so nothing this run measures could be attributed to the spec it was staged from"
		fi
		sleep "$ENGINE_POLL_S"
	done
	k8s_port_forward_stop

	# Recorded here because this is the last moment the fleet is certain to
	# exist: a teardown reads the same thing, but only as a fallback, and by
	# then the pods it would read are the ones it has just deleted.
	k8s_write_engine_image "$RUN_DIR/$ENGINE_IMAGE_FILE" "$ENGINE_PROVENANCE_SELECTOR"
fi

printf 'run_id: %s\n' "$RUN_ID"
