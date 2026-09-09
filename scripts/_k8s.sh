# Shared shell for the cluster drivers: read the site, render a manifest, apply
# it, wait for the Job it made, and delete what a re-run would collide with.
#
# Sourced after `_lib.sh`, never executed: `log`, `die` and `REPO_ROOT` come
# from there.
#
# Every function reads `KUBE_CONTEXT` and `SITE_NAMESPACE` from the environment,
# which a driver sets from the site config before it calls one. Passing them
# rather than re-reading the file per call is what keeps a driver from applying
# a manifest to one cluster and waiting on another.
#
# Nothing here writes to stdout except `k8s_job_logs`, `harness_local` and the
# `site_*` readers, whose output is their answer. A driver's stdout is its
# result — a corpus URI, a run id — and `kubectl`'s own narration would be read
# as part of it.

# Where the site config is. A driver's `--site` overwrites this before the first
# call, and every reader below takes it at call time rather than at source time.
SITE_FILE="${SITE_FILE:-./site.yaml}"

# Where a run directory is fetched to, beside the operator's own site config.
# The bucket side of the same thing is `site.runs_root`, and the two are not
# interchangeable: this one holds what an operator reads, that one what the
# pods write.
RUNS_DIR="${RUNS_DIR:-./runs}"

# The registry path both images are pushed under, and the one a Job's image
# reference is built from.
IMAGE_REPOSITORY_PREFIX=lakehouse-ingest-bench

# How often a Job's conditions are read while waiting on it. Ten seconds is
# below the resolution of anything worth waiting for here and costs one API
# request; the wait itself is hours long for a corpus.
K8S_JOB_POLL_S="${K8S_JOB_POLL_S:-10}"

# ---------------------------------------------------------------------------
# Reading the site
# ---------------------------------------------------------------------------

# One value, empty for a key the file leaves out. `yq` prints the string `null`
# for a missing key, which would otherwise reach a manifest as a hostname.
site_value() {
	local value
	value="$(yq "$1" "$SITE_FILE")"
	if [[ $value == null ]]; then
		value=""
	fi
	printf '%s' "$value"
}

site_required() {
	local value
	value="$(site_value "$1")"
	[[ -n $value ]] || die "$SITE_FILE sets no ${1#.}; copy site.aws.example.yaml for the whole shape of it"
	printf '%s' "$value"
}

# Refuse a missing site config by name. A driver's first read would otherwise
# report a missing key in a file that is not there.
require_site_file() {
	[[ -f $SITE_FILE ]] || die "no site config at $SITE_FILE; copy site.aws.example.yaml and fill it in"
}

# A map or a list, as the one-line JSON the manifests take. JSON is valid YAML
# flow style, so a marker rendered with this is a document `kubectl` accepts
# without the renderer having to know how deep in the manifest it landed.
site_json() {
	yq -o=json -I=0 "$1 // $2" "$SITE_FILE"
}

# One `key=value` per line, for a map of client properties the site declares.
# A map the site leaves out yields nothing, which is the right answer for a
# broker that needs no properties.
#
# Read into a variable first so a `yq` that could not answer is a refusal here:
# in a loop over its output, a failure would read as an empty map instead, and
# the client would be launched with none of the site's properties.
site_pairs() {
	local entries
	entries="$(yq "$1 // {} | to_entries | .[] | .key + \"=\" + .value" "$SITE_FILE")" ||
		die "could not read ${1#.} out of $SITE_FILE; every value under it must be a quoted string"
	printf '%s' "$entries"
}

# site_flags <yq path to a map> <flag> — ` <flag> key=value` per entry, on stdout.
#
# For a command line that reaches a Job as one string the image's shell splits,
# so a value carrying whitespace would arrive as two arguments. Refused here
# rather than reaching a client as a truncated property.
site_flags() {
	# Assigned before the loop reads it, and checked explicitly rather than
	# through `set -e`. Two reasons, and both end the same way — an empty string
	# returned as success, so a Job is launched with none of the site's
	# properties. A `die` inside the loop's own redirection would end only that
	# redirection's subshell; and bash suspends `-e` inside a command
	# substitution, which is where this function itself runs, so a failure would
	# fall through to the `printf` below.
	local pairs
	pairs="$(site_pairs "$1")" ||
		die "cannot build the $2 flags a Job's command line needs; the line above says why"
	local flags="" pair
	while IFS= read -r pair; do
		[[ -n $pair ]] || continue
		[[ $pair != *[[:space:]]* ]] ||
			die "$SITE_FILE sets ${1#.} entry '$pair', and whitespace in it cannot survive a Job's command line"
		flags="$flags $2 $pair"
	done <<<"$pairs"
	printf '%s' "$flags"
}

# The env list every Job gets, which is the region or nothing. A cluster off AWS
# names none, and a region rendered empty reaches an SDK as one it cannot
# resolve — a signing failure far from the file that caused it.
#
# Under both names, because the SDKs disagree about which one is a client's
# region. Java's reads `AWS_REGION`; botocore reads `AWS_DEFAULT_REGION` alone
# and consults `AWS_REGION` only as a hint for its smart-defaults mode, so a
# pod given only that name has a client with no region at all — which resolves
# S3's global endpoint and is refused for a bucket that lives anywhere else.
site_env_json() {
	local region
	region="$(site_value '.kubernetes.aws_region')"
	if [[ -z $region ]]; then
		printf '[]'
	else
		printf '[{"name":"AWS_REGION","value":"%s"},{"name":"AWS_DEFAULT_REGION","value":"%s"}]' "$region" "$region"
	fi
}

# Everything a Job-launching driver reads out of the site, as the globals the
# render calls below take. One reader rather than one per driver: a driver that
# read a different subset could apply a Job to one cluster and wait on another.
k8s_read_site() {
	require_site_file
	SITE_NAMESPACE="$(site_required '.kubernetes.namespace')"
	KUBE_CONTEXT="$(site_required '.kubernetes.context')"
	SERVICE_ACCOUNT="$(site_required '.kubernetes.harness_service_account')"
	REGISTRY="$(site_required '.kubernetes.registry')"
	RUNS_ROOT="$(site_required '.runs_root')"
	NODE_SELECTOR="$(site_json '.kubernetes.node_selector' '{}')"
	TOLERATIONS="$(site_json '.kubernetes.tolerations' '[]')"
	JOB_ENV="$(site_env_json)"
}

# The tag a driver's Jobs run at: the caller's, or this checkout's commit —
# which is the rule `push-images.sh` tags with, so a driver run from the tree
# that was pushed needs no argument.
k8s_image_tag() {
	local given=${1:-}
	if [[ -n $given ]]; then
		printf '%s' "$given"
		return 0
	fi
	git -C "$REPO_ROOT" rev-parse --short HEAD ||
		die "could not read this checkout's commit to tag the image with; pass --image-tag"
}

# ---------------------------------------------------------------------------
# The cluster
# ---------------------------------------------------------------------------

# One of this harness's own commands, from an installed harness or from the
# checkout through `uv`. A driver runs on an operator's machine, where this
# repository is a clone at least as often as it is an installed package.
#
# Its stdout is the command's own, because callers parse it. Every path handed
# to one of these must be absolute: the checkout fallback runs from the
# repository root and not from the operator's working directory.
#
# A leading `--extra <name>`, repeatable, reaches the checkout fallback's `uv
# run`. A command that opens a catalog needs the `aws` extra, because pyiceberg
# imports boto3 only when it comes to sign a Glue request and a plain `uv sync`
# installs no cloud SDK. The installed-harness path takes no extras — an
# installed harness carries whatever it was installed with, which is why
# docs/running.md says to install it with that extra.
harness_local() {
	local extras=()
	while [[ ${1:-} == --extra ]]; do
		extras+=(--extra "${2:?--extra needs the name of an optional dependency group}")
		shift 2
	done
	local name=$1
	shift
	if command -v "$name" >/dev/null 2>&1; then
		"$name" "$@"
	elif command -v uv >/dev/null 2>&1; then
		(cd -- "$REPO_ROOT" && uv run --frozen ${extras[@]+"${extras[@]}"} "$name" "$@")
	else
		die "neither $name nor uv is on PATH; install this harness or install uv — see $PREREQ_DOC"
	fi
}

# k8s_render_apply <template relative to the repository root> NAME=VALUE...
#
# `--namespace` as well as the manifest's own: `kubectl` refuses a document
# whose namespace differs from the flag, which is the check that the Job being
# applied was rendered for the site this driver read.
#
# A Job's spec is immutable, so a driver deletes the previous Job of the same
# name — `k8s_delete job <name>` — before it applies a new one.
k8s_render_apply() {
	local template=$1
	shift
	local set_args=() pair
	for pair in "$@"; do
		set_args+=(--set "$pair")
	done
	# `${a[@]+"${a[@]}"}` here and below, because an empty array expanded plainly
	# is an unbound-variable error under `set -u` on bash 3.2 — which is what
	# `/bin/bash` still is on macOS.
	harness_local render-k8s "$REPO_ROOT/$template" ${set_args[@]+"${set_args[@]}"} |
		kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" apply -f - >&2
}

# A whole manifest a run already carries, applied and deleted by the file that
# holds it. The names inside a run's rendered documents belong to the engine
# that rendered them, so a driver that restated them would drift from the
# renderer the first time one of them changed.
k8s_apply_file() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" apply -f "$1" >&2
}

k8s_delete_file() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" delete -f "$1" --ignore-not-found >&2
}

# k8s_configmap_from_file <name> <key>=<path>...
#
# Rendered client-side and applied rather than created, so a re-run after a
# failure converges instead of refusing a name that is already there.
k8s_configmap_from_file() {
	local name=$1
	shift
	local from=() pair
	for pair in "$@"; do
		from+=(--from-file "$pair")
	done
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" create configmap "$name" \
		${from[@]+"${from[@]}"} --dry-run=client -o yaml |
		kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" apply -f - >&2
}

k8s_job_tail() {
	log "--- last 40 lines of job/$1 ---"
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "job/$1" --tail=40 >&2 || true
}

# Wait for a Job to reach one of its two ends, and print its own log unless it
# was the good one.
#
# Both conditions are read, rather than waiting on `condition=complete` alone: a
# failed Job never gains that condition, so a single wait would spend the whole
# timeout — hours, for a generation — to report a failure the Job announced in
# seconds. Size the timeout to the command, not to the wait.
k8s_wait_job() {
	local name=$1 timeout_s=$2 waited=0 conditions=""
	log "waiting up to ${timeout_s}s for job/$name"
	while :; do
		# Only the conditions the API says are true, one type per line, so a
		# `Failed: False` cannot be read as a failure.
		conditions="$(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get "job/$name" \
			-o 'jsonpath={range .status.conditions[?(@.status=="True")]}{.type}{"\n"}{end}')" ||
			die "could not read job/$name; try: kubectl get job/$name"
		case "$conditions" in
		*Failed*)
			k8s_job_tail "$name"
			die "job/$name failed; the lines above are its own log"
			;;
		*Complete*)
			log "job/$name completed"
			return 0
			;;
		esac
		((waited < timeout_s)) || break
		sleep "$K8S_JOB_POLL_S"
		waited=$((waited + K8S_JOB_POLL_S))
	done
	k8s_job_tail "$name"
	die "job/$name did not complete within ${timeout_s}s; the lines above are its own log"
}

# One Job's log, on stdout, because a driver parses it: the harness commands
# report the URI they wrote, and that report is more trustworthy than a second
# guess at it from outside.
k8s_job_logs() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "job/$1"
}

k8s_delete() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" delete "$1" "$2" --ignore-not-found >&2
}

k8s_deployment_tail() {
	log "--- last 40 lines of deploy/$1 ---"
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "deploy/$1" --tail=40 >&2 || true
}

# The state the Flink operator reports for a run's job, empty until it reports
# one. A missing object is the normal first answer — the operator creates it
# seconds after the apply — so a failed read is empty rather than fatal, and it
# is the caller's timeout that turns a state which never arrives into a refusal
# naming the object.
k8s_flinkdeployment_state() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get "flinkdeployment/$1" \
		-o 'jsonpath={.status.jobStatus.state}' 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# A run's object names
# ---------------------------------------------------------------------------

# The two Jobs `launch.sh` creates and `teardown.sh` deletes, named in one place
# so a rename cannot leave a producer fleet running after a teardown. The
# engine's own objects are named by the documents that render them, and are
# deleted through those documents rather than by a name restated here.
producer_job() {
	printf 'producer-%s' "$1"
}

scorer_job() {
	printf 'scorer-%s' "$1"
}
