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
# Nothing here writes to stdout except `k8s_job_logs`. A driver's stdout is its
# result — a corpus URI, an image reference — and `kubectl`'s own narration
# would be read as part of it.

# Where the site config is. A driver's `--site` overwrites this before the first
# call, and every reader below takes it at call time rather than at source time.
SITE_FILE="${SITE_FILE:-./site.yaml}"

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

# A map or a list, as the one-line JSON the manifests take. JSON is valid YAML
# flow style, so a marker rendered with this is a document `kubectl` accepts
# without the renderer having to know how deep in the manifest it landed.
site_json() {
	yq -o=json -I=0 "$1 // $2" "$SITE_FILE"
}

# The env list every Job gets, which is the region or nothing. A cluster off AWS
# names none, and an `AWS_REGION` rendered empty reaches an SDK as a region it
# cannot resolve — a signing failure far from the file that caused it.
site_env_json() {
	local region
	region="$(site_value '.kubernetes.aws_region')"
	if [[ -z $region ]]; then
		printf '[]'
	else
		printf '[{"name":"AWS_REGION","value":"%s"}]' "$region"
	fi
}

# ---------------------------------------------------------------------------
# The cluster
# ---------------------------------------------------------------------------

# `render-k8s` from an installed harness, or the checkout's own through `uv`. A
# driver runs on an operator's machine, where this repository is a clone at
# least as often as it is an installed package.
_render_k8s() {
	if command -v render-k8s >/dev/null 2>&1; then
		render-k8s "$@"
	elif command -v uv >/dev/null 2>&1; then
		(cd -- "$REPO_ROOT" && uv run --frozen render-k8s "$@")
	else
		die "neither render-k8s nor uv is on PATH; install this harness or install uv — see $PREREQ_DOC"
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
	_render_k8s "$REPO_ROOT/$template" "${set_args[@]}" |
		kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" apply -f - >&2
}

_k8s_job_tail() {
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
			_k8s_job_tail "$name"
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
	_k8s_job_tail "$name"
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
