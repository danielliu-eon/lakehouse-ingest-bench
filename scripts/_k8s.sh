# SPDX-License-Identifier: Apache-2.0
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

# How long a port-forward may take to carry a request. Seconds of work — the
# pod it forwards to is already running by the time one is opened — so a
# tunnel still silent after this is one that is not going to answer.
K8S_PORT_FORWARD_WAIT_S="${K8S_PORT_FORWARD_WAIT_S:-30}"

# Where a run's engine provenance is written, and how long a pod that has not
# reported its image digest yet is waited for. A digest appears once the kubelet
# has pulled the image, which is already true of a pod whose job is RUNNING, so
# the wait is only for the gap between the two reports.
ENGINE_IMAGE_FILE=engine-image.json
ENGINE_IMAGE_WAIT_S="${ENGINE_IMAGE_WAIT_S:-60}"
ENGINE_IMAGE_POLL_S="${ENGINE_IMAGE_POLL_S:-5}"

# The copy of the table's last metadata document, named in one place because
# three drivers address it: a teardown writes it, `file-sizes` reads the
# geometry out of it, and a purge reads the table's location out of it. Guessing
# that location from the table's name is what this file exists to avoid.
#
# Always JSON, whatever the table wrote — see `k8s_fetch_metadata_document`.
METADATA_FINAL_FILE=table-metadata.final.json

# ---------------------------------------------------------------------------
# The table's metadata document
# ---------------------------------------------------------------------------

# What `table-metadata` exits when the catalog holds no such table, as
# ingest_bench.table.cli.TABLE_ABSENT. Any other non-zero exit is a catalog the
# caller could not reach, which is a failure and not an absent table — the two
# drivers that ask have to tell them apart.
TABLE_ABSENT=3

# k8s_fetch_metadata_document <metadata uri> <local path>
#
# The table's current metadata document, stored as the JSON its readers parse.
#
# Iceberg's metadata document may be compressed — the table property
# `write.metadata.compression-codec` makes it gzip, conventionally named
# `*.gz.metadata.json` — and which of the two a run ends up with is the
# writer's choice rather than anything this harness sets. Both readers of the
# stored copy parse it with `jq`, so a gzip body reaches them as a syntax error
# against a table that nothing can then reclaim: decompressing here is what
# makes the file's format one thing rather than the writer's.
#
# The first two bytes decide and not the name, because the codec is the
# property and the suffix is only the convention that usually follows it.
k8s_fetch_metadata_document() {
	local source=$1 path=$2
	aws s3 cp "$source" "$path" >&2 || return 1
	local magic=""
	magic="$(od -An -tx1 -N2 -- "$path" | tr -d ' \n')" ||
		die "could not read the first bytes of $path to tell whether it is compressed"
	[[ $magic == 1f8b ]] || return 0
	log "$source is gzip-compressed; storing it decompressed"
	# Beside it and then moved, so a decompression that fails leaves the
	# document that was fetched rather than a truncated one.
	if ! gzip -dc -- "$path" >"$path.plain"; then
		rm -f "$path.plain"
		log "could not decompress $path, so it is stored as it was fetched"
		return 1
	fi
	mv "$path.plain" "$path"
}

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

# The one scheme the drivers can reach, named so the check below reads as a
# scheme comparison rather than as a glob.
S3_SCHEME="s3://"

# site_root <yq path to a root> — one of the site's storage roots, refused where
# these drivers cannot reach it.
#
# The corpus generator, both engine renderers and the harness's own storage
# layer all serve `gs://`; the drivers do not. Every one of them fetches a run
# directory, an artifact or a listing by shelling out to the `aws` CLI, so a
# GCS site would get a working corpus and a driver layer that cannot read it.
# The cloud path is AWS-only today, and the refusal says so rather than
# surfacing as an `aws s3` error about a URI it could not parse.
site_root() {
	local root
	root="$(site_required "$1")"
	[[ $root == "$S3_SCHEME"* ]] ||
		die "${1#.} is '$root', and these drivers reach storage through the aws CLI: the cloud path is AWS-only today, so every root has to be an s3:// URI"
	printf '%s' "$root"
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

# site_flags <yq path to a map> <flag> — ` <flag> 'key=value'` per entry, on stdout.
#
# For a command line that reaches a Job as one string the image's shell splits.
# Each pair is single-quoted, which is what makes it opaque to that shell:
# whitespace arrives as one argument, and `${env:NAME}` arrives as the six
# characters the site wrote rather than as a substitution. The second is the
# point — a credential is a reference resolved by the Python process that uses
# the property, and a shell that expanded or rejected the form first would
# leave that process nothing to resolve.
#
# The one thing a single-quoted value cannot carry is a single quote, so that is
# refused by name — as is a double quote, which would survive the shell and then
# close the quoted scalar the whole command line is rendered into, leaving
# `kubectl apply` reporting a parse error rather than the value that caused it.
site_flags() {
	# Assigned before the loop reads it, and checked explicitly: bash suspends
	# `-e` inside a command substitution, which is where this function runs,
	# and a `die` inside the loop's own redirection would end only that
	# subshell. Either way an empty string would be returned as success — see
	# `site_pairs`.
	local pairs
	pairs="$(site_pairs "$1")" ||
		die "cannot build the $2 flags a Job's command line needs; the line above says why"
	local flags="" pair
	while IFS= read -r pair; do
		[[ -n $pair ]] || continue
		[[ $pair != *"'"* && $pair != *'"'* ]] ||
			die "$SITE_FILE sets a ${1#.} entry holding a quote, which cannot survive a Job's command line"
		flags="$flags $2 '$pair'"
	done <<<"$pairs"
	printf '%s' "$flags"
}

# The catalog properties as `--catalog-prop key=value` argument pairs, in the
# global array `CATALOG_PROP_FLAGS`, for a harness command run on this machine.
#
# Set rather than printed, and an array rather than a string, because a caller
# splitting one string back apart would split a value on its own spaces too.
# `site_flags` is the string form and is for a Job's command line, where the
# image's shell does that splitting on purpose.
#
# Assigned and checked explicitly, for the reason `site_pairs` gives.
read_catalog_prop_flags() {
	local pairs
	pairs="$(site_pairs '.catalog.props')" ||
		die "cannot build the catalog flags a harness command needs; the line above says why"
	CATALOG_PROP_FLAGS=()
	local pair
	while IFS= read -r pair; do
		[[ -n $pair ]] || continue
		CATALOG_PROP_FLAGS+=(--catalog-prop "$pair")
	done <<<"$pairs"
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

# The Secret every Job's pod reads its environment from, as the `envFrom` list
# the manifests take, or nothing where the site names none.
#
# One Secret for the whole site rather than a key per property: what a
# `${env:NAME}` in the site config names is a variable, and a Secret's keys are
# exactly a set of variable names. So the operator creates one Secret and the
# harness carries no statement of which properties have credentials in them.
site_env_from_json() {
	local secret
	secret="$(site_value '.kubernetes.secret_name')"
	if [[ -z $secret ]]; then
		printf '[]'
	else
		printf '[{"secretRef":{"name":"%s"}}]' "$secret"
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
	RUNS_ROOT="$(site_root '.runs_root')"
	NODE_SELECTOR="$(site_json '.kubernetes.node_selector' '{}')"
	TOLERATIONS="$(site_json '.kubernetes.tolerations' '[]')"
	JOB_ENV="$(site_env_json)"
	JOB_ENV_FROM="$(site_env_from_json)"
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
# to one of these must be absolute — `abs_path` below is what a caller turns
# its own relative paths into, and states why.
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

# Whether one of this harness's commands is available to `harness_local`, which
# is not the same question as whether it is on `PATH`: in a checkout it is `uv`
# that provides the command, and the only statement of which commands there are
# is this project's own script table.
#
# For a driver that offers a step some checkouts cannot take yet. It answers
# only whether the command exists — a command that exists and fails is a
# failure to report, and running one to find out would hide that.
harness_available() {
	if command -v "$1" >/dev/null 2>&1; then
		return 0
	fi
	if command -v uv >/dev/null 2>&1 && grep -q "^$1 = " "$REPO_ROOT/pyproject.toml"; then
		return 0
	fi
	return 1
}

# One path, absolute. `harness_local`'s checkout fallback runs from the
# repository root, so a relative path handed to a harness command there names a
# file in this repository rather than in the operator's working directory.
#
# The directory is expected to exist: every caller has already refused a run
# directory or a site config it could not find.
abs_path() {
	printf '%s/%s' "$(cd -- "$(dirname -- "$1")" && pwd)" "$(basename -- "$1")"
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

# k8s_object_present <kind> <name> — the object's own name on stdout, empty when
# the namespace does not hold it.
#
# A read the API refused is a refusal rather than an empty answer, because the
# caller of this is asking whether something is still running before it destroys
# what that thing is writing: "I could not ask" and "it is not there" have to be
# different answers. Callers assign first and check the status, since a `die`
# inside a command substitution ends only that substitution's subshell.
k8s_object_present() {
	local name
	name="$(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get "$1" "$2" \
		--ignore-not-found -o 'jsonpath={.metadata.name}')" ||
		die "could not read $1/$2 out of $SITE_NAMESPACE; try: kubectl get $1/$2"
	printf '%s' "$name"
}

# k8s_write_engine_image <path> <label selector> [wait seconds]
#
# What the engine actually ran, as `{"image": …, "digest": …}`. The digest is
# the pod's own `imageID`, which names the manifest the node pulled rather than
# the tag it was pulled under: a floating tag repointed after a run would
# otherwise leave a result claiming an image that is no longer the one measured.
#
# A pod that has not reported an `imageID` yet is waited for and then written
# with a null digest. Provenance is not worth failing a run over, and `collect`
# records the absence rather than inventing a digest.
#
# `{range}` over the pods rather than `.items[0]`, because an index into an
# empty list is an error in some `kubectl` versions and empty in others, and no
# pod matching the selector is the normal answer for an engine already deleted.
k8s_write_engine_image() {
	local path=$1 selector=$2 wait_s=${3:-$ENGINE_IMAGE_WAIT_S}
	local waited=0 answer="" line="" image="" digest=""
	while :; do
		answer="$(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pod -l "$selector" \
			-o 'jsonpath={range .items[*]}{.spec.containers[0].image}{" "}{.status.containerStatuses[0].imageID}{"\n"}{end}' \
			2>/dev/null)" || answer=""
		line="${answer%%$'\n'*}"
		image="${line%% *}"
		digest="${line##* }"
		[[ -z $image || -z $digest ]] || break
		((waited < wait_s)) || break
		sleep "$ENGINE_IMAGE_POLL_S"
		waited=$((waited + ENGINE_IMAGE_POLL_S))
	done
	if [[ -z $image ]]; then
		log "no pod matching '$selector' names an image, so $path is not written"
		return 0
	fi
	[[ -n $digest ]] || log "no pod matching '$selector' reported an image digest, so $path records none"
	# Reported and not fatal, like every other absence here. This runs from
	# staging, after the engine is RUNNING and before the run id is printed, so
	# a refusal would cost the caller the id of a run that had already started —
	# a fleet nothing could then address, over a provenance field.
	if jq -n --arg image "$image" --arg digest "$digest" \
		'{image: $image, digest: (if $digest == "" then null else $digest end)}' >"$path"; then
		log "the engine ran $image (digest ${digest:-none reported})"
	else
		log "could not write $path, so this run records no engine image"
	fi
}

# k8s_engine_tail <log target> — the last of an engine's own output, on stderr.
#
# The target is whatever the engine's descriptor named: a Deployment for one
# whose operator raises a fleet behind it, a pod for one whose driver is a pod.
k8s_engine_tail() {
	log "--- last 40 lines of $1 ---"
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "$1" --tail=40 >&2 || true
}

# k8s_pods_present <label selector> — the first matching pod's name, empty when
# the namespace holds none.
#
# For a caller deciding whether there is an engine log to read before it
# refuses. A failed read is empty rather than fatal: the caller is on its way to
# a refusal either way, and not being able to ask costs it only the tail.
k8s_pods_present() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pod -l "$1" \
		-o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | head -n 1 || true
}

# k8s_engine_field <kind> <name> <jsonpath> — one field of what an operator
# reports about a run, empty until it reports one.
#
# The state and the error text are both read through this, because "not yet"
# and "never" are the same empty answer for either of them. A missing object is
# the normal first answer — the operator creates it seconds after the apply —
# so a failed read is empty rather than fatal, and it is the caller that turns
# an answer which never arrives into a refusal naming the object.
k8s_engine_field() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get "$1/$2" \
		-o "jsonpath=$3" 2>/dev/null || true
}

# k8s_write_pods <path> <label selector> — the run's pods as one JSON document.
#
# For an engine whose check reads the fleet's shape rather than only what the
# engine reports about itself: how many pods there are, and whether they hold
# their cores or borrow them, is visible nowhere else. A file and not a pipe,
# because the check is a separate process whose refusal names where the
# document came from.
k8s_write_pods() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pods -l "$2" -o json >"$1" ||
		die "could not read the pods matching '$2' in $SITE_NAMESPACE; try: kubectl get pods -l '$2'"
}

# ---------------------------------------------------------------------------
# A tunnel to one service
# ---------------------------------------------------------------------------

# The pid of the tunnel `k8s_port_forward` opened, in a global rather than
# returned: the caller stops it from an `EXIT` trap, and a pid printed by a
# function that runs in a command substitution would not reach one.
K8S_PORT_FORWARD_PID=""

# k8s_port_forward <resource> <local:remote>
#
# For the harness commands that speak a cluster-internal HTTP API from an
# operator's machine. The wait is a request through the tunnel and not the
# process being up: `kubectl port-forward` opens its listener before the
# connection behind it works, so a command started on the listener alone is
# answered with a reset.
k8s_port_forward() {
	local resource=$1 ports=$2 local_port=${2%%:*} waited=0
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" port-forward "$resource" "$ports" >&2 &
	K8S_PORT_FORWARD_PID=$!
	while ((waited < K8S_PORT_FORWARD_WAIT_S)); do
		if curl -sf --max-time 5 -o /dev/null "http://localhost:$local_port/"; then
			log "port-forward to $resource answers on localhost:$local_port"
			return 0
		fi
		# A tunnel whose own process is gone will never answer, and the
		# refusal that says so is more use than the timeout that follows it.
		if ! kill -0 "$K8S_PORT_FORWARD_PID" 2>/dev/null; then
			die "kubectl port-forward $resource $ports exited; the lines above are its own output"
		fi
		sleep 2
		waited=$((waited + 2))
	done
	die "port-forward to $resource did not answer on localhost:$local_port within ${K8S_PORT_FORWARD_WAIT_S}s"
}

# Safe to call having opened no tunnel, so a caller can trap it before it opens
# one — which is the only ordering where a failure to open leaves nothing
# behind.
k8s_port_forward_stop() {
	[[ -n $K8S_PORT_FORWARD_PID ]] || return 0
	kill "$K8S_PORT_FORWARD_PID" 2>/dev/null || true
	wait "$K8S_PORT_FORWARD_PID" 2>/dev/null || true
	K8S_PORT_FORWARD_PID=""
}

# ---------------------------------------------------------------------------
# A run's object names
# ---------------------------------------------------------------------------

# A run id as a Kubernetes object name — the same rule as
# `knobs.kubernetes_name`, spelled in both languages because the drivers
# address the objects the renderer named. An RFC 1123 subdomain is lowercase
# and the `T` and `Z` in a run id's stamp are not, so an object named by the id
# as it stands is refused by the API server.
#
# Names only: the topic, the table, the run directory and the bucket prefixes
# are addressed by the run id itself and are not lowercased anywhere.
k8s_object_name() {
	printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

# Where a run's object name goes in the descriptor strings that carry one — the
# same marker `specs/kubernetes.NAME` declares, spelled in both languages
# because the driver is what holds the name. A test holds the two together.
ENGINE_NAME_MARKER='<name>'

# How the engine of a run is addressed on this cluster, in the globals below,
# with the run's object name already substituted into the ones that carry it.
#
# One call rather than one per field: the harness command is a Python process,
# and a driver that started thirteen of them would spend seconds asking for
# constants. The engine's own module is the only statement of these names — a
# driver that restated one would drift from the renderer the first time it
# changed, which is also what keeps a third engine out of this file.
#
# k8s_read_engine <engine> <run object name>
k8s_read_engine() {
	local engine=$1 name=$2 fields="" key="" value="" required=""
	fields="$(harness_local engine-k8s "$engine")" ||
		die "could not read how a '$engine' run is addressed on a cluster; the line above says why"
	ENGINE_KIND=""
	ENGINE_RUNNING_STATE=""
	ENGINE_FAILED_STATES=""
	ENGINE_STATE_JSONPATH=""
	ENGINE_ERROR_JSONPATH=""
	ENGINE_LIFECYCLE_JSONPATH=""
	ENGINE_REST_SERVICE_SUFFIX=""
	ENGINE_REST_PORT=""
	ENGINE_LOG_TARGET=""
	ENGINE_PROVENANCE_SELECTOR=""
	ENGINE_PODS_SELECTOR=""
	ENGINE_DOCUMENT_FILE=""
	ENGINE_CONFIGMAP_FILE=""
	# `IFS='='` splits on the first `=` only, which a selector holding one of
	# its own needs. A key this does not know is a refusal rather than a field
	# quietly dropped: the driver would go on to address the cluster with a
	# name nobody read.
	while IFS='=' read -r key value; do
		[[ -n $key ]] || continue
		value="${value//$ENGINE_NAME_MARKER/$name}"
		case "$key" in
		kind) ENGINE_KIND="$value" ;;
		running_state) ENGINE_RUNNING_STATE="$value" ;;
		failed_states) ENGINE_FAILED_STATES="$value" ;;
		state_jsonpath) ENGINE_STATE_JSONPATH="$value" ;;
		error_jsonpath) ENGINE_ERROR_JSONPATH="$value" ;;
		lifecycle_jsonpath) ENGINE_LIFECYCLE_JSONPATH="$value" ;;
		rest_service_suffix) ENGINE_REST_SERVICE_SUFFIX="$value" ;;
		rest_port) ENGINE_REST_PORT="$value" ;;
		log_target) ENGINE_LOG_TARGET="$value" ;;
		provenance_selector) ENGINE_PROVENANCE_SELECTOR="$value" ;;
		pods_selector) ENGINE_PODS_SELECTOR="$value" ;;
		document_file) ENGINE_DOCUMENT_FILE="$value" ;;
		configmap_file) ENGINE_CONFIGMAP_FILE="$value" ;;
		*) die "engine-k8s $engine names a field '$key' that no driver here reads; update scripts/_k8s.sh" ;;
		esac
	done <<<"$fields"
	# Every field but the pods selector, which is empty for an engine whose
	# check reads nothing off the pods.
	for required in ENGINE_KIND ENGINE_RUNNING_STATE ENGINE_FAILED_STATES ENGINE_STATE_JSONPATH \
		ENGINE_ERROR_JSONPATH ENGINE_LIFECYCLE_JSONPATH ENGINE_REST_SERVICE_SUFFIX ENGINE_REST_PORT \
		ENGINE_LOG_TARGET ENGINE_PROVENANCE_SELECTOR ENGINE_DOCUMENT_FILE ENGINE_CONFIGMAP_FILE; do
		[[ -n ${!required} ]] || die "engine-k8s $engine printed no ${required#ENGINE_}, so this run cannot be addressed"
	done
}

# The two Jobs `launch.sh` creates, `teardown.sh` deletes and `purge.sh` looks
# for, named in one place so a rename cannot leave a producer fleet running
# after a teardown — which is also why both take a run id and lowercase it here
# rather than at each of the five call sites. The engine's own objects are named
# by the documents that render them, and are deleted through those documents
# rather than by a name restated here.
producer_job() {
	printf 'producer-%s' "$(k8s_object_name "$1")"
}

scorer_job() {
	printf 'scorer-%s' "$(k8s_object_name "$1")"
}
