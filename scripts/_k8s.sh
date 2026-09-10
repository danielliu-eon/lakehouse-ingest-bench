# SPDX-License-Identifier: Apache-2.0
# Shared helpers for cluster drivers. Source after `_lib.sh` for log, die and REPO_ROOT.
# Drivers load KUBE_CONTEXT and SITE_NAMESPACE once from the site config, so all
# operations use the same cluster and namespace. Keep diagnostics on stderr; callers parse
# stdout as command results.

# Read at call time so a driver's --site option can override the default.
SITE_FILE="${SITE_FILE:-./site.yaml}"

# Local run artifacts. The pods write to site.runs_root in object storage.
RUNS_DIR="${RUNS_DIR:-./runs}"

# Registry repository prefix shared by image builds and Job manifests.
IMAGE_REPOSITORY_PREFIX=lakehouse-ingest-bench

# Job polling interval; callers set the timeout for each operation.
K8S_JOB_POLL_S="${K8S_JOB_POLL_S:-10}"

# Timeout for an HTTP request through a new port-forward.
K8S_PORT_FORWARD_WAIT_S="${K8S_PORT_FORWARD_WAIT_S:-30}"

# Local port and health path for tunnels to cluster-internal catalogs.
CATALOG_FORWARD_PORT="${CATALOG_FORWARD_PORT:-18181}"
CATALOG_FORWARD_PROBE="${CATALOG_FORWARD_PROBE:-/health}"

# Engine image provenance and the wait for the kubelet to report its digest.
ENGINE_IMAGE_FILE=engine-image.json
ENGINE_IMAGE_WAIT_S="${ENGINE_IMAGE_WAIT_S:-60}"
ENGINE_IMAGE_POLL_S="${ENGINE_IMAGE_POLL_S:-5}"

# Saved table metadata used by teardown, geometry measurement and purge. Always stored as
# JSON; see k8s_fetch_metadata_document.
METADATA_FINAL_FILE=table-metadata.final.json

# ---------------------------------------------------------------------------
# The table's metadata document
# ---------------------------------------------------------------------------

# Must match ingest_bench.table.cli.TABLE_ABSENT. Other nonzero statuses indicate a read
# failure, not an absent table.
TABLE_ABSENT=3

# k8s_fetch_metadata_document <metadata uri> <local path>
# Fetch metadata as plain JSON for jq readers. Writers may enable gzip compression; detect
# it by magic bytes because the filename is only a convention.
k8s_fetch_metadata_document() {
	local source=$1 path=$2
	aws s3 cp "$source" "$path" --only-show-errors >&2 || return 1
	local magic=""
	magic="$(od -An -tx1 -N2 -- "$path" | tr -d ' \n')" ||
		die "could not read the first bytes of $path to tell whether it is compressed"
	[[ $magic == 1f8b ]] || return 0
	log "$source is gzip-compressed; storing it decompressed"
	# Replace only after successful decompression, preserving the fetched file on failure.
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

# Return an empty string for a missing key, not yq's literal `null`.
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

# Report a missing config before attempting to read its keys.
require_site_file() {
	[[ -f $SITE_FILE ]] || die "no site config at $SITE_FILE; copy site.aws.example.yaml and fill it in"
}


S3_SCHEME="s3://"

# site_root <yq path to a root>
# Require s3:// because these drivers use the AWS CLI for storage. The Python storage
# layer also supports gs://, but the drivers do not.
site_root() {
	local root
	root="$(site_required "$1")"
	[[ $root == "$S3_SCHEME"* ]] ||
		die "${1#.} is '$root', and these drivers reach storage through the aws CLI: the cloud path is AWS-only today, so every root has to be an s3:// URI"
	printf '%s' "$root"
}

# Return compact JSON, which is valid YAML at any indentation in a manifest.
site_json() {
	yq -o=json -I=0 "$1 // $2" "$SITE_FILE"
}

# Return one key=value per map entry, or nothing for an omitted map. Capture yq's output
# and check its status before iterating, so a failed read cannot become an empty
# configuration.
site_pairs() {
	local entries
	entries="$(yq "$1 // {} | to_entries | .[] | .key + \"=\" + .value" "$SITE_FILE")" ||
		die "could not read ${1#.} out of $SITE_FILE; every value under it must be a quoted string"
	printf '%s' "$entries"
}

# site_flags <yq path to a map> <flag>
# Build single-quoted key=value arguments for a Job's shell command. Quoting preserves
# spaces and literal ${env:NAME} references for Python to resolve. Reject single quotes
# that would break shell quoting and double quotes that would break the enclosing YAML
# scalar.
site_flags() {
	# Check the assignment explicitly: failure inside command substitution does not reliably
	# trigger the caller's set -e.
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

# Populate CATALOG_PROP_FLAGS with local --catalog-prop arguments. Arrays
# preserve spaces in values. Cluster Service URIs use a local tunnel; other
# properties pass through unchanged. Install k8s_port_forward_stop on EXIT
# before calling, because this may open a tunnel.
read_catalog_prop_flags() {
	local pairs
	pairs="$(site_pairs '.catalog.props')" ||
		die "cannot build the catalog flags a harness command needs; the line above says why"
	CATALOG_PROP_FLAGS=()
	local pair
	while IFS= read -r pair; do
		[[ -n $pair ]] || continue
		if [[ $pair == uri=* ]]; then
			k8s_reach_catalog "${pair#uri=}"
			pair="uri=$CATALOG_URI"
		fi
		CATALOG_PROP_FLAGS+=(--catalog-prop "$pair")
	done <<<"$pairs"
}

# k8s_service_host <host>
# Return <service> <namespace> for hosts matching <service>.<namespace>.svc with an
# optional .cluster.local suffix. Return nonzero for other hosts.
k8s_service_host() {
	[[ $1 =~ ^([a-z0-9-]+)\.([a-z0-9-]+)\.svc(\.cluster\.local)?$ ]] || return 1
	printf '%s %s' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
}

# Set CATALOG_URI to the original URI or a local tunnel for a cluster Service. Set globals
# in the caller's shell so its EXIT trap can access the tunnel PID.
k8s_reach_catalog() {
	local uri=$1 rest hostport host port path parts service namespace
	CATALOG_URI="$uri"
	rest="${uri#*://}"
	[[ $rest != "$uri" ]] || return 0
	hostport="${rest%%/*}"
	path="${rest#"$hostport"}"
	host="${hostport%%:*}"
	parts="$(k8s_service_host "$host")" || return 0
	read -r service namespace <<<"$parts"
	[[ $uri == http://* ]] ||
		die "catalog.props.uri is $uri: a Service name is reached through a tunnel, and the tunnel carries plain http only"
	if [[ $hostport == *:* ]]; then
		port="${hostport##*:}"
	else
		port=80
	fi
	k8s_port_forward "svc/$service" "$CATALOG_FORWARD_PORT:$port" "$namespace" "$CATALOG_FORWARD_PROBE"
	CATALOG_URI="http://localhost:$CATALOG_FORWARD_PORT$path"
}

# Set both AWS_REGION and AWS_DEFAULT_REGION for Java and Python SDKs. Omit both when the
# site has no region; an empty value is not a valid region.
site_env_json() {
	local region
	region="$(site_value '.kubernetes.aws_region')"
	if [[ -z $region ]]; then
		printf '[]'
	else
		printf '[{"name":"AWS_REGION","value":"%s"},{"name":"AWS_DEFAULT_REGION","value":"%s"}]' "$region" "$region"
	fi
}

# Render envFrom for the site's optional Secret. Its keys supply the environment variables
# referenced by ${env:NAME} properties.
site_env_from_json() {
	local secret
	secret="$(site_value '.kubernetes.secret_name')"
	if [[ -z $secret ]]; then
		printf '[]'
	else
		printf '[{"secretRef":{"name":"%s"}}]' "$secret"
	fi
}

# Load the site once into the globals shared by all Job operations.
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

# Use the supplied tag or the checkout's commit, matching push-images.sh.
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

# Run an installed harness command, falling back to uv in the checkout. Keep stdout
# available to callers and pass absolute paths because the fallback changes directory.
# Leading --extra options apply only to uv. Catalog commands need the aws extra for Glue
# signing; installed commands must already have their dependencies.
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

# Check PATH or the checkout's script table without executing the command. A command's
# runtime failure must not be mistaken for its absence.
harness_available() {
	if command -v "$1" >/dev/null 2>&1; then
		return 0
	fi
	if command -v uv >/dev/null 2>&1 && grep -q "^$1 = " "$REPO_ROOT/pyproject.toml"; then
		return 0
	fi
	return 1
}

# Resolve paths before harness_local changes directory. The parent directory must already
# exist.
abs_path() {
	printf '%s/%s' "$(cd -- "$(dirname -- "$1")" && pwd)" "$(basename -- "$1")"
}

# k8s_render_apply <template relative to the repository root> NAME=VALUE...
# Pass --namespace to catch a manifest rendered for the wrong site. Callers must delete an
# existing Job before replacing its immutable spec.
k8s_render_apply() {
	local template=$1
	shift
	local set_args=() pair
	for pair in "$@"; do
		set_args+=(--set "$pair")
	done
	# This array expansion supports empty arrays under set -u on Bash 3.2.
	harness_local render-k8s "$REPO_ROOT/$template" ${set_args[@]+"${set_args[@]}"} |
		kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" apply -f - >&2
}

# Use the rendered manifest for object names, keeping deletion consistent with the engine
# renderer.
k8s_apply_file() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" apply -f "$1" >&2
}

k8s_delete_file() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" delete -f "$1" --ignore-not-found >&2
}

# k8s_configmap_from_file <name> <key>=<path>...
# Apply a client-rendered ConfigMap so retries can update an existing object.
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
	k8s_job_pod_events "$1"
}

# Print Job pod events alongside logs: unscheduled pods have no logs, and scheduling
# errors appear only in events. Reads are best-effort so diagnostics cannot replace the
# original failure status.
k8s_job_pod_events() {
	log "--- pods of job/$1 and their events ---"
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pods -l "job-name=$1" -o wide >&2 || true
	local names="" pod=""
	names="$(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pods -l "job-name=$1" \
		-o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null)" || return 0
	while IFS= read -r pod; do
		[[ -n $pod ]] || continue
		kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get events \
			--field-selector "involvedObject.name=$pod" >&2 || true
	done <<<"$names"
}

# Wait for Complete or Failed, reporting logs on failure or timeout. Checking both avoids
# waiting out the timeout after a Job has already failed.
k8s_wait_job() {
	local name=$1 timeout_s=$2 waited=0 conditions=""
	log "waiting up to ${timeout_s}s for job/$name"
	while :; do
		# Only true conditions count; Failed=False is not a failure.
		conditions="$(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get "job/$name" \
			-o 'jsonpath={range .status.conditions[?(@.status=="True")]}{.type}{"\n"}{end}')" ||
			die "could not read job/$name; try: kubectl get job/$name"
		case "$conditions" in
		*Failed*)
			k8s_job_tail "$name"
			die "job/$name failed; its log and its pods' events are above"
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
	die "job/$name did not complete within ${timeout_s}s; its log and its pods' events are above"
}

# Keep logs on stdout so drivers can parse the harness's reported output URI.
k8s_job_logs() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "job/$1"
}

k8s_delete() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" delete "$1" "$2" --ignore-not-found >&2
}

# k8s_object_present <kind> <name>
# Return the object's name, or empty if absent. Treat API failures as errors: purge must
# distinguish an absent writer from an unreadable cluster. Callers must check the
# assignment status because die runs in the command-substitution subshell.
k8s_object_present() {
	local name
	name="$(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get "$1" "$2" \
		--ignore-not-found -o 'jsonpath={.metadata.name}')" ||
		die "could not read $1/$2 out of $SITE_NAMESPACE; try: kubectl get $1/$2"
	printf '%s' "$name"
}

# k8s_write_engine_image <path> <label selector> [wait seconds]
# Record the pod's image reference and imageID to identify the image actually pulled. Wait
# briefly for a digest, then record null if it remains unavailable. Missing provenance
# does not fail the run.
# Use a range so an empty pod list is valid across kubectl versions.
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
	# Do not fail staging over missing provenance after the engine has started.
	if jq -n --arg image "$image" --arg digest "$digest" \
		'{image: $image, digest: (if $digest == "" then null else $digest end)}' >"$path"; then
		log "the engine ran $image (digest ${digest:-none reported})"
	else
		log "could not write $path, so this run records no engine image"
	fi
}

# Print the engine descriptor's log target to stderr.
k8s_engine_tail() {
	log "--- last 40 lines of $1 ---"
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "$1" --tail=40 >&2 || true
}

# Return the first matching pod name, or empty on absence or read failure. This best-
# effort check only decides whether failure diagnostics can include pod logs.
k8s_pods_present() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pod -l "$1" \
		-o 'jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | head -n 1 || true
}

# Read an operator status field, returning empty until available. The caller handles
# retries and timeout diagnostics.
k8s_engine_field() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get "$1/$2" \
		-o "jsonpath=$3" 2>/dev/null || true
}

# Write pod JSON for engine checks that verify fleet size and resource requests. A file
# lets the separate verifier identify its input in errors.
k8s_write_pods() {
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pods -l "$2" -o json >"$1" ||
		die "could not read the pods matching '$2' in $SITE_NAMESPACE; try: kubectl get pods -l '$2'"
}

# k8s_nodes_with_free_cpu <millicores> <nodes.json> <pods.json>
# Estimate how many nodes have room for one more pod. Subtract active, bound pods' app-
# container requests from allocatable CPU. Ignore completed pods, unbound pods and init
# containers.
# This is a warning heuristic, not a scheduler simulation: any site toleration permits all
# NoSchedule-tainted nodes in this count. File inputs allow tests without a cluster.
k8s_nodes_with_free_cpu() {
	local millicores=$1 nodes=$2 pods=$3 tolerated=false
	[[ -z $TOLERATIONS || $TOLERATIONS == "[]" ]] || tolerated=true
	jq --argjson want "$millicores" --argjson tolerated "$tolerated" --slurpfile pods "$pods" '
		def millicores:
			if . == null then 0
			elif type == "string" and endswith("m") then (.[:-1] | tonumber)
			else (tonumber * 1000)
			end;
		def requested: [.spec.containers[]?.resources.requests.cpu | millicores] | add // 0;
		([$pods[0].items[]
			| select(.spec.nodeName != null)
			| select(.status.phase != "Succeeded" and .status.phase != "Failed")
			| {node: .spec.nodeName, cpu: requested}]
			| group_by(.node)
			| map({key: .[0].node, value: (map(.cpu) | add)})
			| from_entries) as $used
		| [.items[]
			| select($tolerated or ([.spec.taints[]? | select(.effect == "NoSchedule")] | length == 0))
			| select((.status.allocatable.cpu | millicores) - ($used[.metadata.name] // 0) >= $want)]
		| length' "$nodes"
}

# ---------------------------------------------------------------------------
# A tunnel to one service
# ---------------------------------------------------------------------------

# Keep the tunnel PID in the caller's shell so its EXIT trap can stop it.
K8S_PORT_FORWARD_PID=""

# k8s_port_forward <resource> <local:remote> [namespace] [probe path]
# Wait for an HTTP response, not just a listening socket. The namespace defaults
# to the site's and the probe path to /.
k8s_port_forward() {
	local resource=$1 ports=$2 namespace=${3:-$SITE_NAMESPACE} probe=${4:-/} local_port=${2%%:*} waited=0
	kubectl --context "$KUBE_CONTEXT" --namespace "$namespace" port-forward "$resource" "$ports" >&2 &
	K8S_PORT_FORWARD_PID=$!
	while ((waited < K8S_PORT_FORWARD_WAIT_S)); do
		if curl -sf --max-time 5 -o /dev/null "http://localhost:$local_port$probe"; then
			log "port-forward to $resource answers on localhost:$local_port"
			return 0
		fi
		# Report an exited tunnel immediately instead of waiting for the timeout.
		if ! kill -0 "$K8S_PORT_FORWARD_PID" 2>/dev/null; then
			die "kubectl port-forward $resource $ports exited; the lines above are its own output"
		fi
		sleep 2
		waited=$((waited + 2))
	done
	die "port-forward to $resource did not answer on localhost:$local_port within ${K8S_PORT_FORWARD_WAIT_S}s"
}

# Safe before a tunnel exists, allowing callers to install cleanup before opening it.
k8s_port_forward_stop() {
	[[ -n $K8S_PORT_FORWARD_PID ]] || return 0
	kill "$K8S_PORT_FORWARD_PID" 2>/dev/null || true
	wait "$K8S_PORT_FORWARD_PID" 2>/dev/null || true
	K8S_PORT_FORWARD_PID=""
}

# ---------------------------------------------------------------------------
# A run's object names
# ---------------------------------------------------------------------------

# Lowercase Kubernetes object names to match the renderer's kubernetes_name rule. Topic
# names, table names, run directories and bucket prefixes retain the original run ID.
k8s_object_name() {
	printf '%s' "$1" | tr '[:upper:]' '[:lower:]'
}

# Must match specs/kubernetes.NAME; tests check the shared marker.
ENGINE_NAME_MARKER='<name>'

# k8s_read_engine <engine> <run object name>
# Load the engine descriptor in one Python invocation and substitute the run's object
# name. Engine modules own these names so drivers need no engine-specific branches.
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
	# Split at the first = to preserve selectors. Reject unknown fields so descriptor changes
	# cannot be silently ignored.
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
	# Only pods_selector may be empty: some verifiers do not inspect pods.
	for required in ENGINE_KIND ENGINE_RUNNING_STATE ENGINE_FAILED_STATES ENGINE_STATE_JSONPATH \
		ENGINE_ERROR_JSONPATH ENGINE_LIFECYCLE_JSONPATH ENGINE_REST_SERVICE_SUFFIX ENGINE_REST_PORT \
		ENGINE_LOG_TARGET ENGINE_PROVENANCE_SELECTOR ENGINE_DOCUMENT_FILE ENGINE_CONFIGMAP_FILE; do
		[[ -n ${!required} ]] || die "engine-k8s $engine printed no ${required#ENGINE_}, so this run cannot be addressed"
	done
}

# Share producer and scorer Job names across launch, teardown and purge. Engine object
# names remain in their rendered manifests.
producer_job() {
	printf 'producer-%s' "$(k8s_object_name "$1")"
}

scorer_job() {
	printf 'scorer-%s' "$(k8s_object_name "$1")"
}
