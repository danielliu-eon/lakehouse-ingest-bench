# SPDX-License-Identifier: Apache-2.0
# How a Spark run is started, made ready and read on the local Compose stack.
#
# The other half of the seam `specs/kubernetes.py` is: that file says how a run
# of this engine is addressed on a cluster, and this one says how it is
# addressed on one machine — the service to build and raise, the env file the
# renderer wrote the submission line's shape into, and a readiness probe nobody
# outside the engine can write. Together they are what keeps `scripts/` free of
# an engine's names.
#
# Sourced by scripts/smoke.sh, never executed, and only for a run whose spec
# names this engine. The four `engine_compose_*` hooks are the whole contract
# and are called in the order they are declared. `compose`, `harness`, `log`
# and `die` come from scripts/_lib.sh; `RUN_ID`, `RUN_DIR`, `SPEC_FILE` and
# `REPO_ROOT` are set before the first call.

# The Spark driver's web UI, as the compose file publishes it. There is no
# submission endpoint to ask instead: the driver is the process the profile
# runs, so its own UI is the only thing that can report on it.
SPARK_UI="${SPARK_UI:-http://localhost:4040}"
SPARK_APP_WAIT_S="${SPARK_APP_WAIT_S:-180}"
SPARK_QUERY_WAIT_S="${SPARK_QUERY_WAIT_S:-180}"

# Spark publishes no REST resource for Structured Streaming: `api/v1/.../streaming`
# is the DStream one and is registered only where a StreamingContext exists, so
# it answers 404 here (checked against 3.5.9). The driver's Structured
# Streaming tab is the remaining read of the same state — the listener that
# records a started query is what renders it — so the count in its heading is
# what is polled.
_spark_active_queries() {
	local count
	count="$(curl -sf --max-time 5 "$SPARK_UI/StreamingQuery/" 2>/dev/null | tr -d '\n' |
		sed -n 's/.*Active Streaming Queries (\([0-9]*\)).*/\1/p' | head -n 1)"
	case "$count" in '' | *[!0-9]*) count=0 ;; esac
	printf '%s' "$count"
}

# The driver is the process, so a driver that exited is a run that has already
# ended. Both waits below poll it: without this, a driver that dies before it
# ever serves its UI reports a timeout rather than the failure that stopped it.
_die_if_the_spark_driver_exited() {
	if compose ps --status exited --services 2>/dev/null | grep -qx spark-job; then
		die "the spark driver exited before its query started; check: compose logs spark-job"
	fi
}

# The application is named after the run — `spark.app.name` is the run id — so
# this is also the check that the driver answering on the UI is running the job
# just started, rather than one a previous `--keep` left behind.
wait_for_spark_query() {
	local name=$1 waited=0 running="" queries=0
	while ((waited < SPARK_APP_WAIT_S)); do
		running="$(curl -sf --max-time 5 "$SPARK_UI/api/v1/applications" 2>/dev/null |
			jq -r --arg name "$name" 'map(select(.name == $name)) | length' 2>/dev/null || true)"
		if [[ $running == 1 ]]; then break; fi
		_die_if_the_spark_driver_exited
		sleep 2
		waited=$((waited + 2))
	done
	[[ $running == 1 ]] ||
		die "no spark application named $name on $SPARK_UI after ${SPARK_APP_WAIT_S}s; check: compose logs spark-job"
	log "spark application $name is up"
	waited=0
	while ((waited < SPARK_QUERY_WAIT_S)); do
		queries="$(_spark_active_queries)"
		if ((queries >= 1)); then
			log "spark has $queries active streaming query(ies)"
			return 0
		fi
		_die_if_the_spark_driver_exited
		sleep 2
		waited=$((waited + 2))
	done
	die "spark started no streaming query within ${SPARK_QUERY_WAIT_S}s; check: compose logs spark-job"
}

engine_compose_build() {
	log "building the engine image"
	compose build spark-job
}

engine_compose_start() {
	# The submission line's shape, as the engine's renderer wrote it. Exported
	# so compose interpolates the driver's cores and its heap. There is no
	# separate submitter: under `--master local[N]` this one container is the
	# driver, its executors and the job.
	set -a
	# shellcheck source=/dev/null
	source "$RUN_DIR/job.env"
	set +a
	log "starting spark: local[$LOCAL_CORES], ${DRIVER_MEM_MB}m driver"
	compose up -d spark-job
}

engine_compose_ready() {
	wait_for_spark_query "$RUN_ID"
}

engine_compose_logs() {
	log "--- last 40 lines of spark-job ---"
	compose logs --tail 40 --no-log-prefix spark-job >&2 || true
}
