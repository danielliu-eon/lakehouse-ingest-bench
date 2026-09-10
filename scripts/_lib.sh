# SPDX-License-Identifier: Apache-2.0
# Shared shell for the run scripts: where the stack is, how to speak to it, and
# the questions every driver asks whatever engine a run names.
#
# Nothing here knows an engine. How one is built, raised, made ready and read
# on this stack is its own package's `engines/<name>/compose.sh`, which
# `smoke.sh` sources for the run's engine alone.
#
# Sourced, never executed. Shell options belong to the caller — nothing here
# sets or clears one, so a script that runs without `set -e` still does.

# Resolved from this file rather than from the caller's working directory. Every
# path the stack mounts is relative to the compose file, so a script invoked
# from elsewhere would otherwise mount a tree that is not this checkout.
_LIB_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$_LIB_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/deploy/compose/local/docker-compose.yml"

# Where a caller's prerequisites are written down, for the one message that has
# to point at them. Set before sourcing this file: the cloud setup scripts under
# deploy/ share these functions and have their own list of tools.
PREREQ_DOC="${PREREQ_DOC:-docs/running.md}"

# stderr, so a caller can still parse a command's stdout through a pipe while
# the narration stays on screen.
log() {
	printf '%s  %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

die() {
	log "$*"
	exit 1
}

# Every profile is activated on every call. Compose interpolates the whole model
# before it filters by profile, so naming them all costs nothing and removes the
# class of failure where a service is invisible to the one command that needs
# it. What actually starts is always named explicitly.
#
# The engines' profiles are read out of the compose files that declare them, so
# a third engine adds a directory of its own rather than a name to this file.
# `tools` is the harness's own service and is this file's to name.
compose() {
	local profiles=() name
	while IFS= read -r name; do
		[[ -n $name ]] || continue
		profiles+=(--profile "$name")
	done < <(yq -N '.services.*.profiles[]' "$REPO_ROOT"/engines/*/compose.yaml | sort -u)
	docker compose -f "$COMPOSE_FILE" --profile tools ${profiles[@]+"${profiles[@]}"} "$@"
}

# One harness command, as the single string the image's shell entrypoint splits.
# `-T` because some of these are parsed, and a TTY carriage-returns every line.
harness() {
	compose run --rm -T harness "$1"
}

# The verdict block, and a refusal unless the run is publishable. Shared by the
# local smoke and the cloud drivers so that both read the same fields in the
# same order — a second copy of this filter would drift, and the copy that lost
# would be the one nobody reread.
#
# A geometry document is optional, and is one line after the block. No field in
# it decides validity, which is why it is not in the filter above; it belongs
# here rather than in a caller because it has to be shown before the refusal
# below, and an invalid run's geometry is exactly as measured as a valid one's.
print_verdict() {
	local summary=$1 geometry=${2:-}
	[[ -f $summary ]] || die "the scorer published no $summary"
	jq '{
  run_valid, state, reason, producer_bound,
  prefix, last_batch, committed_rows, offered_rows,
  freshness: .freshness.window,
  exactness: {exact: .exactness.exact, loss_rows: .exactness.loss_rows, duplicate_rows: .exactness.duplicate_rows},
  keepup
}' "$summary"
	# `select` rather than a conditional: a table that took no commit reports a
	# null p50, and the line is then left out instead of printed over nothing.
	if [[ -n $geometry && -f $geometry ]]; then
		jq -r '(.final.live // empty) | select(.size_quantiles.p50 != null)
  | "geometry: p50 \((.size_quantiles.p50 / 1048576 * 10 | round) / 10) MiB, "
    + "small (<32 MiB) \((.small_file_share_32mib * 1000 | round) / 10)%, \(.files) files"' "$geometry"
	fi
	[[ "$(jq -r .run_valid "$summary")" == true ]] ||
		die "run_valid is false; the block above says why, in full in $summary"
}

# confirm <what> — a y/N question, answered on stdin, for a step that destroys
# measured data or the bucket holding it.
#
# stdin rather than the terminal device, so a caller can answer through a pipe.
# A caller with no stdin at all is refused rather than defaulted: "nothing
# answered" is not consent to delete. The caller has already printed what goes;
# this asks about it.
confirm() {
	[[ -t 0 ]] || die "nothing is attached to answer, and this does not assume one; pass --yes to run unattended"
	printf '%s [y/N] ' "$1"
	local answer=""
	read -r answer || true
	case "$answer" in
	y | Y) return 0 ;;
	*) die "answered '${answer:-nothing}', so nothing was removed" ;;
	esac
}

# Refuse up front rather than half way through a run. A missing `yq` surfaces
# otherwise as a scorer given an empty `--warmup-s`, minutes after the corpus
# was generated.
require_host_tools() {
	local missing="" tool
	for tool in "$@"; do
		command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
	done
	[[ -z $missing ]] || die "missing host tool(s):$missing — see $PREREQ_DOC for what this needs"
}
