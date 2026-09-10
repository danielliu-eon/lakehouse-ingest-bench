# Contributing

## Required checks

Run the same checks as CI:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q -m "not integration"
uv run python scripts/validate-results.py
```

Install `jq` and `yq` (mikefarah v4) on PATH. Cluster-driver tests skip when
these tools are unavailable.

Integration tests require the local Compose stack and are excluded above.
Run `scripts/smoke.sh` for each affected engine when changing
`deploy/compose/`, `engines/*/compose.yaml`, `scripts/smoke.sh`, or an engine
image. The `smoke.yml` CI workflow runs only on manual dispatch.

Use Python 3.12+, strict mypy typing and ruff's 120-column limit. Do not
swallow exceptions or supply defaults for required values. Every `.py` and
`.sh` file needs an SPDX licence header, after the shebang if present.

## Keep private details out of the repository

`tests/test_public_surface.py` scans tracked files for identifying names,
account and project IDs, email addresses, and non-placeholder storage buckets.
Use reproducible placeholders and fixtures instead of private infrastructure
details.

Comments should explain mechanisms, rationale and constraints. Keep incident
history, dates and cluster names out of prose. Put measurements in
`docs/examples/` beside their supporting artifacts, or in `results/`.

## Add a corpus shape

Add a schema JSON file under `workloads/schemas/` and a preset YAML file under
`workloads/presets/`. [`docs/corpus.md`](docs/corpus.md) defines column kinds,
roles, preset keys, value-space constraints and generation checks.

The preset hash identifies the corpus directory. Changing the schema or
preset creates a new corpus identity. Publish the schema and preset in the
same change as any result that uses them.

Preview sizing without writing a corpus:

```bash
gen-corpus --preset <name> --out <uri> --plan
```

## Add an engine

Start with the external contract in
[`docs/adding-an-engine.md`](docs/adding-an-engine.md). It requires no engine
code in the harness: staging prepares the run and prints connection facts.

A managed engine needs the files listed in that guide under `engines/<name>/`
and a knobs-module registration. Keep engine-specific behavior in that
package.

## Add a preset key or engine knob

Loaders reject unknown keys. Add the loader entry, default, test and reference
entry in [`docs/run-spec.md`](docs/run-spec.md) or the engine README. Define
defaults in the loader and avoid duplicating them in callers.

## Add a result

Generate results with `scripts/finish.sh <run_id> --publish results/`.
This writes redacted JSON under `results/<engine>/` and regenerates
`results/RESULTS.md`; do not edit either by hand.

[`results/README.md`](results/README.md) separates automated validation from
reviewer checks. CI rejects a stale results table. To regenerate it separately,
run `results-table results/ --out results/RESULTS.md`.

## Write documentation

Keep each document focused on its audience and purpose.
`tests/test_doc_sizes.py` enforces line budgets. Before adding an explanation,
search for an existing one and link to it where possible. Define benchmark
terms in [`docs/methodology.md`](docs/methodology.md).

## Commit messages

Use `type: subject`, with an imperative subject and no scope, for example:
`fix: reject client properties that override the wire codec`.
Explain the change in the subject and the rationale in the body.
