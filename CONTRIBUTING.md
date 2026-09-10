# Contributing

## The checks

Every change has to pass what CI runs, and CI runs only these:

```bash
uv sync --frozen
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q -m "not integration"
uv run python scripts/validate-results.py
```

`jq` and `yq` (mikefarah v4) have to be on the path: the tests that render the
cluster drivers' inputs skip themselves without both.

Tests marked `integration` need the local Compose stack and are excluded above.
The end-to-end check is `scripts/smoke.sh` — run it for either engine when you
touch `deploy/compose/`, `engines/*/compose.yaml`, `scripts/smoke.sh` or an
engine image. In CI it is `smoke.yml`, on manual dispatch only.

House rules the checks enforce for you: Python 3.12+, `mypy --strict` with no
`Any`, a 120-column ruff line, no swallowed exceptions, no defaulted lookup of a
value that is required, and an SPDX licence line as the first line of every
`.py` and `.sh` file (second, after a shebang).

## What must never enter the tree

`tests/test_public_surface.py` sweeps every tracked file for a company, product
or person name, a cloud account id, a project id, an email address and an
object-store bucket that is not a placeholder or a fixture. It fails on a match
and names the remedy. That guard exists because this repository is run by
strangers against their own accounts: a real bucket or account id is both a leak
and a step nobody else can reproduce.

Comments and documents carry the mechanism, never the history: no observed
figures without the run they came from, no incident narration, no dates or
cluster names. A measurement belongs in `docs/examples/` beside the artifacts it
came from, or in `results/`.

## Adding a corpus shape

A shape is a schema JSON under `workloads/schemas/` and a preset YAML under
`workloads/presets/`. [`docs/corpus.md`](docs/corpus.md) is the reference: the
column kinds and roles, every preset key, the value-space rules the loader
enforces, and the gates generation applies to the corpus it produced.

Two things to know before starting:

1. **The preset hash names the corpus directory**, so any change to a schema or
   a preset produces a new corpus rather than rescoring an old one.
2. **A shape must be shipped here before a result from it can be published** —
   the "ship the preset" rule in that document. Add both files in the same
   change as the result.

Check the shape with `gen-corpus --preset <name> --out <uri> --plan`, which
prints the batch count, the estimated rows and the mean row size without writing
a byte.

## Adding an engine

Read [`docs/adding-an-engine.md`](docs/adding-an-engine.md). The external tier is
the primary contract and needs no code here: the harness prepares the run, prints
the facts and waits. Adding a *managed* engine means a new `engines/<name>/`
carrying the eight files that section lists, and one line registering its knobs
module. The harness holds no engine-specific branch outside `engines/<name>/`.

## Adding a preset or a knob

Both are refuse-unknown-keys surfaces: an unrecognised preset key or engine knob
is an error, not a silent default. So a new key needs its loader entry, its
default, a test, and a row in [`docs/run-spec.md`](docs/run-spec.md) or the
engine's own README. A default that appears in two places will drift; state it
once, where the loader reads it.

## Adding a result

A result is one redacted `run.json` under `results/<engine>/`, written by
`scripts/finish.sh <run_id> --publish results/` and never by hand.
[`results/README.md`](results/README.md) lists what a published result must
satisfy, and which of those rules `validate-results.py` checks as against which
a reviewer has to. `results/RESULTS.md` is generated from the documents beside
it: re-render it with `results-table results/ --out results/RESULTS.md` in the
same change, since CI fails on a stale one.

## Documentation

Each document has one job and a line budget, and `tests/test_doc_sizes.py`
enforces the budget. One explanation lives in one place and the other places link
to it, so before adding a paragraph, grep for a distinctive sentence of it. Terms
are defined in [`docs/methodology.md`](docs/methodology.md); define a new one
there rather than in passing.

## Commits

`type: subject` in the imperative, no scope — `fix: refuse a client property
that would set the wire codec`. The subject says what changes; the body says
why, and carries the reasoning that does not belong in a comment.
