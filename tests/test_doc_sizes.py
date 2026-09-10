# SPDX-License-Identifier: Apache-2.0
"""Each document has one job, and a line budget that keeps it to that job.

A document nobody finishes reading is a document whose facts are not available,
and the way a document stops being read is by absorbing a second subject. The
budgets below are what the split between these files was chosen to fit, so a
document that outgrows one has almost always taken something another file owns —
which is a prompt to move it rather than to raise the number.

Raising a budget is legitimate when a file genuinely gains a subject of its own.
Raise it here in the same change, so the size is a decision and not a drift.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# The reader each file serves is why its budget is the size it is: a front page
# and a pitfall list are skimmed, a reference is consulted, and a methodology is
# read once and returned to.
MAX_LINES: dict[str, int] = {
    "README.md": 150,
    "CONTRIBUTING.md": 150,
    "docs/methodology.md": 250,
    "docs/corpus.md": 300,
    "docs/running.md": 300,
    "docs/run-spec.md": 150,
    "docs/adding-an-engine.md": 200,
    "docs/results-format.md": 150,
    "docs/pitfalls.md": 140,
    "engines/flink/README.md": 150,
    "engines/spark/README.md": 150,
}


@pytest.mark.parametrize("name", sorted(MAX_LINES), ids=sorted(MAX_LINES))
def test_the_document_is_inside_its_budget(name: str) -> None:
    path = REPO_ROOT / name
    lines = len(path.read_text(encoding="utf-8").splitlines())
    assert lines <= MAX_LINES[name], (
        f"{name} is {lines} lines, over its {MAX_LINES[name]}-line budget. "
        "Move what belongs to another document, or raise the budget in this file."
    )


def test_every_budgeted_document_exists() -> None:
    """A budget for a file nobody wrote passes by having nothing to measure."""
    missing = sorted(name for name in MAX_LINES if not (REPO_ROOT / name).is_file())
    assert not missing, f"budgeted but absent: {', '.join(missing)}"
