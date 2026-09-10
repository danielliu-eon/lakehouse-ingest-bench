# SPDX-License-Identifier: Apache-2.0
"""Keep documents within their scope and line budgets. Move unrelated material
before increasing a budget; update the budget when a document gains necessary scope.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Budgets reflect each document's scope and expected reading style.
MAX_LINES: dict[str, int] = {
    "README.md": 160,
    "CONTRIBUTING.md": 150,
    "docs/methodology.md": 250,
    "docs/corpus.md": 300,
    "docs/running.md": 300,
    "docs/run-spec.md": 150,
    "docs/adding-an-engine.md": 220,
    "docs/results-format.md": 150,
    "docs/pitfalls.md": 150,
    "deploy/k8s/stack/README.md": 200,
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
    missing = sorted(name for name in MAX_LINES if not (REPO_ROOT / name).is_file())
    assert not missing, f"budgeted but absent: {', '.join(missing)}"
