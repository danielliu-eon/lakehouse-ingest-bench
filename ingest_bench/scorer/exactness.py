# SPDX-License-Identifier: Apache-2.0
"""Summarize missing rows, duplicate rows, and tally violations.

Score only offered batches, including for shortened replays. Count loss and
duplication separately so they cannot cancel each other out.
"""

from __future__ import annotations

from typing import cast

import numpy as np

from ingest_bench.scorer.tally import CORRUPTION, BatchTally

_VIOLATION_CAP = 200


def _scored(tally: BatchTally, offered_batches: set[int]) -> np.ndarray:
    """Select manifest batches recorded as offered in the publish logs."""
    mask = np.zeros(tally.counts.size, dtype=bool)
    indices = sorted(offered_batches)
    if indices and (indices[0] < 0 or indices[-1] >= mask.size):
        offender = indices[0] if indices[0] < 0 else indices[-1]
        raise ValueError(f"offered batch {offender} is outside the manifest's {mask.size} batches")
    mask[indices] = True
    return mask


def exactness_result(tally: BatchTally, *, offered_batches: set[int]) -> dict[str, object]:
    """Summarize exactness for offered batches, counting missing batches as loss.

    Cap the detailed violation list while retaining complete aggregate counts.
    """
    scored = _scored(tally, offered_batches)
    violations = [
        violation for violation in tally.violations(include_missing=True) if scored[int(cast(int, violation["batch"]))]
    ]
    expected = tally.expected_rows[scored]
    counts = tally.counts[scored]
    expected_rows = int(expected.sum())
    duplicate_rows = int(np.maximum(counts - expected, 0).sum())
    return {
        "expected_rows": expected_rows,
        "rows": int(counts.sum()),
        "scored_batches": int(scored.sum()),
        "loss_rows": int(np.maximum(expected - counts, 0).sum()),
        "duplicate_rows": duplicate_rows,
        "duplicate_ppm": duplicate_rows / expected_rows * 1e6 if expected_rows else None,
        "corrupt_batches": sum(1 for violation in violations if violation["kind"] == CORRUPTION),
        "violations": violations[:_VIOLATION_CAP],
        "exact": not violations,
    }
