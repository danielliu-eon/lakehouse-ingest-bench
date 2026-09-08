"""Whether the table holds exactly the rows the corpus offered.

The tally already knows where the table and the manifest disagree; this reduces
that to the figures a leg is reported by, and to the one boolean a run passes or
fails on. Loss and duplication are counted separately because they are
different faults with different causes — a lost row is data the engine dropped,
a duplicated row is data it replayed — and a run that loses a thousand rows and
duplicates a thousand others is not a run that got the answer right.
"""

from __future__ import annotations

import numpy as np

from ingest_bench.scorer.tally import CORRUPTION, BatchTally

_VIOLATION_CAP = 200


def exactness_result(tally: BatchTally) -> dict[str, object]:
    """One leg's exactness figures, drawn from the whole manifest.

    Batches nothing arrived for are counted as lost rather than withheld: this
    is the judgement taken after the run, and a leg stopped while it was still
    behind must report the rows it never received rather than be scored only on
    the batches it got to.

    The violation list is capped. It exists to name the first faults for a
    reader, and a leg that lost a partition would otherwise carry hundreds of
    thousands of entries into the artifact; the counts above it are the
    complete figures.
    """
    violations = tally.violations(include_missing=True)
    expected_rows = int(tally.expected_rows.sum())
    duplicate_rows = int(np.maximum(tally.counts - tally.expected_rows, 0).sum())
    return {
        "expected_rows": expected_rows,
        "rows": tally.committed_rows(),
        "loss_rows": int(np.maximum(tally.expected_rows - tally.counts, 0).sum()),
        "duplicate_rows": duplicate_rows,
        "duplicate_ppm": duplicate_rows / expected_rows * 1e6 if expected_rows else None,
        "corrupt_batches": sum(1 for violation in violations if violation["kind"] == CORRUPTION),
        "violations": violations[:_VIOLATION_CAP],
        "exact": not violations,
    }
