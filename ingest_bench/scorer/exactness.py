"""Whether the table holds exactly the rows the corpus offered.

The tally already knows where the table and the manifest disagree; this reduces
that to the figures a run is reported by, and to the one boolean a run passes or
fails on. Loss and duplication are counted separately because they are
different faults with different causes — a lost row is data the engine dropped,
a duplicated row is data it replayed — and a run that loses a thousand rows and
duplicates a thousand others is not a run that got the answer right.

Only the batches that were actually offered are judged. A run may be replayed
over a prefix of the corpus, and the manifest then describes more batches than
any producer sent; scoring those would report rows nobody offered as rows the
engine lost, and make a shortened replay invalid by construction.
"""

from __future__ import annotations

from typing import cast

import numpy as np

from ingest_bench.scorer.tally import CORRUPTION, BatchTally

_VIOLATION_CAP = 200


def _scored(tally: BatchTally, offered_batches: set[int]) -> np.ndarray:
    """A mask over the manifest's batches, true for the ones that were offered.

    The offered set comes from the publish logs, which are the only record of
    what a producer actually sent. A batch outside it is neither expected nor
    reported: it has no offer to be judged against.
    """
    mask = np.zeros(tally.counts.size, dtype=bool)
    indices = sorted(offered_batches)
    if indices and (indices[0] < 0 or indices[-1] >= mask.size):
        offender = indices[0] if indices[0] < 0 else indices[-1]
        raise ValueError(f"offered batch {offender} is outside the manifest's {mask.size} batches")
    mask[indices] = True
    return mask


def exactness_result(tally: BatchTally, *, offered_batches: set[int]) -> dict[str, object]:
    """One run's exactness figures over the batches it was offered.

    Batches nothing arrived for are counted as lost rather than withheld: this
    is the judgement taken after the run, and a run stopped while it was still
    behind must report the rows it never received rather than be scored only on
    the batches it got to.

    The violation list is capped. It exists to name the first faults for a
    reader, and a run that lost a partition would otherwise carry hundreds of
    thousands of entries into the artifact; the counts above it are the
    complete figures.
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
