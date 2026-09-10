# SPDX-License-Identifier: Apache-2.0
"""Measure absorbed rows, backlog, and drain time.

These metrics distinguish sustained ingest from a fleet that catches up only
after the offer ends. Use rows consistently with the corpus and tally.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class KeepupSample:
    """Row totals, backlog, and interval rates at one sampling instant."""

    at_ms: int
    offered_rows: int
    committed_rows: int
    backlog_rows: int
    offered_rate: float | None
    committed_rate: float | None


def make_sample(at_ms: int, offered_rows: int, committed_rows: int, previous: KeepupSample | None) -> KeepupSample:
    """Calculate rates since the previous sample.

    The first sample, or a nonpositive interval, has unknown rates.
    """
    gap_ms = at_ms - previous.at_ms if previous is not None else 0
    if previous is not None and gap_ms > 0:
        seconds = gap_ms / 1000
        offered_rate: float | None = (offered_rows - previous.offered_rows) / seconds
        committed_rate: float | None = (committed_rows - previous.committed_rows) / seconds
    else:
        offered_rate, committed_rate = None, None
    return KeepupSample(
        at_ms=at_ms,
        offered_rows=offered_rows,
        committed_rows=committed_rows,
        # Clamp backlog at zero; exactness reports duplicate rows separately.
        backlog_rows=max(offered_rows - committed_rows, 0),
        offered_rate=offered_rate,
        committed_rate=committed_rate,
    )


def keepup_summary(
    samples: list[KeepupSample],
    offer_end_ms: int | None,
    drained_ms: int | None,
) -> dict[str, object]:
    """Summarize offer-end absorption, drain time, and backlog.

    Measure absorption from the last sample at or before the offer ended.
    """
    absorbed: float | None = None
    if offer_end_ms is not None:
        while_offering = [sample for sample in samples if sample.at_ms <= offer_end_ms]
        last = while_offering[-1] if while_offering else None
        if last is not None and last.offered_rows > 0:
            absorbed = last.committed_rows / last.offered_rows
    backlogs = [sample.backlog_rows for sample in samples]
    return {
        "absorbed_at_offer_end": absorbed,
        "drain_s": (drained_ms - offer_end_ms) / 1000 if drained_ms is not None and offer_end_ms is not None else None,
        "backlog_rows_max": max(backlogs) if backlogs else None,
        "backlog_rows_p50": float(np.percentile(np.array(backlogs, dtype=np.float64), 50)) if backlogs else None,
    }
