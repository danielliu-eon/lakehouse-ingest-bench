"""Whether the fleet absorbed the offer as fast as it was offered.

Freshness says how stale the table was; keep-up says whether the staleness was
bounded work or a growing debt. The two come apart at the end of a run: a fleet
that fell an hour behind and then drained still shows a small final lag, and
only the backlog it carried while the offer was running tells that it never
kept pace.

Both figures are counted in rows rather than bytes or offsets, because rows are
what the corpus froze and what the tally counts, so the backlog is the same
quantity on either side of the subtraction.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class KeepupSample:
    """Offered against committed at one instant, with the rates into it."""

    at_ms: int
    offered_rows: int
    committed_rows: int
    backlog_rows: int
    offered_rate: float | None
    committed_rate: float | None


def make_sample(at_ms: int, offered_rows: int, committed_rows: int, previous: KeepupSample | None) -> KeepupSample:
    """One sample, with rates differenced against the sample before it.

    Rates are over the gap to ``previous`` rather than since the epoch: an
    average from the epoch converges and stops reacting, and the point of the
    series is to show the moment the committed rate falls below the offered
    one. The first sample of a run has no gap and so no rates.
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
        # An at-least-once writer can commit more rows than were offered, and a
        # negative backlog is not a backlog: the duplication is exactness's to
        # report, and the floor the gate reads must not go below empty.
        backlog_rows=max(offered_rows - committed_rows, 0),
        offered_rate=offered_rate,
        committed_rate=committed_rate,
    )


def keepup_summary(
    samples: list[KeepupSample],
    offer_end_ms: int | None,
    drained_ms: int | None,
) -> dict[str, object]:
    """The keep-up scalars a run is reported by.

    ``absorbed_at_offer_end`` is read at the instant the offer stopped, not at
    the end of the run, because everything after that instant is drain: given
    long enough every fleet absorbs the whole offer, and the fraction only
    distinguishes fleets while rows are still arriving.
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
