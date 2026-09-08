"""The live verdict that stops a run that cannot pass.

A run lasts hours, and most of the runs in a sizing sweep are undersized by
construction. Waiting for the full duration to learn that costs the sweep more
than it costs to judge early, so the gate reads the two signals that separate a
fleet which is merely warming up from one which will never catch up: the lag
right now, and whether the backlog it carries has a rising floor.

The floor is what makes the second signal trustworthy. Backlog is sawtoothed —
it fills between commits and empties at each one — so its instantaneous value
says almost nothing. The minimum over a window is the debt the fleet failed to
clear, and a minimum that rises window over window is a fleet falling behind
however busy each individual commit looked.

UNDERSIZED and VOID are kept apart on purpose: the first is an answer about the
fleet, the second is the absence of an answer, and a sweep that conflates them
reports missing measurements as capacity limits.
"""

from __future__ import annotations

from collections.abc import Mapping

from ingest_bench.scorer.keepup import KeepupSample

PASS = "PASS"
UNDERSIZED = "UNDERSIZED"
VOID = "VOID"

_FLOOR_WINDOWS = 3


def _backlog_floors(samples: list[KeepupSample], *, now_ms: int, window_s: int, windows: int) -> list[int | None]:
    """The unclearable backlog in each of the last ``windows`` windows, newest first.

    A window with no samples yields None rather than zero: an empty window is a
    reader that stopped, and treating it as an empty backlog would clear the
    rising-floor signal exactly when the evidence went missing.
    """
    step_ms = window_s * 1000
    floors: list[int | None] = []
    for index in range(windows):
        upper_ms = now_ms - index * step_ms
        lower_ms = upper_ms - step_ms
        backlogs = [sample.backlog_rows for sample in samples if lower_ms <= sample.at_ms < upper_ms]
        floors.append(min(backlogs) if backlogs else None)
    return floors


def gate_verdict(
    freshness_partial: Mapping[str, object],
    keepup_samples: list[KeepupSample],
    *,
    bound_s: float,
    adaptation_s: int,
    window_s: int,
    now_ms: int,
    epoch_ms: int,
) -> tuple[str, str]:
    """Judge a run in flight, with the figure that decided it.

    Nothing is judged undersized before the adaptation period is up. A fleet
    scaling out to meet its first rows is lagging for a reason that will pass,
    and a gate that fired there would report every cold start as a capacity
    limit.
    """
    if freshness_partial["aborted"]:
        return VOID, "the freshness reader aborted, so the run has no measurement to judge"

    lag = freshness_partial["lag_s"]
    if lag is not None and not isinstance(lag, (int, float)):
        raise TypeError(f"freshness lag must be a number or None, and arrived as {type(lag).__name__}")
    max_bound_s = 2 * bound_s
    adapted = now_ms >= epoch_ms + adaptation_s * 1000
    if adapted and lag is not None and lag > max_bound_s:
        return UNDERSIZED, f"lag {lag:.1f}s is past the {max_bound_s:.0f}s max bound {adaptation_s}s after the epoch"

    recent, middle, oldest = _backlog_floors(keepup_samples, now_ms=now_ms, window_s=window_s, windows=_FLOOR_WINDOWS)
    if recent is not None and middle is not None and oldest is not None and recent > middle > oldest > 0:
        return UNDERSIZED, f"backlog floor rose {oldest} -> {middle} -> {recent} rows over three {window_s}s windows"

    lag_figure = f"lag {lag:.1f}s" if lag is not None else "no lag sample yet"
    floor_figure = f"backlog floor {recent} rows" if recent is not None else "no backlog samples in the last window"
    return PASS, f"{lag_figure} within the {max_bound_s:.0f}s max bound, {floor_figure} not rising"
