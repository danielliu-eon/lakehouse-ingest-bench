# SPDX-License-Identifier: Apache-2.0
"""Decide whether a live run should continue.

Check scorer liveness before judging lag or a rising backlog floor. A stale
sample indicates missing measurements even if the last summary looked healthy.
Window minima distinguish persistent backlog growth from normal commit cycles.
Keep UNDERSIZED (a capacity result) separate from VOID (no valid measurement).
"""

from __future__ import annotations

from collections.abc import Mapping

from ingest_bench.scorer.keepup import KeepupSample

PASS = "PASS"
UNDERSIZED = "UNDERSIZED"
VOID = "VOID"

_FLOOR_WINDOWS = 3


def _backlog_floors(samples: list[KeepupSample], *, now_ms: int, window_s: int, windows: int) -> list[int | None]:
    """Return minimum backlog in each recent window, newest first.

    An empty window yields ``None`` because missing samples do not prove an
    empty backlog.
    """
    step_ms = window_s * 1000
    floors: list[int | None] = []
    for index in range(windows):
        upper_ms = now_ms - index * step_ms
        lower_ms = upper_ms - step_ms
        backlogs = [sample.backlog_rows for sample in samples if lower_ms <= sample.at_ms < upper_ms]
        floors.append(min(backlogs, default=None))
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
    stale_after_s: int,
) -> tuple[str, str]:
    """Return the live verdict and its reason.

    Require recent scorer samples. Apply the lag limit after adaptation, and
    check for a rising positive backlog floor across three populated windows.
    """
    if freshness_partial["aborted"]:
        return VOID, "the freshness reader aborted, so the run has no measurement to judge"

    newest_ms = max((sample.at_ms for sample in keepup_samples), default=None)
    if newest_ms is None:
        return VOID, "the scorer has published no keep-up sample, so nothing is measuring the run"
    age_s = (now_ms - newest_ms) / 1000
    if age_s > stale_after_s:
        return VOID, (
            f"the scorer's newest keep-up sample is {age_s:.0f}s old, past the {stale_after_s}s staleness bound, "
            "so nothing is measuring the run"
        )

    lag = freshness_partial["lag_s"]
    if lag is not None and not isinstance(lag, (int, float)):
        raise TypeError(f"freshness lag must be a number or None, and arrived as {type(lag).__name__}")
    max_bound_s = 2 * bound_s
    adapted = now_ms >= epoch_ms + adaptation_s * 1000
    if adapted and lag is not None and lag > max_bound_s:
        return UNDERSIZED, f"lag {lag:.1f}s is past the {max_bound_s:.0f}s max bound {adaptation_s}s after the epoch"

    recent, middle, oldest = _backlog_floors(keepup_samples, now_ms=now_ms, window_s=window_s, windows=_FLOOR_WINDOWS)
    if recent is None:
        # Keep this guard if the staleness limit is increased beyond the window.
        return VOID, f"no keep-up sample in the last {window_s}s, so the backlog floor cannot be read"
    if middle is not None and oldest is not None and recent > middle > oldest > 0:
        return UNDERSIZED, f"backlog floor rose {oldest} -> {middle} -> {recent} rows over three {window_s}s windows"

    if now_ms < epoch_ms:
        return PASS, f"waiting for epoch in {(epoch_ms - now_ms) / 1000:.1f}s, backlog floor {recent} rows not rising"
    lag_figure = f"lag {lag:.1f}s" if lag is not None else "no lag sample yet"
    return PASS, f"{lag_figure} within the {max_bound_s:.0f}s max bound, backlog floor {recent} rows not rising"
