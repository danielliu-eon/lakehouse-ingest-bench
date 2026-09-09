# SPDX-License-Identifier: Apache-2.0
"""How stale the table is, sampled over time rather than at each commit.

Freshness is one number about a whole run, and the honest one is a quantile of
the lag a reader would have seen at an arbitrary instant. That is not the same
as a quantile over commits: a fleet that commits ten times in one second and
then stalls for five minutes looks excellent per commit and terrible to a
reader. Sampling the lag on a fixed grid weights every second of the run
equally, which is what makes the p95 mean what the bound claims.

The lag at time ``t`` is measured from the emit time of the newest batch whose
rows are all present — the tally's prefix — so a batch that is half in the
table has not arrived. Both clocks a run can be judged on are supported: the
table's own commit timestamps, and the wall time at which the scorer first saw
each commit. The first is what a reader of the table sees and is the default;
the second is immune to a writer whose clock disagrees with the producer's,
which is the fault ``clock_skew_suspected`` exists to name.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

TIMESTAMP_CLOCK = "timestamp_ms"
FIRST_SEEN_CLOCK = "first_seen_ms"


@dataclass(frozen=True)
class Observation:
    """The completeness watermark at one commit, on both clocks.

    ``prefix`` is the tally's prefix as of this commit: the largest batch whose
    rows, and every earlier batch's rows, are in the table.
    """

    timestamp_ms: int
    first_seen_ms: int
    prefix: int


def lag_series(
    observations: list[Observation],
    emit_ms: dict[int, int],
    t0_ms: int,
    end_ms: int,
    grid_ms: int,
    clock: str,
) -> list[dict[str, int | float | None]]:
    """L(t) sampled on a fixed grid.

    S(t) is a right-continuous step function that only changes when a snapshot
    becomes visible, so sampling the steps on a uniform grid — rather than
    recording one sample per commit — is what makes the p95 time-weighted: a
    prefix that sits stalled for five minutes must dominate the quantile over
    one that advanced ten times in a second.

    Before any batch is complete the lag is measured from t0: an engine that
    has committed nothing has been late since the offer began.

    A prefix whose emit time is absent from the publish log yields
    lag_s = None instead of raising: the gap is a coverage failure the caller
    records (see missing_emit_prefixes), and a raise here would happen after
    hours of measurement but before any artifact is written.
    """
    if clock not in (TIMESTAMP_CLOCK, FIRST_SEEN_CLOCK):
        raise ValueError(f"unknown observation clock: {clock}")
    steps = sorted(
        ((obs.timestamp_ms if clock == TIMESTAMP_CLOCK else obs.first_seen_ms, obs) for obs in observations),
        key=lambda pair: pair[0],
    )
    series: list[dict[str, int | float | None]] = []
    index = 0
    current: Observation | None = None
    grid_points = list(range(t0_ms, end_ms + 1, grid_ms))
    if not grid_points or grid_points[-1] != end_ms:
        grid_points.append(end_ms)
    for at_ms in grid_points:
        while index < len(steps) and steps[index][0] <= at_ms:
            current = steps[index][1]
            index += 1
        prefix = current.prefix if current is not None else -1
        reference_ms = emit_ms.get(prefix) if prefix >= 0 else t0_ms
        series.append(
            {
                "at_ms": at_ms,
                "prefix": prefix,
                "lag_s": (at_ms - reference_ms) / 1000 if reference_ms is not None else None,
            }
        )
    return series


def missing_emit_prefixes(series: list[dict[str, int | float | None]]) -> list[int]:
    """Prefixes sampled without an emit time — publish-log coverage gaps as
    seen from the table side. Non-empty means the run cannot be scored."""
    gaps = {int(row["prefix"]) for row in series if row["lag_s"] is None and row["prefix"] is not None}
    return sorted(gaps)


def _no_quantiles() -> dict[str, float | None]:
    return {"p50_s": None, "p95_s": None, "p99_s": None, "max_s": None}


def lag_quantiles(series: list[dict[str, int | float | None]]) -> dict[str, float | None]:
    lags: list[float] = []
    for row in series:
        lag = row["lag_s"]
        # One unlaggable sample voids the quantiles: computing them over the
        # remaining samples would report a flattering number for a run whose
        # coverage failure already makes it unscorable.
        if lag is None:
            return _no_quantiles()
        lags.append(float(lag))
    if not lags:
        return _no_quantiles()
    arr = np.array(lags, dtype=np.float64)
    return {
        "p50_s": float(np.percentile(arr, 50)),
        "p95_s": float(np.percentile(arr, 95)),
        "p99_s": float(np.percentile(arr, 99)),
        "max_s": float(arr.max()),
    }


def _observation_lags_s(observations: list[Observation], emit_ms: dict[int, int]) -> list[float]:
    """The lag at each commit that advanced the prefix, on the table's clock.

    This is one lag per commit rather than one per grid instant, so it is not
    what the bound is judged on. What it is for is the smallest value in it:
    the closest a batch came to being queryable before the producer had
    finished acking it.

    A commit whose prefix has no emit time is skipped rather than raising. The
    gap is a coverage failure the caller already records, and it must not turn
    a scored run into an exception hours in.
    """
    return [
        (obs.timestamp_ms - emit_ms[obs.prefix]) / 1000
        for obs in observations
        if obs.prefix >= 0 and obs.prefix in emit_ms
    ]


def min_observation_lag_s(observations: list[Observation], emit_ms: dict[int, int]) -> float | None:
    """The smallest per-commit lag, or None when no commit advanced the prefix.

    Negative is the tell: a batch cannot be queryable before it was acked in
    any single frame of reference, so a negative minimum means the table's
    clock and the producer's disagree, and every figure drawn from those
    timestamps is suspect. ``clock_skew_suspected`` is exactly this figure
    being negative, so the two cannot contradict each other.
    """
    lags = _observation_lags_s(observations, emit_ms)
    return min(lags) if lags else None


def clock_skew_suspected(observations: list[Observation], emit_ms: dict[int, int]) -> bool:
    """Whether a batch was queryable before the producer finished acking it.

    Only the table's own commit timestamps can disagree with the producer's
    clock; ``first_seen_ms`` is the scorer's own reading and cannot. So the
    test is against ``timestamp_ms`` whichever clock a run is scored on.
    """
    minimum = min_observation_lag_s(observations, emit_ms)
    return minimum is not None and minimum < 0


@dataclass
class FreshnessResult:
    """The freshness verdict, and every figure it was drawn from.

    Both the windowed and the full-run quantiles are published. The window is
    what the bound is judged on, and the full run is what says how much of the
    lag the warmup hid — a run whose window passes only because its warmup
    swallowed a ten-minute cold start is a different result from one that was
    fresh throughout, and the artifact should not have to be re-derived to
    tell them apart.

    ``min_lag_s`` is the per-commit minimum, not a quantile of the grid: it is
    the skew figure, and it is negative exactly when
    ``clock_skew_suspected`` is true.
    """

    window: dict[str, float | None]
    full: dict[str, float | None]
    verdict: bool
    drained: bool
    bound_s: float
    max_bound_s: float
    warmup_s: int
    clock: str
    clock_skew_suspected: bool
    min_lag_s: float | None
    missing_emit_prefixes: list[int]


def _at_ms(row: dict[str, int | float | None]) -> int:
    at_ms = row["at_ms"]
    if at_ms is None:
        raise ValueError("a lag sample must carry the grid instant it was taken at")
    return int(at_ms)


def freshness_result(
    observations: list[Observation],
    emit_ms: dict[int, int],
    *,
    epoch_ms: int,
    end_ms: int,
    last_batch: int,
    warmup_s: int,
    bound_s: float,
    grid_ms: int = 1000,
    clock: str = TIMESTAMP_CLOCK,
) -> FreshnessResult:
    """Score one run's freshness over the measurement window.

    The window starts a warmup after the epoch because a fleet that has just
    been handed its first rows is provisioning, not lagging, and the bound is a
    claim about steady state. Draining is a separate condition rather than a
    lag sample: a run that ends with rows still outside the table has no lag to
    measure for them, and quantiles over the samples that do exist would score
    it as though those rows were never offered.
    """
    series = lag_series(observations, emit_ms, epoch_ms, end_ms, grid_ms, clock)
    window_start = epoch_ms + warmup_s * 1000
    # A warmup longer than the run itself leaves the window empty; the last
    # grid point stands in so the verdict is drawn from the run's final state
    # rather than from no samples at all.
    window = [row for row in series if _at_ms(row) >= window_start] or series[-1:]
    missing = missing_emit_prefixes(series)
    window_quantiles, full_quantiles = lag_quantiles(window), lag_quantiles(series)
    drained = bool(observations) and observations[-1].prefix == last_batch
    p95_s, max_s = window_quantiles["p95_s"], window_quantiles["max_s"]
    verdict = (
        drained
        and not missing
        and p95_s is not None
        and p95_s <= bound_s
        and max_s is not None
        and max_s <= 2 * bound_s
    )
    return FreshnessResult(
        window=window_quantiles,
        full=full_quantiles,
        verdict=verdict,
        drained=drained,
        bound_s=bound_s,
        max_bound_s=2 * bound_s,
        warmup_s=warmup_s,
        clock=clock,
        clock_skew_suspected=clock_skew_suspected(observations, emit_ms),
        min_lag_s=min_observation_lag_s(observations, emit_ms),
        missing_emit_prefixes=missing,
    )
