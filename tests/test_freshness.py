# SPDX-License-Identifier: Apache-2.0
import numpy as np

from ingest_bench.corpus.generate import BatchRecord
from ingest_bench.scorer import exactness, freshness, gate, keepup, tally

E = 1_000_000  # epoch ms


def _emit(n: int) -> dict[int, int]:
    return {k: E + k * 1000 + 100 for k in range(n)}


def test_lag_series_is_time_weighted_step_function() -> None:
    obs = [freshness.Observation(E + 5_000, E + 5_050, 3), freshness.Observation(E + 65_000, E + 65_020, 60)]
    series = freshness.lag_series(obs, _emit(61), E, E + 70_000, 1000, "timestamp_ms")
    assert series[0]["lag_s"] == 0.0 and series[4]["prefix"] == -1
    assert series[5]["prefix"] == 3 and series[5]["lag_s"] == (E + 5_000 - (E + 3_100)) / 1000
    # Between commits the lag grows one second per grid step.
    assert series[30]["lag_s"] == series[5]["lag_s"] + 25
    assert series[70]["prefix"] == 60
    q = freshness.lag_quantiles(series)
    p50_s, p95_s, max_s = q["p50_s"], q["p95_s"], q["max_s"]
    assert p50_s is not None and p95_s is not None and max_s is not None
    assert max_s >= p95_s >= p50_s


def test_lagging_lane_is_not_complete() -> None:
    # Half of batch 1 landed early, the rest much later: the prefix stays at 0 until then.
    records = [
        BatchRecord(
            k,
            k * 1000,
            100,
            k << 32,
            (k << 32) + 99,
            ((k << 32) * 2 + 99) * 100 // 2 % 1_000_000_007,
            0,
            0,
            "",
            "",
            {},
        )
        for k in range(3)
    ]
    t = tally.BatchTally(records, 1_000_000_007)
    t.add_ids(np.arange(0, 100))
    t.add_ids(np.arange(1 << 32, (1 << 32) + 50))
    assert t.prefix() == 0
    t.add_ids(np.arange((1 << 32) + 50, (1 << 32) + 100))
    assert t.prefix() == 1


def test_warmup_window_and_verdict() -> None:
    emit = _emit(300)
    # Cold start: nothing committed for 100 s, then a commit every 10 s that catches up to the previous second.
    obs = [freshness.Observation(E + t * 1000, E + t * 1000, t - 1) for t in range(100, 301, 10)]
    result = freshness.freshness_result(
        obs, emit, epoch_ms=E, end_ms=E + 300_000, last_batch=299, warmup_s=120, bound_s=60
    )
    assert result.drained
    window_max_s, window_p95_s = result.window["max_s"], result.window["p95_s"]
    assert window_max_s is not None and window_p95_s is not None
    assert result.full["max_s"] == 99.0 and window_max_s < 11
    assert window_p95_s <= 60 and result.verdict
    # Every commit here lands 0.9 s after its batch's last ack, and the
    # per-commit minimum says so rather than reporting the epoch's own zero.
    assert result.min_lag_s == 0.9
    strict = freshness.freshness_result(
        obs, emit, epoch_ms=E, end_ms=E + 300_000, last_batch=299, warmup_s=0, bound_s=60
    )
    strict_p95_s = strict.full["p95_s"]
    assert strict_p95_s is not None
    assert strict_p95_s > 60 and strict.verdict is False


def test_clock_skew_flag() -> None:
    emit = _emit(5)
    ok = [freshness.Observation(E + 4_500, E + 4_500, 3)]
    bad = [freshness.Observation(E + 2_000, E + 2_000, 3)]  # visible 1.1 s before its last ack
    assert not freshness.clock_skew_suspected(ok, emit)
    assert freshness.clock_skew_suspected(bad, emit)
    assert freshness.min_observation_lag_s(ok, emit) == 1.4
    r = freshness.freshness_result(bad, emit, epoch_ms=E, end_ms=E + 6_000, last_batch=4, warmup_s=0, bound_s=180)
    assert r.clock_skew_suspected and not r.drained
    # The skew flag is this figure being negative, so it cannot disagree with it.
    assert r.min_lag_s == -1.1
    assert freshness.min_observation_lag_s([], emit) is None


def test_keepup_summary() -> None:
    samples: list[keepup.KeepupSample] = []
    prev = None
    for i in range(10):
        prev = keepup.make_sample(
            E + i * 1000, offered_rows=1000 * (i + 1), committed_rows=800 * (i + 1), previous=prev
        )
        samples.append(prev)
    assert samples[1].offered_rate == 1000.0 and samples[1].committed_rate == 800.0
    s = keepup.keepup_summary(samples, offer_end_ms=E + 9000, drained_ms=E + 12000)
    assert s["absorbed_at_offer_end"] == 0.8 and s["drain_s"] == 3.0 and s["backlog_rows_max"] == 2000


def test_exactness_result() -> None:
    records = [
        BatchRecord(k, 0, 10, k << 32, (k << 32) + 9, ((k << 32) * 2 + 9) * 10 // 2 % 1_000_000_007, 0, 0, "", "", {})
        for k in range(2)
    ]
    t = tally.BatchTally(records, 1_000_000_007)
    t.add_ids(np.arange(0, 10))
    t.add_ids(np.arange(1 << 32, (1 << 32) + 12))  # two extra rows
    r = exactness.exactness_result(t, offered_batches={0, 1})
    assert r["exact"] is False and r["duplicate_rows"] == 2 and r["loss_rows"] == 0 and r["duplicate_ppm"] == 100_000.0


def test_gate() -> None:
    samples = [
        keepup.KeepupSample(E + i * 1000, 1000 * i, 1000 * i - min(i, 5) * 100, min(i, 5) * 100, None, None)
        for i in range(1, 400)
    ]
    flat = gate.gate_verdict(
        {"lag_s": 30.0, "aborted": False},
        samples,
        bound_s=180,
        adaptation_s=120,
        window_s=60,
        now_ms=E + 400_000,
        epoch_ms=E,
    )
    assert flat[0] == "PASS"
    late = gate.gate_verdict(
        {"lag_s": 400.0, "aborted": False},
        samples,
        bound_s=180,
        adaptation_s=120,
        window_s=60,
        now_ms=E + 400_000,
        epoch_ms=E,
    )
    assert late[0] == "UNDERSIZED"
    rising = [keepup.KeepupSample(E + i * 1000, 1000 * i, 700 * i, 300 * i, None, None) for i in range(1, 400)]
    assert (
        gate.gate_verdict(
            {"lag_s": 30.0, "aborted": False},
            rising,
            bound_s=180,
            adaptation_s=120,
            window_s=60,
            now_ms=E + 400_000,
            epoch_ms=E,
        )[0]
        == "UNDERSIZED"
    )
    assert (
        gate.gate_verdict(
            {"lag_s": None, "aborted": True},
            samples,
            bound_s=180,
            adaptation_s=120,
            window_s=60,
            now_ms=E + 400_000,
            epoch_ms=E,
        )[0]
        == "VOID"
    )
