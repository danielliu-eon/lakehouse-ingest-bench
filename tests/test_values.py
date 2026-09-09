from datetime import datetime
from pathlib import Path
from typing import cast

import fastavro
import numpy as np

from ingest_bench.corpus import columns as c
from ingest_bench.corpus import values as v

SCHEMAS = Path(__file__).resolve().parents[1] / "workloads" / "schemas"
COLUMNS = c.load_schema(SCHEMAS / "events.json")
EPOCH_MS = 1_767_225_600_000  # 2026-01-01T00:00:00Z
INTERVAL_MS = 1000


def _block(seed: int = 7, batch: int = 3, start: int = 0, rows: int = 64) -> v.RowBlock:
    cdf = v.zipf_cdf(64, 1.0)
    keys = v.draw_partition_keys(seed, batch, start, rows, cdf)
    width = v.calibrate_payload_width(seed, 256, COLUMNS)
    return v.build_row_block(seed, batch, start, keys, width, EPOCH_MS + batch * INTERVAL_MS, INTERVAL_MS, COLUMNS)


def test_ids_are_batch_block_plus_position() -> None:
    block = _block(batch=3, start=10, rows=5)
    assert block.ids.tolist() == [3 * c.ID_BLOCK + 10 + i for i in range(5)]


def test_bit_identical_regeneration_and_position_independence() -> None:
    whole = _block(rows=64)
    again = _block(rows=64)
    assert whole.avro_records_bytes() == again.avro_records_bytes()
    tail = _block(start=32, rows=32)
    assert whole.avro_records()[32:] == tail.avro_records()


def test_records_decode_with_fastavro_under_the_published_schema() -> None:
    block = _block(rows=16)
    schema = fastavro.parse_schema(c.avro_schema(COLUMNS))
    data = block.avro_records_bytes()
    offsets = np.concatenate([[0], np.cumsum(block.encoded_sizes)])
    for i in range(16):
        import io

        row = cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(data[offsets[i] : offsets[i + 1]]), schema))
        assert row["id"] == block.ids[i]
        assert row["partition_key"] == v.partition_label(int(block.partition_keys[i]))
        drawn = cast(int, block.values["event_time"][i])
        assert EPOCH_MS + 3 * INTERVAL_MS <= drawn < EPOCH_MS + 4 * INTERVAL_MS
        # fastavro renders `timestamp-millis` as an aware datetime, and the
        # instant it renders is the drawn epoch millisecond with nothing finer
        # under it — which is what the engines have to commit unchanged.
        event_time = cast(datetime, row["event_time"])
        assert round(event_time.timestamp() * 1000) == drawn
        assert event_time.microsecond % 1000 == 0


def test_calibration_lands_within_two_percent() -> None:
    width = v.calibrate_payload_width(7, 256, COLUMNS)
    block = _block(rows=4096)
    mean = block.encoded_sizes.mean()
    assert abs(mean - 256) / 256 < 0.02
    assert width > 0


def test_zipf_cdf_and_key_draw() -> None:
    cdf = v.zipf_cdf(64, 1.0)
    assert cdf.shape == (64,) and abs(cdf[-1] - 1.0) < 1e-12 and np.all(np.diff(cdf) > 0)
    keys = v.draw_partition_keys(7, 0, 0, 100_000, cdf)
    assert keys.min() >= 0 and keys.max() < 64
    share = np.bincount(keys, minlength=64) / 100_000
    assert 0.17 < share[0] < 0.23  # 1/H(64) ~ 0.21 for alpha 1
    assert np.array_equal(keys, v.draw_partition_keys(7, 0, 0, 100_000, cdf))
    uniform = v.draw_partition_keys(7, 0, 0, 100_000, v.zipf_cdf(64, 0.0))
    assert np.bincount(uniform, minlength=64).min() > 1000


def test_column_strings_for_sidecars() -> None:
    block = _block(rows=8)
    users = v.column_strings(block, "user_id")
    assert len(users) == 8 and all(s.startswith("u-") for s in users)
    assert v.column_strings(block, "partition_key") == [v.partition_label(int(k)) for k in block.partition_keys]
