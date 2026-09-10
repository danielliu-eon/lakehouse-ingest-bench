# SPDX-License-Identifier: Apache-2.0
from pathlib import Path

import pytest

from ingest_bench.corpus import preset as p

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


def test_parse_quantity() -> None:
    assert p.parse_quantity("100MB") == 100_000_000
    assert p.parse_quantity("5MB") == 5_000_000
    assert p.parse_quantity("64MiB") == 64 * 1024 * 1024
    assert p.parse_quantity("2GB") == 2_000_000_000
    assert p.parse_quantity("12345") == 12345
    with pytest.raises(ValueError):
        p.parse_quantity("5 bananas")


def test_smoke_preset_shape() -> None:
    preset = p.load_preset("smoke", workloads_dir=WORKLOADS)
    assert preset.schema_name == "events"
    assert preset.offered_bytes_per_s == 5_000_000
    assert preset.batch_count == 300
    assert preset.batch_bytes == 5_000_000
    assert preset.kafka_key_columns == ("user_id", "partition_key")
    assert [c.name for c in preset.columns[:2]] == ["id", "partition_key"]


@pytest.mark.parametrize(
    "name", ["events-100mbs-uniform", "events-100mbs-skew", "events-600mbs-uniform", "events-600mbs-skew"]
)
def test_shipped_presets_load(name: str) -> None:
    preset = p.load_preset(name, workloads_dir=WORKLOADS)
    assert preset.duration_s == 3600
    assert preset.partition_count == 512
    assert preset.alpha in (0.0, 1.0)


def test_set_overrides_change_hash_and_values() -> None:
    base = p.load_preset("smoke", workloads_dir=WORKLOADS)
    changed = p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["alpha=0.5", "duration_s=30"])
    assert changed.alpha == 0.5 and changed.duration_s == 30 and changed.batch_count == 30
    assert p.corpus_hash(base) != p.corpus_hash(changed)
    assert p.corpus_dir_name(changed).startswith("smoke-")
    with pytest.raises(ValueError, match="unknown preset key"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["colour=blue"])


def test_column_override_via_set() -> None:
    preset = p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["column_overrides.country.alpha=0.0"])
    assert next(c for c in preset.columns if c.name == "country").alpha == 0.0


def test_hash_is_stable_and_ignores_nothing_relevant() -> None:
    a = p.load_preset("smoke", workloads_dir=WORKLOADS)
    b = p.load_preset(str(WORKLOADS / "presets" / "smoke.yaml"), workloads_dir=WORKLOADS)
    assert p.corpus_hash(a) == p.corpus_hash(b)
    assert len(p.corpus_hash(a)) == 8


def test_key_columns_must_exist() -> None:
    with pytest.raises(ValueError, match="kafka_key_columns"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["kafka_key_columns=[nope]"])


def test_key_column_must_be_a_string_column() -> None:
    with pytest.raises(ValueError, match="string"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["kafka_key_columns=[id]"])


def test_an_event_time_finer_than_its_batch_window_is_refused() -> None:
    """A batch window of N milliseconds supports at most N distinct millisecond
    timestamps, regardless of a larger declared cardinality.
    """
    at_the_window = p.load_preset(
        "smoke", workloads_dir=WORKLOADS, overrides=["column_overrides.event_time.cardinality=1000"]
    )
    assert next(c for c in at_the_window.columns if c.name == "event_time").cardinality == 1000
    with pytest.raises(ValueError, match="event_time"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["column_overrides.event_time.cardinality=1001"])
    # The window is a preset key, so a batch long enough for the declaration
    # is the other half of the answer.
    p.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["column_overrides.event_time.cardinality=2000", "batch_interval_ms=2000"],
    )
