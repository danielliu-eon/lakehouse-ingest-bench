import json
from pathlib import Path

import pytest

from ingest_bench.corpus import columns as c

SCHEMAS = Path(__file__).resolve().parents[1] / "workloads" / "schemas"


def test_events_schema_loads_with_reserved_fields_first() -> None:
    cols = c.load_schema(SCHEMAS / "events.json")
    assert [col.name for col in cols[:2]] == ["id", "partition_key"]
    assert c.role_column(cols, c.ROLE_EVENT_TIME).name == "event_time"
    assert c.role_column(cols, c.ROLE_ENTITY).name == "event_id"
    assert c.role_column(cols, c.ROLE_SUM_MEASURE).name == "amount_micros"
    assert c.role_column(cols, c.ROLE_PAYLOAD).name == "payload"


def test_avro_schema_types() -> None:
    cols = c.load_schema(SCHEMAS / "events.json")
    schema = c.avro_schema(cols)
    fields = {f["name"]: f["type"] for f in schema["fields"]}  # type: ignore[attr-defined]
    assert fields["id"] == "long"
    assert fields["partition_key"] == "string"
    assert fields["event_time"] == {"type": "long", "logicalType": "timestamp-micros"}
    assert fields["payload"] == "bytes"
    assert schema["name"] == "event"


@pytest.mark.parametrize(
    "raw, message",
    [
        ({"name": "ts", "kind": "timestamp", "cardinality": 10}, "only the event_time column may be"),
        ({"name": "b", "kind": "blob", "cardinality": 5}, "needs a positive width"),
        (
            {"name": "x", "kind": "categorical", "alpha": 1.0, "cardinality": 2_000_000, "vocabulary": ["x"]},
            "above the",
        ),
        ({"name": "n", "kind": "integer", "cardinality": 1000, "minimum": 5, "maximum": 5}, "numeric range holding"),
        ({"name": "f", "kind": "boolean", "cardinality": 3}, "must be 1 or 2"),
        ({"name": "id", "kind": "integer", "cardinality": 3, "minimum": 0, "maximum": 9}, "collides with a reserved"),
    ],
)
def test_rejections(raw: dict[str, object], message: str) -> None:
    base = list(c.load_schema(SCHEMAS / "events.json")[2:])
    with pytest.raises(ValueError, match=message):
        c.build_columns(tuple(base + [c.column_from_dict(raw)]))


def test_overrides_patch_one_column() -> None:
    cols = c.load_schema(SCHEMAS / "events.json")
    patched = c.apply_column_overrides(cols, {"country": {"alpha": 0.0}})
    assert next(col for col in patched if col.name == "country").alpha == 0.0
    with pytest.raises(ValueError, match="unknown column"):
        c.apply_column_overrides(cols, {"nope": {"alpha": 0.0}})


def test_iceberg_type_names() -> None:
    assert c.iceberg_type_name("long") == "long"
    assert c.iceberg_type_name({"type": "long", "logicalType": "timestamp-micros"}) == "timestamp"
    assert c.iceberg_type_name("bytes") == "binary"
    with pytest.raises(ValueError):
        c.iceberg_type_name("float")


def test_events_json_has_no_internal_vocabulary() -> None:
    text = (SCHEMAS / "events.json").read_text().lower()
    for banned in ("adevents", "eon", "maelstrom", "rise"):
        assert banned not in text
    assert json.loads(text)["name"] == "events"
