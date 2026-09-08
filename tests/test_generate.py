import io
import json
from pathlib import Path
from typing import cast

import fastavro
import pytest

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import frames, generate, preset

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


@pytest.fixture(scope="module")
def tiny(tmp_path_factory: pytest.TempPathFactory) -> tuple[preset.Preset, str, dict[str, object]]:
    p = preset.load_preset(
        "smoke", workloads_dir=WORKLOADS, overrides=["offered_bytes_per_s=200KB", "duration_s=6", "partition_count=8"]
    )
    out = str(tmp_path_factory.mktemp("corpus"))
    meta = generate.generate(p, out, seed=3, row_block=64)
    return p, uri.join(out, preset.corpus_dir_name(p)), meta


def test_layout_and_manifest(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, meta = tiny
    assert meta["batch_count"] == 6 and meta["generator_version"] == "4"
    manifest = [
        generate.BatchRecord.from_json(line)
        for line in uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()
    ]
    assert [r.batch for r in manifest] == list(range(6))
    assert [r.offset_ms for r in manifest] == [0, 1000, 2000, 3000, 4000, 5000]
    for r in manifest:
        assert r.encoded_bytes >= p.batch_bytes and r.encoded_bytes < p.batch_bytes + 64 * 400
        assert r.id_min == r.batch * c.ID_BLOCK and r.id_max == r.id_min + r.rows - 1
        assert r.checksum == ((r.id_min + r.id_max) * r.rows // 2) % c.P
        assert set(r.key_uris) == {"user_id", "partition_key"}
        assert uri.exists(uri.join(corpus_uri, r.uri))
    assert json.loads(uri.read_text(uri.join(corpus_uri, "schema.avsc")))["name"] == "event"


def test_frames_decode_and_match_sidecars(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    _, corpus_uri, meta = tiny
    schema = fastavro.parse_schema(json.loads(uri.read_text(uri.join(corpus_uri, "schema.avsc"))))
    record = generate.BatchRecord.from_json(uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()[2])
    data = frames.decompress(uri.read_bytes(uri.join(corpus_uri, record.uri)))
    users = [
        b.decode()
        for b in frames.iter_frames(frames.decompress(uri.read_bytes(uri.join(corpus_uri, record.key_uris["user_id"]))))
    ]
    rows = [
        cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(fr), schema)) for fr in frames.iter_frames(data)
    ]
    assert len(rows) == record.rows == len(users)
    assert [r["user_id"] for r in rows] == users
    assert rows[0]["id"] == record.id_min and rows[-1]["id"] == record.id_max


def test_partition_truth_matches_rows(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    _, corpus_uri, meta = tiny
    truth = json.loads(uri.read_text(uri.join(corpus_uri, "partition_truth.json")))
    assert sum(v["rows"] for v in truth.values()) == meta["row_count"]
    assert set(truth) == {f"p{k:05d}" for k in range(8)}
    # Published even when the corpus is too small to gate.
    assert isinstance(meta["partition_byte_share_max_relative_deviation"], float)


def test_regeneration_is_bit_identical(tmp_path: Path, tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, meta = tiny
    again = generate.generate(p, str(tmp_path), seed=3, row_block=64)
    assert again["manifest_sha256"] == meta["manifest_sha256"]
    a = uri.read_bytes(uri.join(corpus_uri, "batches", frames.batch_file_name(1)))
    b = uri.read_bytes(uri.join(str(tmp_path), preset.corpus_dir_name(p), "batches", frames.batch_file_name(1)))
    assert a == b


def test_shards_reproduce_the_whole(tmp_path: Path, tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, _ = tiny
    whole = uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()
    parts: list[str] = []
    for i in range(3):
        out = str(tmp_path / f"shard-{i}")
        generate.generate(p, out, seed=3, shard_index=i, shard_count=3, row_block=64)
        parts += uri.read_text(uri.join(out, preset.corpus_dir_name(p), "manifest.jsonl")).splitlines()
    assert sorted(parts) == sorted(whole)


def test_corpus_json_publishes_types_roles_and_gates(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    _, _, meta = tiny
    assert meta["iceberg_types"]["event_time"] == "timestamp"  # type: ignore[index]
    assert meta["iceberg_types"]["id"] == "long"  # type: ignore[index]
    assert meta["column_roles"]["sum_measure"] == "amount_micros"  # type: ignore[index]
    assert abs(float(meta["mean_encoded_row_size_relative_deviation"])) <= 0.02  # type: ignore[arg-type]
    assert meta["key_columns"] == ["user_id", "partition_key"]
    assert meta["p"] == c.P and meta["id_block"] == c.ID_BLOCK


def test_verify_batch_catches_a_flipped_byte(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, _ = tiny
    record = generate.BatchRecord.from_json(uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()[0])
    data = bytearray(uri.read_bytes(uri.join(corpus_uri, record.uri)))
    data[-1] ^= 0xFF
    keys = {k: uri.read_bytes(uri.join(corpus_uri, u)) for k, u in record.key_uris.items()}
    with pytest.raises(AssertionError, match="batch 0"):
        generate.verify_batch(record, bytes(data), keys, p.columns, {})
