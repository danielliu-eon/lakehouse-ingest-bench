# SPDX-License-Identifier: Apache-2.0
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest
from pyiceberg.io import FileIO
from pyiceberg.manifest import ManifestEntry, ManifestFile
from pyiceberg.table.metadata import TableMetadata

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.scorer import snapshots, tally
from ingest_bench.table import create

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


def _snapshot_manifests(meta: TableMetadata, snapshot_id: int, io: FileIO) -> list[ManifestFile]:
    snapshot = next(s for s in meta.snapshots if s.snapshot_id == snapshot_id)
    return snapshot.manifests(io)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    p = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=300KB", "duration_s=3", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("c"))
    generate.generate(p, out, seed=4, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(p)))


def _props(tmp_path: Path) -> dict[str, str]:
    return {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}


def _rows_of(record: generate.BatchRecord, meta: metadata.CorpusMetadata) -> pa.Table:
    # Only id and partition_key carry truth; other columns are filled with constants of the right type.
    n = record.rows
    ids = np.arange(record.id_min, record.id_max + 1, dtype=np.int64)
    cols: dict[str, pa.Array] = {"id": pa.array(ids, pa.int64()), "partition_key": pa.array(["p00001"] * n)}
    for name, t in meta.iceberg_types.items():
        if name in cols:
            continue
        cols[name] = {
            "long": pa.array(np.zeros(n, np.int64)),
            "string": pa.array(["x"] * n),
            "double": pa.array(np.zeros(n)),
            "boolean": pa.array([True] * n),
            "timestamp": pa.array(np.zeros(n, np.int64)).cast(pa.timestamp("us")),
            "binary": pa.array([b"x"] * n, pa.binary()),
        }[t]
    order = meta.field_names()
    schema = pa.schema([pa.field(name, cols[name].type, nullable=False) for name in order])
    return pa.table({name: cols[name] for name in order}).cast(schema)


def test_tally_prefix_and_violations(corpus: metadata.CorpusMetadata) -> None:
    records = metadata.read_manifest(corpus.uri)
    t = tally.BatchTally(records, corpus.p)
    assert t.prefix() == -1
    t.add_ids(np.arange(records[1].id_min, records[1].id_max + 1))
    assert t.complete(1) and not t.complete(0) and t.prefix() == -1
    t.add_ids(np.arange(records[0].id_min, records[0].id_max + 1))
    assert t.prefix() == 1
    half = records[2]
    t.add_ids(np.arange(half.id_min, half.id_min + half.rows // 2))
    assert t.prefix() == 1 and t.committed_rows() == records[0].rows + records[1].rows + half.rows // 2
    t.add_ids(np.arange(half.id_min + half.rows // 2, half.id_max + 1))
    t.add_ids(np.array([half.id_min]))  # a duplicate row
    assert t.prefix() == 2 and t.covered(2) and not t.complete(2)
    kinds = {v["batch"]: v["kind"] for v in t.violations()}
    assert kinds == {2: "duplication"}
    with pytest.raises(ValueError, match="outside"):
        t.add_ids(np.array([99 * c.ID_BLOCK]))


def test_corruption_is_a_checksum_mismatch_at_equal_count(corpus: metadata.CorpusMetadata) -> None:
    records = metadata.read_manifest(corpus.uri)
    t = tally.BatchTally(records, corpus.p)
    ids = np.arange(records[0].id_min, records[0].id_max + 1)
    ids[0] = ids[1]  # same count, wrong content
    t.add_ids(ids)
    assert t.violations()[0]["kind"] == "corruption"


def test_snapshots_and_added_files_from_a_real_table(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.s", corpus, create.parse_partition("unpartitioned"), {})
    table.append(_rows_of(records[0], corpus))
    table.append(_rows_of(records[1], corpus))
    meta = snapshots.read_metadata(snapshots.load_table(props, "bench.s"))
    snaps = snapshots.snapshots_in_order(meta)
    assert len(snaps) == 2 and all(s.operation == "append" for s in snaps)
    assert snaps[0].timestamp_ms <= snaps[1].timestamp_ms and snaps[1].parent_id == snaps[0].snapshot_id
    files = snapshots.added_files(meta, snaps[1].snapshot_id, table.io)
    assert len(files) >= 1 and sum(f.record_count for f in files) == records[1].rows
    ids = np.concatenate([tally.read_id_column(f.path, f.file_format) for f in files])
    t = tally.BatchTally(records, corpus.p)
    t.add_ids(ids)
    assert t.complete(1) and not t.complete(0)


def test_chunked_accumulation_matches_one_pass(
    corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The checksum is accumulated through float64 bincount weights, so the chunk
    # size is what keeps it exact; a batch here fits one chunk at the real size.
    records = metadata.read_manifest(corpus.uri)
    monkeypatch.setattr(tally, "_CHUNK_ROWS", 97)
    t = tally.BatchTally(records, corpus.p)
    ids = np.arange(records[0].id_min, records[0].id_max + 1)
    t.add_ids(ids)
    assert t.complete(0) and t.prefix() == 0 and t.violations() == []
    assert int(t.sum_mod[0]) == records[0].checksum and int(t.counts[0]) == records[0].rows


def test_added_files_parses_only_the_manifest_the_snapshot_wrote(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A snapshot's manifest list carries every live manifest, so parsing them
    # all would make the score loop quadratic in the number of commits.
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.m", corpus, create.parse_partition("unpartitioned"), {})
    for record in records:
        table.append(_rows_of(record, corpus))
    meta = snapshots.read_metadata(snapshots.load_table(props, "bench.m"))
    last = snapshots.snapshots_in_order(meta)[-1]
    assert len(_snapshot_manifests(meta, last.snapshot_id, table.io)) == len(records)

    parsed: list[str] = []
    unwrapped = ManifestFile.fetch_manifest_entry

    def counting(self: ManifestFile, io: FileIO, discard_deleted: bool = True) -> list[ManifestEntry]:
        parsed.append(self.manifest_path)
        return unwrapped(self, io, discard_deleted=discard_deleted)

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", counting)
    files = snapshots.added_files(meta, last.snapshot_id, table.io)
    assert len(parsed) == 1
    assert sum(f.record_count for f in files) == records[-1].rows
