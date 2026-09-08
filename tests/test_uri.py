from pathlib import Path

import pytest

from ingest_bench import uri


def test_unsupported_scheme_is_refused() -> None:
    for bad in ("gcs://bucket/corpus", "http://host/corpus", "s3a://bucket/corpus"):
        with pytest.raises(ValueError, match="unsupported URI scheme"):
            uri.filesystem_for(bad)


def test_file_scheme_round_trips(tmp_path: Path) -> None:
    target = f"file://{tmp_path}/nested/corpus.json"
    uri.write_text(target, "{}")
    assert uri.read_text(target) == "{}"
    assert (tmp_path / "nested" / "corpus.json").read_text() == "{}"
    assert uri.exists(target)
    assert not uri.is_remote(target)


def test_local_paths_are_not_mistaken_for_schemes(tmp_path: Path) -> None:
    assert uri.filesystem_for(str(tmp_path))[1] == str(tmp_path)
    assert uri.filesystem_for("corpus/smoke-89a47407")[1] == "corpus/smoke-89a47407"
    assert uri.is_remote("s3://bucket/corpus") and uri.is_remote("gs://bucket/corpus")
    assert not uri.is_remote(str(tmp_path))


def test_join_and_listdir(tmp_path: Path) -> None:
    root = str(tmp_path)
    assert uri.join(root, "batches/", "/000001.bin.zst") == f"{root}/batches/000001.bin.zst"
    uri.write_bytes(uri.join(root, "batches", "000001.bin.zst"), b"x")
    uri.write_bytes(uri.join(root, "batches", "000000.bin.zst"), b"y")
    assert uri.listdir(uri.join(root, "batches")) == ["000000.bin.zst", "000001.bin.zst"]
    assert uri.read_bytes(uri.join(root, "batches", "000000.bin.zst")) == b"y"
    assert not uri.exists(uri.join(root, "batches", "000002.bin.zst"))
