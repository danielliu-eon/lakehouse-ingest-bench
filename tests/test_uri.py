from pathlib import Path

import fsspec
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


def test_read_bytes_reads_the_whole_object_rather_than_a_buffered_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Objects here are rewritten while they are read, so a read is one request.

    A caching file object pins the ETag it opened with and fails a later range
    request once the object behind it has been replaced. The producer
    republishes its publish log every few seconds and the scorer reads that log
    on every poll, so that failure would end a run. This fake refuses exactly
    the call that would take that path.
    """

    class Republished:
        def cat_file(self, path: str) -> bytes:
            return b"fresh"

        def open(self, path: str, mode: str) -> object:
            raise AssertionError("read_bytes opened a buffered file")

    monkeypatch.setattr(uri, "filesystem_for", lambda _: (Republished(), "runs/r/publish_log-0.jsonl"))
    assert uri.read_bytes("s3://runs/r/publish_log-0.jsonl") == b"fresh"


def _recording_filesystem(monkeypatch: pytest.MonkeyPatch) -> dict[str, object]:
    """Patches ``fsspec.filesystem`` and returns the kwargs the next s3 call passes."""
    calls: dict[str, object] = {}

    def fake_filesystem(protocol: str, **kwargs: object) -> object:
        calls["protocol"] = protocol
        calls.update(kwargs)
        return object()

    monkeypatch.setattr(fsspec, "filesystem", fake_filesystem)
    return calls


def test_s3_filesystem_passes_aws_region_as_client_kwarg(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "eu-west-1")
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    calls = _recording_filesystem(monkeypatch)
    uri.filesystem_for("s3://bucket/corpus")
    assert calls["client_kwargs"] == {"region_name": "eu-west-1"}
    assert calls["use_listings_cache"] is False


def test_s3_filesystem_falls_back_to_aws_default_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    calls = _recording_filesystem(monkeypatch)
    uri.filesystem_for("s3://bucket/corpus")
    assert calls["client_kwargs"] == {"region_name": "ap-south-1"}


def test_s3_filesystem_prefers_aws_region_over_aws_default_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-2")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    calls = _recording_filesystem(monkeypatch)
    uri.filesystem_for("s3://bucket/corpus")
    assert calls["client_kwargs"] == {"region_name": "us-east-2"}


def test_s3_filesystem_omits_region_when_neither_env_var_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AWS_REGION", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    calls = _recording_filesystem(monkeypatch)
    uri.filesystem_for("s3://bucket/corpus")
    assert "client_kwargs" not in calls
    assert calls["use_listings_cache"] is False


def test_s3_filesystem_combines_endpoint_and_region(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AWS_REGION", "us-east-2")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://minio:9000")
    calls = _recording_filesystem(monkeypatch)
    uri.filesystem_for("s3://bucket/corpus")
    assert calls["client_kwargs"] == {"endpoint_url": "http://minio:9000", "region_name": "us-east-2"}
