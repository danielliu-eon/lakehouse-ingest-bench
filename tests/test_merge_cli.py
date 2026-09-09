# SPDX-License-Identifier: Apache-2.0
import json
import subprocess
import sys
from pathlib import Path

import pytest

from ingest_bench import uri
from ingest_bench.corpus import generate, merge, metadata, preset

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"
OVERRIDES = ["offered_bytes_per_s=1MB", "duration_s=4", "partition_count=8"]


def test_merge_equals_unsharded(tmp_path: Path) -> None:
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=OVERRIDES)
    whole = generate.generate(p, str(tmp_path / "whole"), seed=5, row_block=64)
    shards = []
    for i in range(2):
        generate.generate(p, str(tmp_path / f"s{i}"), seed=5, shard_index=i, shard_count=2, row_block=64)
        shards.append(uri.join(str(tmp_path / f"s{i}"), preset.corpus_dir_name(p)))
    merged = merge.merge(shards, str(tmp_path / "merged"))
    for key in (
        "row_count",
        "encoded_bytes",
        "partition_rows",
        "partition_sum_mod",
        "mean_encoded_row_size",
        "corpus_hash",
        # A shard's sample is a subset of the whole corpus's sample by
        # construction, so the merged statistic is the same statistic.
        "column_stats",
        "column_cardinality_max_relative_deviation",
    ):
        assert merged[key] == whole[key], key
    assert merged["shard_count"] == 1
    meta = metadata.read(uri.join(str(tmp_path / "merged"), preset.corpus_dir_name(p)))
    records = metadata.read_manifest(meta.uri)
    assert [r.batch for r in records] == [0, 1, 2, 3]
    assert records[1].uri.startswith(shards[1]) and uri.exists(records[1].uri)
    # A merged corpus is metadata beside the per-shard data, not a copy of it.
    assert "batches" not in uri.listdir(meta.uri)
    # An unsharded manifest holds relative references, and resolves against its own corpus.
    whole_meta = metadata.read(uri.join(str(tmp_path / "whole"), preset.corpus_dir_name(p)))
    whole_records = metadata.read_manifest(whole_meta.uri)
    assert whole_records[1].uri.startswith(whole_meta.uri) and uri.exists(whole_records[1].uri)
    assert all(uri.exists(key_uri) for key_uri in whole_records[1].key_uris.values())


def test_merge_refuses_a_missing_shard(tmp_path: Path) -> None:
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=OVERRIDES)
    generate.generate(p, str(tmp_path), seed=5, shard_index=0, shard_count=2, row_block=64)
    shard = uri.join(str(tmp_path), preset.corpus_dir_name(p))
    with pytest.raises(ValueError, match="expected shards 0..1"):
        merge.merge([shard], str(tmp_path / "merged"))


def test_metadata_refuses_a_shard(tmp_path: Path) -> None:
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=OVERRIDES)
    generate.generate(p, str(tmp_path), seed=5, shard_index=0, shard_count=2, row_block=64)
    corpus_uri = uri.join(str(tmp_path), preset.corpus_dir_name(p))
    try:
        metadata.read(corpus_uri)
    except ValueError as err:
        assert "shard" in str(err)
    else:
        raise AssertionError("a shard corpus.json must be refused")
    published = json.loads(uri.read_text(uri.join(corpus_uri, "corpus.json")))
    published["generator_version"] = "0"
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps(published))
    with pytest.raises(ValueError, match="generator version 0"):
        metadata.read(corpus_uri)
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps({"generator_version": generate.GENERATOR_VERSION}))
    with pytest.raises(ValueError, match="missing key 'shard_count'"):
        metadata.read(corpus_uri)


def test_cli_plan_and_generate(tmp_path: Path) -> None:
    env = {"INGEST_BENCH_WORKLOADS": str(WORKLOADS)}
    plan = subprocess.run(
        [
            sys.executable,
            "-m",
            "ingest_bench.corpus.cli",
            "gen",
            "--preset",
            "smoke",
            "--set",
            "duration_s=2",
            "--plan",
            "--out",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        check=True,
        env={**env, "PATH": ""},
    )
    assert "batches: 2" in plan.stdout and not list(tmp_path.iterdir())
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ingest_bench.corpus.cli",
            "gen",
            "--preset",
            "smoke",
            "--set",
            "duration_s=2",
            "--set",
            "offered_bytes_per_s=500KB",
            "--set",
            "partition_count=8",
            "--out",
            str(tmp_path),
            "--seed",
            "9",
        ],
        check=True,
        env={**env, "PATH": ""},
    )
    dirs = list(tmp_path.iterdir())
    assert len(dirs) == 1 and dirs[0].name.startswith("smoke-")
    assert json.loads((dirs[0] / "corpus.json").read_text())["seed"] == 9
