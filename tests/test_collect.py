# SPDX-License-Identifier: Apache-2.0
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from ingest_bench import stage
from ingest_bench.collect import cli, run_json
from ingest_bench.collect.redact import redact_document, redact_props, redact_uri
from ingest_bench.specs import model

BUCKET = "s3://a-bucket"

EPOCH_MS = 1_000_000_000_000
LAST_ACK_MS = EPOCH_MS + 1_800_000

# A price per vCPU-hour and per GiB-hour that make the fleet below cost
# 0.25 USD/h: 1 x (1 x 0.04 + 2 x 0.005) + 2 x (2 x 0.04 + 4 x 0.005).
VCPU_HOUR_USD = 0.04
GIB_HOUR_USD = 0.005

FLINK_SPEC = """
name: collect-flink
engine: flink
corpus: smoke
table:
  partition: identity(partition_key)
kafka:
  partitions: 4
  key: user_id
producer:
  shards: 1
scoring:
  freshness_bound_s: 60
  warmup_s: 60
flink:
  taskmanagers: 2
  slots: 4
  tm_cpu: 2
  tm_mem_mb: 4096
  jm_cpu: 1
  jm_mem_mb: 2048
  checkpoint_interval: 10s
  distribution_mode: hash
  machine_type: m6i.xlarge
"""

EXTERNAL_SPEC = """
name: collect-external
engine: external
corpus: smoke
kafka:
  partitions: 4
  key: user_id
external:
  name: some-engine
  version: "1.2"
  notes: "read the topic with its own writer"
fleet:
  - {role: worker, count: 3, vcpu: 4, gib: 8, machine_type: n2-standard-4}
"""


def _site(tmp_path: Path) -> Path:
    path = tmp_path / "site.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {
                    "bootstrap_servers": "broker.invalid:9092",
                    "security": {"sasl.password": "the-broker-password"},
                },
                "catalog": {
                    "props": {
                        "uri": "https://catalog.invalid/iceberg",
                        "warehouse": f"{BUCKET}/warehouse",
                        "token": "a-literal-catalog-token",
                        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
                    }
                },
                "pricing": {"vcpu_hour_usd": VCPU_HOUR_USD, "gib_hour_usd": GIB_HOUR_USD},
            }
        )
    )
    return path


def _summary() -> dict[str, object]:
    """The recorded AWS smoke's summary, trimmed to the keys `collect` reads."""
    return {
        "aborted": False,
        "backlog_rows": 0,
        "clock_skew_suspected": False,
        "committed_rows": 5840896,
        "corpus_hash": "e13842f9",
        "corpus_uri": f"{BUCKET}/corpus/smoke-e13842f9",
        "epoch_ms": EPOCH_MS,
        "keepup": {
            "absorbed_at_offer_end": 0.9927591463414634,
            "backlog_rows_max": 136192,
            "backlog_rows_p50": 0.0,
            "drain_s": 5.866,
        },
        "producer": {"behind_ms": 315, "errors": 0},
        "producer_bound": False,
        "reason": None,
        "run_valid": True,
        "state": "drained",
        "table": "ingest_bench.t_collect_flink_20260909T052508Z",
    }


def _freshness() -> dict[str, object]:
    return {
        "bound_s": 60.0,
        "clock": "timestamp_ms",
        "clock_skew_suspected": False,
        "drained": True,
        "full": {"max_s": 27.445, "p50_s": 7.697, "p95_s": 11.76955, "p99_s": 24.425},
        "lag_series": {"timestamp_ms": [{"at_ms": EPOCH_MS, "lag_s": 0.0, "prefix": -1}]},
        "min_lag_s": 1.887,
        "missing_emit_prefixes": [],
        "verdict": True,
        "warmup_s": 60,
        "window": {"max_s": 27.445, "p50_s": 7.715, "p95_s": 14.345, "p99_s": 25.025},
    }


def _exactness() -> dict[str, object]:
    return {
        "corrupt_batches": 0,
        "duplicate_ppm": 0.0,
        "duplicate_rows": 0,
        "exact": True,
        "expected_rows": 5840896,
        "loss_rows": 0,
        "rows": 5840896,
        "scored_batches": 300,
        "violations": [{"batch": 7, "expected_rows": 10, "rows": 9, "kind": "loss"}],
    }


def _geometry() -> dict[str, object]:
    return {
        "epoch_ms": EPOCH_MS,
        "offsets_s": [600, 1200],
        "at": {
            "600": {
                "snapshot_id": 11,
                "timestamp_ms": EPOCH_MS + 600_000,
                "live": {
                    "files": 4,
                    "rows": 400,
                    "bytes": 4096,
                    "size_quantiles": {"p50": 1024.0, "p90": 1024.0, "p99": 1024.0, "min": 1024, "max": 1024},
                    "small_file_share_32mib": 1.0,
                    "small_file_share_8mib": 1.0,
                    "log2_histogram": {"2^10..2^11": 4},
                },
                "per_commit": {
                    "commits": 2,
                    "files_added_quantiles": {"p50": 2.0, "p90": 2.0, "p99": 2.0},
                    "file_size_quantiles": {"p50": 1024.0, "p90": 1024.0, "p99": 1024.0},
                },
            },
            "1200": "absent",
        },
        "final": None,
    }


def _run_dir(
    tmp_path: Path,
    *,
    spec: str = FLINK_SPEC,
    geometry: bool = True,
    publish_logs: bool = True,
    engine_image: bool = True,
) -> Path:
    run_dir = tmp_path / "runs" / "collect-flink-20260909T052508Z"
    scores = run_dir / "scores"
    scores.mkdir(parents=True)
    (run_dir / "spec.yaml").write_text(spec)
    (run_dir / "facts.json").write_text(
        json.dumps(
            {
                "run_id": "collect-flink-20260909T052508Z",
                "bootstrap": "broker.invalid:9092",
                "topic": "collect-flink-20260909T052508Z",
                "corpus_uri": f"{BUCKET}/corpus/smoke-e13842f9",
                "schema_avsc_uri": f"{BUCKET}/corpus/smoke-e13842f9/schema.avsc",
                "catalog_props": {
                    "uri": "https://catalog.invalid/iceberg",
                    "warehouse": f"{BUCKET}/warehouse",
                    "token": "<redacted>",
                    "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
                },
                "table": "ingest_bench.t_collect_flink_20260909T052508Z",
                "partition": "identity(partition_key)",
                "ddl": None,
                "key_column": "user_id",
                "epoch": EPOCH_MS / 1000,
            }
        )
    )
    (run_dir / "timeline.log").write_text("2026-09-09T05:25:08Z staged\n")
    if engine_image:
        (run_dir / "engine-image.json").write_text(
            json.dumps({"image": "registry.invalid/lakehouse-ingest-bench/flink:abc123", "digest": "sha256:deadbeef"})
        )
    (scores / "summary.json").write_text(json.dumps(_summary()))
    (scores / "freshness.json").write_text(json.dumps(_freshness()))
    (scores / "exactness.json").write_text(json.dumps(_exactness()))
    (scores / "keepup_samples.jsonl").write_text(
        json.dumps({"at_ms": EPOCH_MS, "offered_rows": 0, "committed_rows": 0, "backlog_rows": 0}) + "\n"
    )
    # The scorer's own snapshot records carry no path today. One is written here
    # anyway: the redaction must hold for whatever fields an artifact carries,
    # not for the fields it carries at the moment the test was written.
    (scores / "snapshots.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "operation": "append",
                    "timestamp_ms": EPOCH_MS + 600_000 * index,
                    "added_files": 2,
                    "added_rows": 200,
                    "added_bytes": 2048,
                    "prefix_after": index,
                    "data_files": [f"{BUCKET}/warehouse/ingest_bench/t_collect/data/{snapshot_id}-0.parquet"],
                }
            )
            for index, snapshot_id in enumerate((11, 12))
        )
        + "\n"
    )
    if geometry:
        (scores / "geometry.json").write_text(json.dumps(_geometry()))
    if publish_logs:
        producer = run_dir / "producer"
        producer.mkdir()
        # Two batches spanning the 1800 s the run is costed over, 0.9 GB each,
        # the second acked 315 ms after it was due.
        (producer / "publish_log-0.jsonl").write_text(
            "\n".join(
                json.dumps(
                    {
                        "batch": batch,
                        "scheduled_ms": EPOCH_MS + batch * 1000,
                        "first_ack_ms": EPOCH_MS if batch == 0 else EPOCH_MS + 1315,
                        "last_ack_ms": EPOCH_MS + 400 if batch == 0 else LAST_ACK_MS,
                        "rows": 100,
                        "bytes": 900_000_000,
                        "errors": 0,
                    }
                )
                for batch in (0, 1)
            )
            + "\n"
            + json.dumps({"done": True, "shard": 0, "batches": 2})
            + "\n"
        )
    return run_dir


COLLECTED_AT = datetime(2026, 9, 20, 11, 30, 0, tzinfo=UTC)


def _build(run_dir: Path, site_path: Path, *, variant: str = "hash") -> dict[str, object]:
    return run_json.build_run_json(
        run_dir,
        model.load_site(site_path),
        harness_version="9.9.9",
        collected_at=COLLECTED_AT,
        variant=variant,
    )


def _strings(value: object) -> list[str]:
    """Every string anywhere in a document, keys included."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        found: list[str] = []
        for key, entry in value.items():
            found.append(str(key))
            found.extend(_strings(entry))
        return found
    if isinstance(value, list):
        return [text for entry in value for text in _strings(entry)]
    return []


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def _site_config(tmp_path: Path) -> model.SiteConfig:
    return model.load_site(_site(tmp_path))


def test_redact_uri_takes_the_longest_matching_root(tmp_path: Path) -> None:
    path = tmp_path / "nested.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                # The corpus root is the bucket itself, so the runs prefix sits
                # under it and only the longer root describes a run's path.
                "corpus_root": BUCKET,
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse/",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"uri": "https://catalog.invalid"}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_uri(f"{BUCKET}/runs/r1/scores/summary.json", site) == "<runs_root>/r1/scores/summary.json"
    assert redact_uri(f"{BUCKET}/warehouse/ns/t/data/a.parquet", site) == "<warehouse>/ns/t/data/a.parquet"
    assert redact_uri(f"{BUCKET}/corpus/smoke-1", site) == "<corpus_root>/corpus/smoke-1"
    assert redact_uri(BUCKET, site) == "<corpus_root>"
    assert redact_uri("s3://another-bucket/runs/r1", site) == "s3://another-bucket/runs/r1"


def test_redact_uri_covers_the_registry_and_a_glue_warehouse(tmp_path: Path) -> None:
    path = tmp_path / "glue.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                # Glue's warehouse property is the account id itself, twelve digits
                # that sit under no object-store root.
                "catalog": {"props": {"uri": "https://glue.invalid/iceberg", "warehouse": "123456789012"}},
                "kubernetes": {
                    "context": "c",
                    "namespace": "n",
                    "harness_service_account": "h",
                    "flink_service_account": "f",
                    "registry": "123456789012.dkr.ecr.us-east-2.amazonaws.com",
                },
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    registry = "123456789012.dkr.ecr.us-east-2.amazonaws.com"
    assert redact_uri("123456789012", site) == "<catalog_warehouse>"
    assert redact_uri(f"{registry}/bench/flink:abc", site) == "<registry>/bench/flink:abc"
    assert redact_uri(f"{registry}/bench/flink@sha256:0", site) == "<registry>/bench/flink@sha256:0"
    # A catalog warehouse that is the site warehouse keeps the site's own name.
    same = _site_config(tmp_path)
    assert redact_uri(f"{BUCKET}/warehouse/ns/t", same) == "<warehouse>/ns/t"


def test_redact_uri_does_not_match_a_sibling_prefix(tmp_path: Path) -> None:
    site = _site_config(tmp_path)
    assert redact_uri(f"{BUCKET}/corpus-archive/smoke-1", site) == f"{BUCKET}/corpus-archive/smoke-1"
    assert redact_uri(f"{BUCKET}/corpus/smoke-1", site) == "<corpus_root>/smoke-1"


def test_redact_uri_ignores_a_root_a_site_left_empty(tmp_path: Path) -> None:
    path = tmp_path / "empty-root.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": "",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"uri": "https://catalog.invalid"}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_uri("/opt/bench/run/job.sql", site) == "/opt/bench/run/job.sql"
    assert redact_uri(f"{BUCKET}/warehouse/ns/t", site) == "<warehouse>/ns/t"


def test_redact_props_replaces_literal_credentials_and_site_roots(tmp_path: Path) -> None:
    site = _site_config(tmp_path)
    props = {
        "uri": "https://catalog.invalid/iceberg",
        "warehouse": f"{BUCKET}/warehouse",
        "token": "a-literal-catalog-token",
        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
    }
    assert redact_props(props, site) == {
        "uri": "https://catalog.invalid/iceberg",
        "warehouse": "<warehouse>",
        "token": "<redacted>",
        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
    }
    # Without a site the URIs survive: `facts.json` is what an engine is
    # configured from, and a placeholder there points it at nothing.
    assert redact_props(props, None)["warehouse"] == f"{BUCKET}/warehouse"


def test_redact_document_reaches_nested_strings_and_keys(tmp_path: Path) -> None:
    site = _site_config(tmp_path)
    document = {
        "paths": [f"{BUCKET}/warehouse/a.parquet", {"inner": f"{BUCKET}/runs/r1"}],
        f"{BUCKET}/corpus/x": 3,
        "count": 7,
        "flag": True,
        "nothing": None,
    }
    assert redact_document(document, site) == {
        "paths": ["<warehouse>/a.parquet", {"inner": "<runs_root>/r1"}],
        "<corpus_root>/x": 3,
        "count": 7,
        "flag": True,
        "nothing": None,
    }


def test_stage_redact_still_answers_for_the_secret_rule() -> None:
    assert stage.redact({"token": "t", "uri": "http://catalog:8181"}) == {
        "token": "<redacted>",
        "uri": "http://catalog:8181",
    }


# ---------------------------------------------------------------------------
# The document
# ---------------------------------------------------------------------------


def test_run_json_publishes_schema_version_2_and_no_site(tmp_path: Path) -> None:
    site_path = _site(tmp_path)
    document = _build(_run_dir(tmp_path), site_path)
    assert document["schema_version"] == 2
    assert document["collected_at"] == "2026-09-20T11:30:00Z"
    assert document["harness_version"] == "9.9.9"
    strings = _strings(document)
    assert not [text for text in strings if "a-bucket" in text]
    assert "the-broker-password" not in strings
    assert "a-literal-catalog-token" not in strings
    assert "<warehouse>/ingest_bench/t_collect/data/11-0.parquet" in strings
    assert "<corpus_root>/smoke-e13842f9" in strings
    # The security block is never read, so neither its keys nor its values reach
    # the document under any name.
    assert "sasl.password" not in strings


def test_run_json_redacts_the_catalog_properties(tmp_path: Path) -> None:
    document = _build(_run_dir(tmp_path), _site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["catalog_props"] == {
        "uri": "https://catalog.invalid/iceberg",
        "warehouse": "<warehouse>",
        "token": "<redacted>",
        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
    }
    assert run["variant"] == "hash"
    assert run["topic"] == "collect-flink-20260909T052508Z"
    assert run["table"] == "ingest_bench.t_collect_flink_20260909T052508Z"
    assert run["epoch_ms"] == EPOCH_MS
    assert run["corpus_hash"] == "e13842f9"
    # Copied verbatim rather than re-serialised from the loaded dataclass.
    assert run["spec"] == yaml.safe_load(FLINK_SPEC)


def test_run_json_derives_the_figures_a_result_is_read_by(tmp_path: Path) -> None:
    document = _build(_run_dir(tmp_path), _site(tmp_path))
    derived = document["derived"]
    assert isinstance(derived, dict)
    assert derived["freshness"] == {
        "window": {"max_s": 27.445, "p50_s": 7.715, "p95_s": 14.345, "p99_s": 25.025},
        "full": {"max_s": 27.445, "p50_s": 7.697, "p95_s": 11.76955, "p99_s": 24.425},
        "clock_skew_suspected": False,
        "min_lag_s": 1.887,
    }
    exactness = derived["exactness"]
    assert isinstance(exactness, dict)
    assert exactness["loss_rows"] == 0 and exactness["duplicate_rows"] == 0 and exactness["exact"] is True
    # The violation list is evidence rather than a figure and stays in `data`.
    assert "violations" not in exactness
    data = document["data"]
    assert isinstance(data, dict)
    assert data["exactness"] == _exactness()
    assert derived["keepup"] == {
        "absorbed_at_offer_end": 0.9927591463414634,
        "drain_s": 5.866,
        "backlog_rows_max": 136192,
        "backlog_rows_p50": 0.0,
    }
    assert derived["producer"] == {
        "behind_ms_max": 315,
        "errors": 0,
        # 1.8 GB acked over the 1800 s between the first and the last
        # acknowledgement.
        "effective_offered_rate_bytes_per_s": pytest.approx(1_000_000.0),
        "producer_bound": False,
    }
    assert document["geometry"] == _geometry()


def test_run_json_summarizes_each_producer_shard(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    # A second shard that stopped without its trailer: a result whose shards are
    # not all done describes a partial offer.
    (run_dir / "producer" / "publish_log-1.jsonl").write_text(
        json.dumps(
            {
                "batch": 2,
                "scheduled_ms": EPOCH_MS + 2000,
                "first_ack_ms": EPOCH_MS + 2100,
                "last_ack_ms": EPOCH_MS + 2400,
                "rows": 50,
                "bytes": 1_000,
                "errors": 3,
            }
        )
        + "\n"
    )
    artifacts = _build(run_dir, _site(tmp_path))["artifacts"]
    assert isinstance(artifacts, dict)
    assert artifacts["publish_logs"] == [
        {
            "shard": 0,
            "batches": 2,
            "first_scheduled_ms": EPOCH_MS,
            "first_ack_ms": EPOCH_MS,
            "last_ack_ms": LAST_ACK_MS,
            "bytes": 1_800_000_000,
            "rows": 200,
            "behind_ms_max": 315,
            "errors": 0,
            "done": True,
        },
        {
            "shard": 1,
            "batches": 1,
            "first_scheduled_ms": EPOCH_MS + 2000,
            "first_ack_ms": EPOCH_MS + 2100,
            "last_ack_ms": EPOCH_MS + 2400,
            "bytes": 1_000,
            "rows": 50,
            "behind_ms_max": 100,
            "errors": 3,
            "done": False,
        },
    ]


def test_run_json_points_at_the_run_s_own_artifacts(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    artifacts = _build(run_dir, _site(tmp_path))["artifacts"]
    assert isinstance(artifacts, dict)
    assert artifacts["spec"] == "spec.yaml"
    assert artifacts["facts"] == "facts.json"
    assert artifacts["timeline"] == "timeline.log"
    assert artifacts["engine_image"] == "engine-image.json"
    assert artifacts["summary"] == "scores/summary.json"
    assert artifacts["geometry"] == "scores/geometry.json"
    assert artifacts["keepup_samples"] == "scores/keepup_samples.jsonl"
    # The commit series is embedded whole; every freshness figure came from it.
    snapshots = artifacts["snapshots"]
    assert isinstance(snapshots, list)
    assert [row["snapshot_id"] for row in snapshots] == [11, 12]


def test_run_json_costs_the_fleet_over_the_run(tmp_path: Path) -> None:
    document = _build(_run_dir(tmp_path), _site(tmp_path))
    derived = document["derived"]
    assert isinstance(derived, dict)
    assert derived["cost"] == {
        "usd_per_hour": pytest.approx(0.25),
        "run_hours": pytest.approx(0.5),
        "usd": pytest.approx(0.125),
    }


def test_run_json_names_every_optional_input_it_could_not_read(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path, geometry=False, publish_logs=False, engine_image=False)
    document = _build(run_dir, _site(tmp_path))
    assert document["missing"] == [
        "engine-image.json",
        "scores/geometry.json",
        "producer/publish_log-*.jsonl",
    ]
    assert document["geometry"] is None
    run = document["run"]
    assert isinstance(run, dict)
    assert run["engine_versions"] is None
    derived = document["derived"]
    assert isinstance(derived, dict)
    # Without the logs the scorer's own reading of them stands in, and the rate
    # it would have been divided from is left unmeasured.
    assert derived["producer"] == {
        "behind_ms_max": 315,
        "errors": 0,
        "effective_offered_rate_bytes_per_s": None,
        "producer_bound": False,
    }
    cost = derived["cost"]
    assert isinstance(cost, dict)
    # The table's last commit stands in for the producer's last acknowledgement.
    assert cost["run_hours"] == pytest.approx(600_000 / 3_600_000)


def test_run_json_refuses_a_directory_that_names_no_run(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="spec.yaml"):
        _build(empty, _site(tmp_path))


# ---------------------------------------------------------------------------
# Fleet
# ---------------------------------------------------------------------------


def test_fleet_of_a_managed_flink_run_comes_from_its_knobs(tmp_path: Path) -> None:
    document = _build(_run_dir(tmp_path), _site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["fleet"] == [
        {"role": "jobmanager", "count": 1, "vcpu": 1.0, "gib": 2.0, "machine_type": "m6i.xlarge"},
        {"role": "taskmanager", "count": 2, "vcpu": 2.0, "gib": 4.0, "machine_type": "m6i.xlarge"},
    ]
    assert run["engine_versions"] == {
        "image": "registry.invalid/lakehouse-ingest-bench/flink:abc123",
        "digest": "sha256:deadbeef",
    }


def test_fleet_without_a_machine_type_knob_says_so(tmp_path: Path) -> None:
    spec = FLINK_SPEC.replace("  machine_type: m6i.xlarge\n", "")
    document = _build(_run_dir(tmp_path, spec=spec), _site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    fleet = run["fleet"]
    assert isinstance(fleet, list)
    assert {role["machine_type"] for role in fleet} == {"unspecified"}


def test_fleet_of_an_external_run_is_the_spec_s_own(tmp_path: Path) -> None:
    document = _build(_run_dir(tmp_path, spec=EXTERNAL_SPEC), _site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["engine"] == "external"
    assert run["fleet"] == [{"role": "worker", "count": 3, "vcpu": 4.0, "gib": 8.0, "machine_type": "n2-standard-4"}]
    assert run["engine_versions"] == {
        "name": "some-engine",
        "version": "1.2",
        "notes": "read the topic with its own writer",
    }
    derived = document["derived"]
    assert isinstance(derived, dict)
    cost = derived["cost"]
    assert isinstance(cost, dict)
    # 3 x (4 x 0.04 + 8 x 0.005) = 0.6 USD/h.
    assert cost["usd_per_hour"] == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_results_name_is_date_engine_corpus_variant() -> None:
    assert (
        cli.results_name("flink", "events-100mbs-skew", "hash", COLLECTED_AT)
        == "2026-09-20-flink-events-100mbs-skew-hash.json"
    )


def test_collect_writes_run_json_beside_the_run_by_default(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run_dir = _run_dir(tmp_path)
    site_path = _site(tmp_path)
    assert cli.main(["--run-dir", str(run_dir), "--site", str(site_path)]) == 0
    document = json.loads((run_dir / "run.json").read_text())
    assert document["schema_version"] == 2
    assert document["harness_version"]
    assert "run.json" in capsys.readouterr().out


def test_collect_names_the_file_when_out_is_a_directory(tmp_path: Path) -> None:
    run_dir = _run_dir(tmp_path)
    results = tmp_path / "results" / "flink"
    results.mkdir(parents=True)
    assert (
        cli.main(
            [
                "--run-dir",
                str(run_dir),
                "--site",
                str(_site(tmp_path)),
                "--out",
                str(results),
                "--variant",
                "sorted",
            ]
        )
        == 0
    )
    written = sorted(path.name for path in results.iterdir())
    assert len(written) == 1
    name = written[0]
    assert name.endswith("-flink-smoke-sorted.json")
    document = json.loads((results / name).read_text())
    assert document["run"]["variant"] == "sorted"
