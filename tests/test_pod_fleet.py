# SPDX-License-Identifier: Apache-2.0
import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest

from ingest_bench.k8s.fleet import fleet_from_pods, main, quantity
from ingest_bench.specs.model import load_run_spec

ROOT = Path(__file__).resolve().parents[1]


def spark_pods() -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {"name": name, "labels": {"spark-role": role}},
                "status": {"phase": "Running"},
                "spec": {
                    "containers": [
                        {
                            "name": "spark",
                            "resources": {
                                "requests": {"cpu": cpu, "memory": memory},
                                "limits": {"cpu": "99", "memory": "99Gi"},
                            },
                        }
                    ]
                },
            }
            for name, role, cpu, memory in (
                ("driver", "driver", "1000m", "2867Mi"),
                ("executor-1", "executor", "2", "5734Mi"),
                ("executor-2", "executor", "2", "5734Mi"),
            )
        ]
    }


def test_fleet_uses_admitted_requests_including_spark_overhead() -> None:
    spec = load_run_spec(ROOT / "runs/aws-smoke-spark.yaml")
    roles = fleet_from_pods(spec, spark_pods())
    assert sum(role.count * role.gib for role in roles) == pytest.approx(14335 / 1024)
    assert sum(role.count * role.vcpu for role in roles) == 5


def test_pod_role_label_comes_from_the_engine_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    from engines.spark import knobs

    monkeypatch.setattr(knobs, "KUBERNETES", replace(knobs.KUBERNETES, pod_role_label="custom-role"))
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    for pod in items:
        labels = pod["metadata"]["labels"]
        labels["custom-role"] = labels.pop("spark-role")
    assert sum(role.count for role in fleet_from_pods(load_run_spec(ROOT / "runs/aws-smoke-spark.yaml"), pods)) == 3


@pytest.mark.parametrize("fault", ["missing", "pending", "duplicate", "no-request"])
def test_incomplete_or_invalid_fleet_cannot_be_costed(fault: str) -> None:
    pods = copy.deepcopy(spark_pods())
    items = pods["items"]
    assert isinstance(items, list)
    if fault == "missing":
        items.pop()
    elif fault == "pending":
        items[0]["status"]["phase"] = "Pending"
    elif fault == "duplicate":
        items[1]["metadata"]["name"] = items[2]["metadata"]["name"]
    else:
        items[0]["spec"]["containers"][0]["resources"]["requests"].pop("memory")
    with pytest.raises(ValueError):
        fleet_from_pods(load_run_spec(ROOT / "runs/aws-smoke-spark.yaml"), pods)


@pytest.mark.parametrize(
    "value, expected", [("500m", 0.5), ("4Gi", 4294967296), ("1e3", 1000), ("2G", 2e9), ("1Ei", 1024**6)]
)
def test_kubernetes_quantity_units(value: str, expected: float) -> None:
    assert quantity(value) == expected


@pytest.mark.parametrize("value", ["1e999", "nan", "-1", "0", "4GB"])
def test_invalid_or_unrepresentable_resource_quantity(value: str) -> None:
    with pytest.raises(ValueError):
        quantity(value)


def test_capture_retains_resource_evidence_without_environment_secrets(tmp_path: Path) -> None:
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    items[0]["spec"]["containers"][0]["env"] = [{"name": "TOKEN", "value": "secret-token"}]
    source, output = tmp_path / "raw.json", tmp_path / "engine-pods.json"
    source.write_text(json.dumps(pods))
    assert main(["--spec", str(ROOT / "runs/aws-smoke-spark.yaml"), "--pods", str(source), "--out", str(output)]) == 0
    assert "secret-token" not in output.read_text()
    assert fleet_from_pods(load_run_spec(ROOT / "runs/aws-smoke-spark.yaml"), json.loads(output.read_text()))


@pytest.mark.parametrize("phase, deleting", [("Failed", False), ("Succeeded", False), ("Running", True)])
def test_capture_ignores_retired_pods_and_preserves_that_filter_on_reload(
    tmp_path: Path, phase: str, deleting: bool
) -> None:
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    old = copy.deepcopy(items[-1])
    old["metadata"]["name"] = "old-executor"
    old["status"]["phase"] = phase
    if deleting:
        old["metadata"]["deletionTimestamp"] = "2026-09-10T12:00:00Z"
    items.append(old)
    source, output = tmp_path / "raw.json", tmp_path / "saved.json"
    source.write_text(json.dumps(pods))
    spec_path = ROOT / "runs/aws-smoke-spark.yaml"
    assert main(["--spec", str(spec_path), "--pods", str(source), "--out", str(output)]) == 0
    saved = json.loads(output.read_text())
    assert len(saved["items"]) == 3
    assert sum(role.count for role in fleet_from_pods(load_run_spec(spec_path), saved)) == 3


@pytest.mark.parametrize("key", ["initContainers", "overhead", "resources"])
def test_unsupported_pod_resource_layouts_are_not_silently_undercounted(key: str) -> None:
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    items[0]["spec"][key] = {"memory": "1Gi"}
    with pytest.raises(ValueError, match="container-only requests"):
        fleet_from_pods(load_run_spec(ROOT / "runs/aws-smoke-spark.yaml"), pods)
