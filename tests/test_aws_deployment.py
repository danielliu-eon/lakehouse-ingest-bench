# SPDX-License-Identifier: Apache-2.0
"""Exercise deployment selection and storage setup without cloud access."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
AWS = ROOT / "deploy" / "aws"


@pytest.fixture
def aws_environment(tmp_path: Path) -> dict[str, str]:
    for tool in ("bash", "jq", "yq", "envsubst"):
        if not shutil.which(tool):
            pytest.skip(f"requires {tool}")
    binary = tmp_path / "bin"
    binary.mkdir()
    stub = binary / "stub"
    stub.write_text(
        f"#!{sys.executable}\n"
        + """import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ["CALLS"], "a") as stream:
    stream.write(json.dumps([name, *args]) + "\\n")
if name == "aws":
    service, action = args[:2]
    if os.environ.get("TEST_NO_MSK") == "true" and service in ("kafka", "ec2"):
        sys.exit("MSK API called when disabled")
    if (service, action) == ("sts", "get-caller-identity"):
        print("123456789012")
    elif (service, action) == ("eks", "describe-cluster"):
        if os.environ.get("TEST_EKS_ABSENT"):
            sys.exit(1)
        print(json.dumps({"cluster": {"resourcesVpcConfig": {"vpcId": "vpc-1", "subnetIds": ["s1", "s2"]}}}))
    elif (service, action) == ("s3api", "get-bucket-tagging"):
        print("true")
    elif (service, action) == ("s3api", "get-bucket-versioning"):
        print("None")
    elif (service, action) == ("iam", "put-role-policy"):
        pathlib.Path(os.environ["POLICY"]).write_text(args[args.index("--policy-document") + 1])
    elif (service, action) == ("ec2", "describe-subnets"):
        print(json.dumps({"Subnets": [
            {"SubnetId": "s1", "AvailabilityZone": "a", "MapPublicIpOnLaunch": False},
            {"SubnetId": "s2", "AvailabilityZone": "b", "MapPublicIpOnLaunch": False}]}))
    elif (service, action) == ("ec2", "describe-security-groups"):
        print("sg-1")
    elif (service, action) == ("ec2", "describe-vpcs"):
        print("10.0.0.0/16")
    elif (service, action) == ("kafka", "list-clusters"):
        print("None" if os.environ.get("TEST_MSK_ABSENT") else "arn:aws:kafka:us-west-2:123456789012:cluster/bench/id")
    elif (service, action) == ("kafka", "describe-cluster"):
        query = args[args.index("--query") + 1]
        print("ACTIVE\\tv1\\t100" if "VolumeSize" in query else "ACTIVE")
    elif (service, action) == ("kafka", "get-bootstrap-brokers"):
        print("broker.example:9098")
elif name == "kubectl":
    if "get-contexts" in args:
        print("bench")
    elif "nodes" in args:
        print(json.dumps({"items": [{"status": {"nodeInfo": {"architecture": "amd64"}}}]}))
    elif "apply" in args:
        sys.stdin.read()
elif name == "helm" and "status" in args:
    print(json.dumps({"info": {"status": "deployed"}}))
elif name == "helm":
    print('[{"chart": "operator-1.0"}]')
elif name == "helmfile":
    assert os.environ["CATALOG_STORAGE_CLASS"] == "ingest-bench-catalog"
"""
    )
    stub.chmod(0o755)
    for name in ("aws", "kubectl", "helm", "helmfile"):
        (binary / name).symlink_to(stub)
    environment = dict(os.environ)
    for key in ("MSK_BROKERS", "MSK_VOLUME_GIB", "WITH_MSK"):
        environment.pop(key, None)
    environment.update(
        PATH=f"{binary}:{os.environ['PATH']}",
        AWS_REGION="us-west-2",
        CLUSTER_NAME="bench",
        KUBE_CONTEXT="bench",
        NAMESPACE="ingest-bench",
        BUCKET="bench-bucket",
        WITH_SCHEMA_REGISTRY="false",
        CALLS=str(tmp_path / "calls.jsonl"),
        POLICY=str(tmp_path / "policy.json"),
    )
    site = tmp_path / "input-site.yaml"
    site.write_text("kafka:\n  deployment: in-cluster\n")
    environment["SITE_FILE"] = str(site)
    environment["CLOUD"] = "aws"
    return environment


@pytest.mark.parametrize("deployment", ["managed", "in-cluster", "external"])
def test_setup_follows_configured_deployment(aws_environment: dict[str, str], tmp_path: Path, deployment: str) -> None:
    source = {
        "kafka": {
            "deployment": deployment,
            "bootstrap_servers": "external.example:9093",
            "security": {"security.protocol": "SASL_SSL", "sasl.password": "${env:PASSWORD}"},
            "schema_registry": {"url": "https://registry.example"},
        },
        "catalog": {"props": {"uri": "https://catalog.example", "warehouse": "existing"}},
    }
    Path(aws_environment["SITE_FILE"]).write_text(yaml.safe_dump(source))
    # A stale opt-out flag cannot override the site in either direction.
    aws_environment["WITH_MSK"] = "false" if deployment == "managed" else "true"
    if deployment != "managed":
        aws_environment["TEST_NO_MSK"] = "true"
        aws_environment["MSK_BROKERS"] = "unused"
        aws_environment["MSK_VOLUME_GIB"] = "unused"
        aws_environment["WITH_SCHEMA_REGISTRY"] = "true"
    site = tmp_path / "site.yaml"
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh"), "--site", aws_environment["SITE_FILE"], "--write-site", str(site)],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    policy = json.loads(Path(aws_environment["POLICY"]).read_text())
    settings = yaml.safe_load(site.read_text())
    assert settings["kafka"]["deployment"] == deployment
    statements = {statement["Sid"] for statement in policy["Statement"]}
    assert "ReadAndWriteTheThreePrefixes" in statements
    assert ("GlueIcebergCatalog" in statements) == (deployment != "in-cluster")
    if deployment != "managed":
        assert not any(sid.startswith("Msk") for sid in statements)
        calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
        assert not any(call[:2] in (["aws", "kafka"], ["aws", "ec2"]) for call in calls)
        assert "waiting for lakehouse-ingest-bench to reach ACTIVE" not in result.stdout
    if deployment == "in-cluster":
        assert settings["kafka"]["security"] == {}
        assert settings["kafka"]["bootstrap_servers"] == "ingest-bench-kafka-bootstrap.ingest-bench.svc:9092"
        assert settings["kafka"]["schema_registry"]["url"] == (
            "http://schema-registry.ingest-bench.svc:8080/apis/ccompat/v7"
        )
        assert settings["catalog"]["props"] == {
            "uri": "http://lakekeeper.ingest-bench.svc:8181/catalog",
            "warehouse": "ingest-bench",
            "s3.region": "us-west-2",
        }
        assert "glue." not in result.stdout
    elif deployment == "external":
        assert settings["kafka"] == source["kafka"]
        assert settings["catalog"] == source["catalog"]
    else:
        assert {"MskCluster", "MskTopics", "MskGroups"} <= statements
        assert settings["kafka"]["security"]["sasl.mechanism"] == "OAUTHBEARER"
        assert settings["kafka"]["bootstrap_servers"] == "broker.example:9098"


@pytest.mark.parametrize("deployment", ["in-cluster", "external"])
def test_teardown_skips_msk_for_other_deployments(aws_environment: dict[str, str], deployment: str) -> None:
    Path(aws_environment["SITE_FILE"]).write_text(f"kafka:\n  deployment: {deployment}\n")
    aws_environment["WITH_MSK"] = "true"
    aws_environment["TEST_NO_MSK"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
    assert not any(call[:2] in (["aws", "kafka"], ["aws", "ec2"]) for call in calls)
    assert any(call[:3] == ["aws", "iam", "delete-role"] for call in calls)


@pytest.mark.parametrize("eks_present,vpc_override", [(True, ""), (False, "vpc-original"), (False, "")])
def test_teardown_scopes_msk_security_group_to_original_vpc(
    aws_environment: dict[str, str], eks_present: bool, vpc_override: str
) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}\n")
    aws_environment["TEST_MSK_ABSENT"] = "true"
    aws_environment["VPC_ID"] = vpc_override
    if not eks_present:
        aws_environment["TEST_EKS_ABSENT"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh")], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
    group_calls = [call for call in calls if call[:3] == ["aws", "ec2", "describe-security-groups"]]
    deletes = [call for call in calls if call[:3] == ["aws", "ec2", "delete-security-group"]]
    if eks_present or vpc_override:
        assert result.returncode == 0, result.stderr
        assert len(group_calls) == len(deletes) == 1
        expected_vpc = "vpc-1" if eks_present else vpc_override
        assert f"Name=vpc-id,Values={expected_vpc}" in group_calls[0]
        assert "Name=tag:lakehouse-ingest-bench,Values=true" in group_calls[0]
    else:
        assert result.returncode != 0
        assert "set VPC_ID" in result.stderr
        assert not group_calls and not deletes


@pytest.mark.parametrize("script", ["aws/setup.sh", "aws/teardown.sh", "k8s/stack/setup.sh", "k8s/stack/teardown.sh"])
@pytest.mark.parametrize("configuration", [None, "kafka: {}", "kafka: {deployment: invalid}"])
def test_missing_or_invalid_deployment_fails_before_cloud_access(
    aws_environment: dict[str, str], script: str, configuration: str | None
) -> None:
    source = Path(aws_environment["SITE_FILE"])
    if configuration is None:
        source.unlink()
    else:
        source.write_text(configuration)
    result = subprocess.run(
        ["bash", str(ROOT / "deploy" / script)], env=aws_environment, capture_output=True, text=True, timeout=60
    )
    assert result.returncode != 0
    assert "--site" in result.stderr, result.stderr
    assert not Path(aws_environment["CALLS"]).exists()


@pytest.mark.parametrize("script", ["setup.sh", "teardown.sh"])
def test_stack_refuses_managed_kafka_before_cloud_access(aws_environment: dict[str, str], script: str) -> None:
    Path(aws_environment["SITE_FILE"]).write_text("kafka: {deployment: managed}")
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/k8s/stack" / script)],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "must set kafka.deployment: in-cluster" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()


def test_catalog_storage_is_baseline_and_independent() -> None:
    manifest = yaml.safe_load((AWS / "k8s" / "catalog-storageclass.yaml").read_text())
    assert manifest["metadata"]["name"] == "ingest-bench-catalog"
    assert manifest["parameters"] == {"type": "gp3", "throughput": "125", "iops": "3000", "encrypted": "true"}
    assert manifest["volumeBindingMode"] == "WaitForFirstConsumer"
    assert manifest["reclaimPolicy"] == "Delete"
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'''set -eu
source "{AWS / "_stack_hooks.sh"}"
log() {{ :; }}
kubectl() {{ printf '%s\\n' "$*"; }}
KUBE_CONTEXT=bench
KAFKA_STORAGE_CLASS=custom-broker-class
stack_delete_storage_class
''',
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "delete storageclass custom-broker-class ingest-bench-catalog --ignore-not-found" in result.stdout


def test_catalog_storage_setup_keeps_broker_tuning_separate() -> None:
    if not shutil.which("envsubst"):
        pytest.skip("requires envsubst")
    result = subprocess.run(
        [
            "bash",
            "-c",
            f'''set -eu
source "{AWS / "_stack_hooks.sh"}"
log() {{ :; }}
kubectl() {{
    printf '%s\\n' '---'
    if [[ ${{@: -1}} == - ]]; then cat; else cat "${{@: -1}}"; fi
}}
KUBE_CONTEXT=bench
KAFKA_VOLUME_THROUGHPUT_MIBS=500
KAFKA_VOLUME_IOPS=12000
stack_storage_class
''',
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    broker, catalog = list(yaml.safe_load_all(result.stdout))
    assert broker["parameters"]["throughput"] == "500"
    assert broker["parameters"]["iops"] == "12000"
    assert catalog["metadata"]["name"] == "ingest-bench-catalog"
    assert catalog["parameters"]["throughput"] == "125"
    assert catalog["parameters"]["iops"] == "3000"


def test_stack_teardown_exports_catalog_class_and_removes_both_classes(
    aws_environment: dict[str, str],
) -> None:
    aws_environment["CLOUD"] = "aws"
    result = subprocess.run(
        ["bash", str(ROOT / "deploy/k8s/stack/teardown.sh"), "--all", "--yes"],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
    assert [
        "kubectl",
        "--context",
        "bench",
        "delete",
        "storageclass",
        "ingest-bench-kafka",
        "ingest-bench-catalog",
        "--ignore-not-found",
    ] in calls


@pytest.mark.parametrize("catalog", [None, {"props": []}])
def test_external_site_generation_requires_catalog_before_cloud_access(
    aws_environment: dict[str, str], tmp_path: Path, catalog: object
) -> None:
    source: dict[str, object] = {"kafka": {"deployment": "external", "bootstrap_servers": "broker.example:9092"}}
    if catalog is not None:
        source["catalog"] = catalog
    Path(aws_environment["SITE_FILE"]).write_text(yaml.safe_dump(source))
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh"), "--write-site", str(tmp_path / "generated.yaml")],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "catalog.props" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()
    assert not (tmp_path / "generated.yaml").exists()


@pytest.mark.parametrize("example,managed", [("site.aws.example.yaml", True), ("site.k8s.example.yaml", False)])
def test_teardown_recovers_missing_site_with_example(
    aws_environment: dict[str, str], example: str, managed: bool
) -> None:
    Path(aws_environment["SITE_FILE"]).unlink()
    aws_environment["TEST_MSK_ABSENT"] = "true"
    result = subprocess.run(
        ["bash", str(AWS / "teardown.sh"), "--site", str(ROOT / example)],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [json.loads(line) for line in Path(aws_environment["CALLS"]).read_text().splitlines()]
    assert any(call[:3] == ["aws", "kafka", "list-clusters"] for call in calls) == managed


@pytest.mark.parametrize(
    "kafka,catalog",
    [
        ({"bootstrap_servers": "YOUR_KAFKA_BOOTSTRAP"}, {"props": {}}),
        ({"bootstrap_servers": "broker:9092"}, {"props": {"uri": "https://YOUR_CATALOG"}}),
        ({"bootstrap_servers": "broker:9092", "security": {"sasl.password": "YOUR_PASSWORD"}}, {"props": {}}),
    ],
)
def test_external_site_refuses_placeholders_before_cloud_access(
    aws_environment: dict[str, str], tmp_path: Path, kafka: dict[str, object], catalog: dict[str, object]
) -> None:
    source = {"kafka": {"deployment": "external", **kafka}, "catalog": catalog}
    Path(aws_environment["SITE_FILE"]).write_text(yaml.safe_dump(source))
    result = subprocess.run(
        ["bash", str(AWS / "setup.sh"), "--write-site", str(tmp_path / "generated.yaml")],
        env=aws_environment,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0
    assert "YOUR_" in result.stderr
    assert not Path(aws_environment["CALLS"]).exists()
    assert not (tmp_path / "generated.yaml").exists()
