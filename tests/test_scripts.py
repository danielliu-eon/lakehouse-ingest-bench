"""The scripts have to parse, and their documents have to render, before a cluster exists.

A syntax error in `smoke.sh` would otherwise surface only in the compose smoke,
which is opt-in on a pull request and takes tens of minutes — so it would reach
`main` and fail there. `bash -n` and the two argument paths that need no Docker
are the whole of what can be checked without a stack, and they are the cheap
half of every mistake actually made in a shell script.

The AWS setup scripts cannot be exercised at all without an account, so what is
checked here is everything they *feed* to `aws` and `kubectl`: the IAM documents
render to policies whose statements say what they are meant to say, the
namespace manifest renders to the objects the pods need, and the example site
config an operator copies loads. A wrong action in a policy document surfaces
otherwise as a denial minutes into a run on a live cluster.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from string import Template
from typing import cast

import pytest
import yaml

from ingest_bench.specs.derive import TABLE_NAMESPACE
from ingest_bench.specs.model import load_site

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = REPO_ROOT / "scripts"
AWS_DEPLOY = REPO_ROOT / "deploy" / "aws"
SMOKE = SCRIPTS / "smoke.sh"
AWS_SETUP = AWS_DEPLOY / "setup.sh"
AWS_TEARDOWN = AWS_DEPLOY / "teardown.sh"
SITE_AWS_EXAMPLE = REPO_ROOT / "site.aws.example.yaml"

needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")

# What `setup.sh` exports before it renders the IAM documents with envsubst. The
# values are shaped like the real ones: a document that only renders with a
# placeholder left in it would not be a policy AWS accepts.
IAM_VALUES = {
    "ACCOUNT": "123456789012",
    "REGION": "eu-west-1",
    "BUCKET": "a-bucket",
    "MSK_ARN": "arn:aws:kafka:eu-west-1:123456789012:cluster/a-cluster/aaaa-bbbb-1",
    "MSK_TOPIC_ARN": "arn:aws:kafka:eu-west-1:123456789012:topic/a-cluster/aaaa-bbbb-1/*",
    "MSK_GROUP_ARN": "arn:aws:kafka:eu-west-1:123456789012:group/a-cluster/aaaa-bbbb-1/*",
}

# Every YOUR_ placeholder in `site.aws.example.yaml`, and something plausible to
# put in its place. Substituting by name rather than by pattern is what makes a
# new placeholder a test failure instead of an untested line.
SITE_AWS_FILLINGS = {
    "YOUR_BUCKET": "a-bucket",
    "YOUR_MSK_IAM_BOOTSTRAP": "b-1.a-cluster.abc123.c2.kafka.eu-west-1.amazonaws.com",
    "YOUR_REGION": "eu-west-1",
    "YOUR_ACCOUNT_ID": "123456789012",
    "YOUR_KUBE_CONTEXT": "a-cluster",
}


def _shell_files() -> list[Path]:
    return sorted(SCRIPTS.glob("*.sh")) + sorted(AWS_DEPLOY.glob("*.sh"))


def _shell_entrypoints() -> list[Path]:
    """The scripts meant to be run, which is every one but the sourced library."""
    return [path for path in _shell_files() if not path.name.startswith("_")]


def _iam_documents() -> list[Path]:
    return sorted((AWS_DEPLOY / "iam").glob("*.json"))


def _mapping(value: object) -> dict[str, object]:
    """``value`` as a mapping, for reading parsed YAML and JSON under strict typing."""
    assert isinstance(value, dict), f"expected a mapping, got {type(value).__name__}"
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def _sequence(value: object) -> list[object]:
    assert isinstance(value, list), f"expected a list, got {type(value).__name__}"
    return cast(list[object], value)


def _rendered_policy(path: Path) -> dict[str, object]:
    """``path`` rendered the way ``setup.sh`` renders it, parsed.

    ``string.Template`` takes the same ``${NAME}`` syntax as ``envsubst`` and
    raises on a placeholder the mapping has no value for, so this is also the
    check that a document references nothing the script does not export.
    """
    rendered = Template(path.read_text()).substitute(IAM_VALUES)
    assert "${" not in rendered, f"{path.name} still holds an unrendered placeholder"
    return _mapping(json.loads(rendered))


def _statements(document: dict[str, object]) -> list[dict[str, object]]:
    return [_mapping(statement) for statement in _sequence(document["Statement"])]


def _one(statements: list[dict[str, object]], resources: set[str], what: str) -> dict[str, object]:
    """The single statement whose resources are exactly ``resources``."""
    matching = [statement for statement in statements if _resources(statement) == resources]
    assert len(matching) == 1, f"expected one statement for {what}, found {len(matching)}"
    return matching[0]


def _one_or_many(value: object) -> set[str]:
    """An IAM ``Action`` or ``Resource``, which is either one string or a list of them."""
    if isinstance(value, str):
        return {value}
    return {str(entry) for entry in _sequence(value)}


def _actions(statement: dict[str, object]) -> set[str]:
    return _one_or_many(statement["Action"])


def _resources(statement: dict[str, object]) -> set[str]:
    return _one_or_many(statement["Resource"])


# ---------------------------------------------------------------------------
# The shell itself
# ---------------------------------------------------------------------------


@needs_bash
def test_every_script_parses() -> None:
    scripts = _shell_files()
    assert scripts, f"no shell scripts under {SCRIPTS} or {AWS_DEPLOY}"
    for script in scripts:
        check = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
        assert check.returncode == 0, f"{script.name}: {check.stderr}"


def test_every_script_is_executable() -> None:
    entrypoints = _shell_entrypoints()
    assert len(entrypoints) == len(_shell_files()) - 1, "one sourced library was expected, and it is not executable"
    for script in entrypoints:
        assert os.access(script, os.X_OK), f"{script} is not executable"


@needs_bash
def test_help_needs_no_stack() -> None:
    """`--help` has to answer before the host-tool check, on any machine.

    It is the one thing a reader runs first, and refusing it for a missing `yq`
    would be refusing to say what the script does.
    """
    out = subprocess.run([str(SMOKE), "--help"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "--external-ready-file" in out.stdout


@needs_bash
def test_an_unknown_argument_is_refused() -> None:
    out = subprocess.run([str(SMOKE), "--warmup"], capture_output=True, text=True)
    assert out.returncode == 2, out.stdout
    assert "unknown argument --warmup" in out.stderr


@needs_bash
def test_teardown_takes_its_argument_before_it_needs_an_account() -> None:
    """`--all` deletes a corpus, so its meaning has to be readable with nothing set.

    Also the check that argument parsing runs before the required-variable
    refusals: a reader asking what the flag does should not have to name a
    cluster first.
    """
    environment = {key: value for key, value in os.environ.items() if key not in ("AWS_REGION", "CLUSTER_NAME")}
    out = subprocess.run([str(AWS_TEARDOWN), "--help"], capture_output=True, text=True, env=environment)
    assert out.returncode == 0, out.stderr
    assert "--all" in out.stdout

    refused = subprocess.run([str(AWS_TEARDOWN), "--everything"], capture_output=True, text=True, env=environment)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --everything" in refused.stderr


def test_the_setup_script_renders_only_the_placeholders_it_exports() -> None:
    """The envsubst argument in `setup.sh` and the documents' variables are one list.

    envsubst given a restricted list silently leaves out anything not in it, so
    a placeholder added to a document and not to the script would reach IAM
    verbatim and be refused as a malformed ARN.
    """
    setup = AWS_SETUP.read_text()
    for path in _iam_documents():
        for name in Template(path.read_text()).get_identifiers():
            assert name in IAM_VALUES, f"{path.name} uses ${{{name}}}, which this test does not know about"
            assert f"${{{name}}}" in setup, f"{path.name} uses ${{{name}}}, which setup.sh never gives envsubst"


# ---------------------------------------------------------------------------
# The IAM documents
# ---------------------------------------------------------------------------


def test_every_iam_document_renders_to_a_policy() -> None:
    documents = _iam_documents()
    assert documents, f"no IAM documents under {AWS_DEPLOY / 'iam'}"
    for path in documents:
        document = _rendered_policy(path)
        assert document["Version"] == "2012-10-17", path.name
        for statement in _statements(document):
            assert statement["Effect"] == "Allow", f"{path.name}: {statement['Sid']} is not an Allow"


def test_the_trust_document_grants_assume_role_and_tag_session() -> None:
    """Pod Identity needs both.

    The agent tags the session it hands the pod, so a trust policy with only
    `sts:AssumeRole` fails at credential-vending time — after the pod is
    running, as an unattributed AccessDenied from inside a cloud SDK.
    """
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "trust.json"))
    pod_identity = [s for s in statements if s["Principal"] == {"Service": "pods.eks.amazonaws.com"}]
    assert len(pod_identity) == 1, statements
    assert _actions(pod_identity[0]) == {"sts:AssumeRole", "sts:TagSession"}


def test_the_harness_policy_covers_the_three_bucket_prefixes() -> None:
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "harness-policy.json"))
    bucket = f"arn:aws:s3:::{IAM_VALUES['BUCKET']}"

    listing = _one(statements, {bucket}, "listing the bucket")
    assert "s3:ListBucket" in _actions(listing)

    prefixes = {f"{bucket}/corpus/*", f"{bucket}/runs/*", f"{bucket}/warehouse/*"}
    objects = _one(statements, prefixes, "the objects under the three prefixes")
    assert _actions(objects) == {"s3:GetObject", "s3:PutObject", "s3:DeleteObject"}


def test_the_harness_policy_names_the_table_namespace_the_harness_uses() -> None:
    """The Glue database in the policy is the namespace `derive` creates tables in.

    They are the same name written in two files, and a policy scoped to a
    different database denies `CreateTable` on the first run rather than here.
    """
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "harness-policy.json"))
    region, account = IAM_VALUES["REGION"], IAM_VALUES["ACCOUNT"]
    glue = _one(
        statements,
        {
            f"arn:aws:glue:{region}:{account}:catalog",
            f"arn:aws:glue:{region}:{account}:database/{TABLE_NAMESPACE}",
            f"arn:aws:glue:{region}:{account}:table/{TABLE_NAMESPACE}/*",
        },
        "the Glue catalog, database and tables",
    )
    assert {"glue:GetCatalog", "glue:CreateDatabase", "glue:CreateTable", "glue:UpdateTable"} <= _actions(glue)


def test_the_msk_topic_statement_allows_idempotent_writes() -> None:
    """`WriteDataIdempotently`, without which `InitProducerId` is denied.

    The producer is idempotent, so its very first send fails without this
    action — and the denial names a producer id rather than the policy.
    """
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "harness-policy.json"))

    topics = _one(statements, {IAM_VALUES["MSK_TOPIC_ARN"]}, "the topics")
    actions = _actions(topics)
    assert "kafka-cluster:WriteDataIdempotently" in actions
    assert {"kafka-cluster:WriteData", "kafka-cluster:ReadData", "kafka-cluster:CreateTopic"} <= actions

    cluster = _one(statements, {IAM_VALUES["MSK_ARN"]}, "the cluster")
    assert "kafka-cluster:Connect" in _actions(cluster)

    groups = _one(statements, {IAM_VALUES["MSK_GROUP_ARN"]}, "the consumer groups")
    assert _actions(groups) == {"kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"}


# ---------------------------------------------------------------------------
# The Kubernetes manifest and the eksctl example
# ---------------------------------------------------------------------------


def test_the_namespace_manifest_renders_both_identities_and_the_flink_rbac() -> None:
    template = AWS_DEPLOY / "k8s" / "namespace.yaml.tmpl"
    rendered = Template(template.read_text()).substitute({"NAMESPACE": "a-namespace"})
    objects = [document for document in yaml.safe_load_all(rendered) if document is not None]

    by_kind: dict[str, list[dict[str, object]]] = {}
    for document in objects:
        mapping = _mapping(document)
        by_kind.setdefault(str(mapping["kind"]), []).append(mapping)
    assert sorted(by_kind) == ["Namespace", "Role", "RoleBinding", "ServiceAccount"]
    for kind, documents in by_kind.items():
        for document in documents:
            metadata = _mapping(document["metadata"])
            named = metadata["name"] if kind == "Namespace" else metadata["namespace"]
            assert named == "a-namespace", f"{kind} was rendered into {named!r}"

    # The site example names the accounts a run's pods ask for; a manifest that
    # creates differently named ones leaves every pod unschedulable for want of
    # a ServiceAccount nobody created.
    site = _mapping(_mapping(yaml.safe_load(SITE_AWS_EXAMPLE.read_text()))["kubernetes"])
    accounts = {str(_mapping(document["metadata"])["name"]) for document in by_kind["ServiceAccount"]}
    assert accounts == {site["harness_service_account"], site["flink_service_account"]}

    # The JobManager creates its own TaskManager pods and the ConfigMaps that
    # configure them, so these are the resources a FlinkDeployment cannot start
    # without.
    role = by_kind["Role"][0]
    granted: set[tuple[str, str]] = set()
    for entry in _sequence(role["rules"]):
        rule = _mapping(entry)
        for group in _sequence(rule["apiGroups"]):
            for resource in _sequence(rule["resources"]):
                granted.add((str(group), str(resource)))
    assert {("", "pods"), ("", "configmaps"), ("apps", "deployments")} <= granted

    binding = by_kind["RoleBinding"][0]
    assert _mapping(binding["roleRef"])["name"] == _mapping(role["metadata"])["name"]
    subject = _mapping(_sequence(binding["subjects"])[0])
    assert subject["name"] == site["flink_service_account"]
    assert subject["namespace"] == "a-namespace"


def test_the_eksctl_example_parses_and_holds_its_placeholders() -> None:
    """The example is copied and edited, so it has to parse before it is edited.

    The two placeholders are the whole of what an operator replaces; a third
    value left over from the machine that wrote the file would build a cluster
    somewhere nobody asked for.
    """
    config = yaml.safe_load((AWS_DEPLOY / "eksctl-cluster.example.yaml").read_text())
    assert config["kind"] == "ClusterConfig"
    assert config["metadata"]["name"] == "YOUR_CLUSTER_NAME"
    assert config["metadata"]["region"] == "YOUR_REGION"
    assert "eks-pod-identity-agent" in {addon["name"] for addon in config["addons"]}
    assert len(config["managedNodeGroups"]) == 1
    assert config["managedNodeGroups"][0]["privateNetworking"] is True


# ---------------------------------------------------------------------------
# The example site config
# ---------------------------------------------------------------------------


def test_the_aws_site_example_refuses_its_own_placeholders(tmp_path: Path) -> None:
    copied = tmp_path / "site.yaml"
    copied.write_text(SITE_AWS_EXAMPLE.read_text())
    with pytest.raises(ValueError, match="YOUR_"):
        load_site(copied)


def test_the_aws_site_example_loads_once_every_placeholder_is_filled(tmp_path: Path) -> None:
    text = SITE_AWS_EXAMPLE.read_text()
    # Off the parsed document rather than off the file's text: the header
    # comment names the placeholder convention and would otherwise count as one.
    in_values = set(re.findall(r"YOUR_[A-Z_]+", yaml.safe_dump(yaml.safe_load(text))))
    assert in_values == set(SITE_AWS_FILLINGS), "the example's placeholders and the ones filled here have drifted"
    for placeholder, value in SITE_AWS_FILLINGS.items():
        text = text.replace(placeholder, value)
    copied = tmp_path / "site.yaml"
    copied.write_text(text)

    site = load_site(copied)
    assert site.corpus_root == "s3://a-bucket/corpus"
    assert site.runs_root == "s3://a-bucket/runs"
    assert site.warehouse == "s3://a-bucket/warehouse"
    assert site.kafka_bootstrap.endswith(":9098")
    # MSK IAM is SASL over TLS, and the region is the harness's own key for
    # signing the token rather than a librdkafka property.
    assert site.kafka_security["security.protocol"] == "SASL_SSL"
    assert site.kafka_security["sasl.mechanism"] == "OAUTHBEARER"
    assert site.kafka_security["aws.region"] == "eu-west-1"
    # Glue's `warehouse` is a catalog id, so it stays an account id and the
    # table's location comes from `site.warehouse` instead.
    assert site.catalog_props["warehouse"] == "123456789012"
    assert site.catalog_props["uri"] == "https://glue.eu-west-1.amazonaws.com/iceberg"
    assert site.catalog_props["rest.signing-name"] == "glue"
    assert site.kubernetes["namespace"] == "ingest-bench"
    assert site.kubernetes["registry"] == "123456789012.dkr.ecr.eu-west-1.amazonaws.com"


def test_the_smoke_offers_the_run_the_spec_asks_for() -> None:
    """Every `producer:` key reaches a command line rather than a hardcoded value.

    The staged run directory keeps the spec verbatim as the record of what was
    asked for, so a spec key the script ignores publishes a claim about a run
    that did not happen. Checked as text because the alternative needs Docker,
    a broker and a corpus, which is the compose smoke and not a unit test.
    """
    text = SMOKE.read_text()
    for key in ("speed", "seconds", "behind_max_ms"):
        assert f"yq '.producer.{key}'" in text, f"smoke.sh never reads producer.{key}"
    for flag in ("--speed $SPEED", "--seconds $REPLAY_SECONDS", "--behind-max-ms $BEHIND_MAX_MS"):
        assert flag in text, f"smoke.sh reads a producer key but never passes {flag.split()[0]}"
    assert "--speed 1" not in text, "smoke.sh still hardcodes a replay speed"
