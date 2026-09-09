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

import ast
import importlib
import inspect
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import cast

import pytest
import yaml

from ingest_bench.specs.derive import TABLE_NAMESPACE
from ingest_bench.specs.model import KubernetesConfig, load_site

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = REPO_ROOT / "scripts"
AWS_DEPLOY = REPO_ROOT / "deploy" / "aws"
SMOKE = SCRIPTS / "smoke.sh"
GEN_CORPUS = SCRIPTS / "gen-corpus.sh"
PUSH_IMAGES = SCRIPTS / "push-images.sh"
STAGE = SCRIPTS / "stage.sh"
LAUNCH = SCRIPTS / "launch.sh"
GATE = SCRIPTS / "gate.sh"
TEARDOWN = SCRIPTS / "teardown.sh"
FINISH = SCRIPTS / "finish.sh"
AWS_SETUP = AWS_DEPLOY / "setup.sh"
AWS_TEARDOWN = AWS_DEPLOY / "teardown.sh"
SITE_AWS_EXAMPLE = REPO_ROOT / "site.aws.example.yaml"

# The two names a pod's region is rendered under, in the order a driver writes
# them. Java's SDK reads the first, botocore only the second.
REGION_ENV_NAMES = ("AWS_REGION", "AWS_DEFAULT_REGION")

needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")

# The drivers read the site with `yq` and a run's facts with `jq`, and neither
# is a Python dependency — so a machine without them can still run the rest of
# this file rather than failing on a missing tool the harness never needs.
needs_shell_tools = pytest.mark.skipif(
    any(shutil.which(tool) is None for tool in ("bash", "jq", "yq", "git")),
    reason="the cluster drivers read the site and a run's facts with jq, yq and git",
)

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


# The first and last lines of the Kafka-version choice in `setup.sh`, so the
# block can be lifted out and run on its own. Anchors rather than a copy: a
# copy would keep passing after the script's own version of it broke.
_VERSION_CHOICE_FIRST = 'MSK_KAFKA_VERSION="$(tr '
_VERSION_CHOICE_LAST = "set MSK_KAFKA_VERSION yourself"


def _version_choice_block() -> str:
    lines = AWS_SETUP.read_text().splitlines()
    starts = [index for index, line in enumerate(lines) if _VERSION_CHOICE_FIRST in line]
    ends = [index for index, line in enumerate(lines) if _VERSION_CHOICE_LAST in line]
    assert len(starts) == 1 and len(ends) == 1, "setup.sh no longer holds one Kafka-version choice to lift out"
    assert starts[0] < ends[0]
    return "\n".join(lines[starts[0] : ends[0] + 1])


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
    sourced = sorted(set(_shell_files()) - set(entrypoints))
    assert [path.name for path in sourced] == ["_k8s.sh", "_lib.sh"], "these are the two sourced libraries"
    for script in entrypoints:
        assert os.access(script, os.X_OK), f"{script} is not executable"
    for script in sourced:
        # A sourced file that is executable invites being run, and neither of
        # these does anything on its own but set variables the caller needs.
        assert not os.access(script, os.X_OK), f"{script} is sourced, so it should not be executable"


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
@pytest.mark.parametrize("script", [GEN_CORPUS, PUSH_IMAGES, STAGE, LAUNCH, GATE, TEARDOWN, FINISH])
def test_a_cluster_driver_answers_before_it_reads_a_site(script: Path) -> None:
    """`--help` and an unknown argument, with no site config and no cluster.

    These drivers are read before they are run, and refusing to say what they
    do until a site config exists would be refusing the first question anyone
    asks of them.
    """
    out = subprocess.run([str(script), "--help"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "--site PATH" in out.stdout

    refused = subprocess.run([str(script), "--warmup"], capture_output=True, text=True)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --warmup" in refused.stderr


@needs_bash
def test_gen_corpus_refuses_bad_arguments_before_it_needs_a_cluster() -> None:
    """A non-numeric `--shards` renders a Job with a completions field of 'two'.

    Both refusals happen before the site config is read, because the values
    reach a manifest and a rejected Job is a slower way to learn the same
    thing.
    """
    out = subprocess.run([str(GEN_CORPUS), "smoke", "--shards", "two"], capture_output=True, text=True)
    assert out.returncode == 1, out.stdout
    assert "--shards must be a positive integer" in out.stderr

    both = subprocess.run([str(GEN_CORPUS), "smoke", "events-100mbs-skew"], capture_output=True, text=True)
    assert both.returncode == 1, both.stdout
    assert "takes one preset" in both.stderr


def test_gen_corpus_shards_each_shard_into_its_own_prefix() -> None:
    """A sharded corpus needs a `--out` per shard, and one shard needs none.

    Every shard of a preset writes a directory of the same name, so shards
    sharing one `--out` would overwrite each other's metadata. The script
    cannot be run without a cluster, so what is checked is the command it
    renders into the Job.
    """
    text = GEN_CORPUS.read_text()
    assert "--out $CORPUS_ROOT/shards/\\$JOB_COMPLETION_INDEX" in text
    assert "--shard-index \\$JOB_COMPLETION_INDEX --shard-count $SHARDS" in text
    assert "gen-corpus --preset $PRESET --out $CORPUS_ROOT --seed $SEED" in text, "one shard writes to the root"
    for template in ("corpus-gen-job.yaml.tmpl", "harness-job.yaml.tmpl"):
        assert f"deploy/k8s/{template}" in text
        assert (REPO_ROOT / "deploy" / "k8s" / template).exists()


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


@needs_bash
@pytest.mark.parametrize(
    "offered, chosen",
    [
        # The newest plain 3.x wins, and 10 is newer than 6 rather than sorting
        # before it. A `.tiered` variant is a different storage mode, so it is
        # not what an unset knob should pick even though it sorts higher.
        ("3.6.0\t3.10.0\t3.6.0.tiered\t2.8.1", "3.10.0"),
        ("3.6.0", "3.6.0"),
        # No plain 3.x at all: the script must reach its own refusal.
        ("2.8.1\t4.0.0", None),
        ("", None),
    ],
)
def test_the_kafka_version_choice_reaches_its_refusal(offered: str, chosen: str | None) -> None:
    """Run `setup.sh`'s own version-selection lines against a fixed answer from MSK.

    The whole path is unreachable from the guard tests — it sits behind a live
    account — so the block is lifted out of the script and run on its own. It is
    worth running rather than reading because the filter is a pipeline inside an
    assignment: under `pipefail` an unguarded one aborts the script the moment
    `grep` matches nothing, which silently skipped the refusal below it.
    """
    harness = f"""
        set -euo pipefail
        log() {{ printf 'log %s\\n' "$*"; }}
        die() {{ printf 'die %s\\n' "$*"; exit 3; }}
        KAFKA_VERSIONS="{offered}"
{_version_choice_block()}
        printf 'chose %s\\n' "$MSK_KAFKA_VERSION"
    """
    out = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
    if chosen is None:
        assert out.returncode == 3, f"expected the refusal, got {out.returncode}: {out.stdout}{out.stderr}"
        assert "die no ACTIVE 3.x Kafka version" in out.stdout, out.stdout + out.stderr
    else:
        assert out.returncode == 0, out.stdout + out.stderr
        assert f"chose {chosen}" in out.stdout, out.stdout + out.stderr


def test_the_operator_chart_comes_from_the_archive_at_the_pinned_version() -> None:
    """`downloads.apache.org` carries only the current releases, so a pin 404s there.

    That is not a failure a pinned version can avoid by being new: it becomes
    one the day the next release lands, and `setup.sh` then refuses a cluster it
    was working on the day before. `archive.apache.org` keeps every release,
    current ones included. The version has to reach the URL from the variable
    too — a second one written into the URL would install a chart the log names
    wrongly.
    """
    setup = AWS_SETUP.read_text()
    urls = re.findall(r'"(https://\S*flink-kubernetes-operator\S*)"', setup)
    assert urls == ["https://archive.apache.org/dist/flink/flink-kubernetes-operator-$FLINK_OPERATOR_VERSION/"], urls
    assert 'FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-' in setup, "the pin should be overridable"


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
    assert _actions(objects) == {"s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"}


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
    `cluster` is its only resource type, so it belongs on the cluster ARN,
    not the topic ARN alongside `WriteData`.
    """
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "harness-policy.json"))

    topics = _one(statements, {IAM_VALUES["MSK_TOPIC_ARN"]}, "the topics")
    actions = _actions(topics)
    assert "kafka-cluster:WriteDataIdempotently" not in actions
    assert {"kafka-cluster:WriteData", "kafka-cluster:ReadData", "kafka-cluster:CreateTopic"} <= actions

    cluster = _one(statements, {IAM_VALUES["MSK_ARN"]}, "the cluster")
    assert {"kafka-cluster:Connect", "kafka-cluster:WriteDataIdempotently"} <= _actions(cluster)

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
    # without. The verbs are enumerated rather than `*`, so a Role that widens
    # to a wildcard is a failure and not a silent grant of everything the API
    # group ever gains.
    role = by_kind["Role"][0]
    granted: set[tuple[str, str]] = set()
    for entry in _sequence(role["rules"]):
        rule = _mapping(entry)
        verbs = {str(verb) for verb in _sequence(rule["verbs"])}
        assert verbs == {"get", "list", "watch", "create", "update", "patch", "delete"}, verbs
        for group in _sequence(rule["apiGroups"]):
            for resource in _sequence(rule["resources"]):
                granted.add((str(group), str(resource)))
    assert granted == {
        ("", "pods"),
        ("", "configmaps"),
        ("apps", "deployments"),
        ("apps", "deployments/finalizers"),
    }

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
    # Compared whole rather than field by field: this block is the one part of
    # the example a driver reads attribute by attribute, so a key that does not
    # survive loading is a pod with no identity, registry or region.
    assert site.kubernetes == KubernetesConfig(
        context="a-cluster",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        service_account_annotations={},
        registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        aws_region="eu-west-1",
        node_selector={},
        tolerations=[],
    )


@needs_shell_tools
@pytest.mark.parametrize("region", ["eu-west-1", None])
def test_the_job_env_names_the_region_under_both_names_an_sdk_reads(tmp_path: Path, region: str | None) -> None:
    """A pod's region has to reach botocore as well as Java's SDK.

    Java's reads `AWS_REGION`; botocore reads `AWS_DEFAULT_REGION` alone and
    treats `AWS_REGION` as a hint for something else, so a pod given only that
    name has an S3 client with no region — which resolves the global endpoint
    and is refused for a bucket that lives anywhere else. A cluster off AWS
    names no region and gets neither variable rather than an empty one.
    """
    site = _filled_site()
    if region is None:
        site = "".join(line for line in site.splitlines(keepends=True) if "aws_region" not in line)
    site_file = tmp_path / "site.yaml"
    site_file.write_text(site)
    program = "\n".join(
        [
            "set -euo pipefail",
            "PREREQ_DOC=deploy/aws/README.md",
            "source scripts/_lib.sh",
            f"SITE_FILE={site_file}",
            "source scripts/_k8s.sh",
            "site_env_json",
        ]
    )
    out = subprocess.run(["bash", "-c", program], cwd=REPO_ROOT, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    expected = [] if region is None else [{"name": name, "value": region} for name in REGION_ENV_NAMES]
    assert json.loads(out.stdout) == expected


def test_the_shell_calls_the_harness_with_arguments_it_takes() -> None:
    """An inline `python -c` is a call site neither mypy nor a test would see.

    `measure-producer.sh` reaches into `kafka_admin` directly, so when those
    functions gained the client properties every call takes, that script kept
    passing the old arity — and nothing short of running it could say so. Every
    call whose arguments are literals is bound against the real signature here.
    """
    checked = 0
    for script in _shell_entrypoints():
        for snippet in re.findall(r"python -c '(.*?)'", script.read_text()):
            program = ast.parse(snippet.replace('\\"', '"'))
            modules = {
                alias.asname or alias.name: importlib.import_module(f"ingest_bench.{alias.name}")
                for node in ast.walk(program)
                if isinstance(node, ast.ImportFrom) and node.module == "ingest_bench"
                for alias in node.names
            }
            for call in ast.walk(program):
                if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
                    continue
                target = call.func.value
                if not isinstance(target, ast.Name) or target.id not in modules:
                    continue
                try:
                    args = [ast.literal_eval(argument) for argument in call.args]
                    kwargs = {str(word.arg): ast.literal_eval(word.value) for word in call.keywords}
                except ValueError:
                    # An argument computed rather than written down; its own
                    # call is checked on its own turn through this loop.
                    continue
                function = getattr(modules[target.id], call.func.attr)
                signature = inspect.signature(function)
                signature.bind(*args, **kwargs)  # raises TypeError on an argument it does not take
                checked += 1
    assert checked >= 2, f"expected to find harness calls in the shell to check, bound {checked}"


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


# ---------------------------------------------------------------------------
# The cluster drivers, against a stub kubectl and aws
# ---------------------------------------------------------------------------

# Every invocation is recorded and then answered, so the drivers' own parsing —
# the run id off a Job's log, the epoch arithmetic, the command lines the Jobs
# carry — is exercised with no cluster and no account. Anything not matched here
# answers nothing and succeeds, which is what `kubectl apply` and `delete` do.
KUBECTL_STUB = """
printf '%s\\n' "$*" >>"$STUB_LOG"
case "$*" in
*"apply -f -")
	count=$(find "$STUB_APPLIED_DIR" -type f | wc -l | tr -d ' ')
	cat >"$STUB_APPLIED_DIR/$count.yaml"
	;;
*"create configmap"*) printf 'apiVersion: v1\\nkind: ConfigMap\\nmetadata:\\n  name: stub\\n' ;;
*"get job/"*) printf 'Complete\\n' ;;
*"logs job/scorer-"*) printf 'POLL t=0.1 prefix=0/0\\n' ;;
*"logs job/"*) cat "$STUB_JOB_LOG" ;;
*"get flinkdeployment/"*) printf '%s\\n' "${STUB_FLINK_STATE:-RUNNING}" ;;
esac
"""

# `s3 sync` stands in for the run directory the stage Job published; every other
# subcommand succeeds silently.
AWS_STUB = """
printf '%s\\n' "$*" >>"$STUB_AWS_LOG"
if [[ ${1:-} == s3 && ${2:-} == sync ]]; then
	mkdir -p "$4"
	cp -R "$STUB_STAGE_DIR/." "$4"
fi
"""

RUN_ID = "smoke-flink-20260908T120000Z"
# What the same run's Kubernetes objects are named, since an RFC 1123 name is
# lowercase and the stamp in a run id is not.
RUN_OBJECT = RUN_ID.lower()
BOOTSTRAP = SITE_AWS_FILLINGS["YOUR_MSK_IAM_BOOTSTRAP"] + ":9098"

# What the stage Job printed, in the shape `stage` prints it: the run id first,
# because that is the line the driver reads.
STAGE_JOB_LOG = f"""run_id: {RUN_ID}
bootstrap: {BOOTSTRAP}
topic: {RUN_ID}
table: ingest_bench.t_smoke_flink_20260908T120000Z
run_dir: /work/runs/{RUN_ID}
"""

FACTS = {
    "run_id": RUN_ID,
    "bootstrap": BOOTSTRAP,
    "topic": RUN_ID,
    "corpus_uri": "s3://a-bucket/corpus/smoke-1a2b3c4d",
    "table": "ingest_bench.t_smoke_flink_20260908T120000Z",
    "key_column": "user_id",
    "epoch": None,
}


def _filled_site() -> str:
    """The shipped AWS example with its placeholders filled, as an operator's own."""
    text = SITE_AWS_EXAMPLE.read_text()
    for placeholder, value in SITE_AWS_FILLINGS.items():
        text = text.replace(placeholder, value)
    return text


def _stub_bin(directory: Path, programs: dict[str, str]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for name, body in programs.items():
        stub = directory / name
        stub.write_text(f"#!/usr/bin/env bash\nset -euo pipefail\n{body}")
        stub.chmod(0o755)
    return directory


@dataclass(frozen=True)
class DriverRun:
    """One driver run against the stubs, and everything it left behind."""

    result: subprocess.CompletedProcess[str]
    calls: str
    aws_calls: str
    applied: list[dict[str, object]]


def _run_driver(
    script: Path,
    arguments: list[str],
    tmp_path: Path,
    environment: dict[str, str],
    site: str | None = None,
    programs: dict[str, str] | None = None,
) -> DriverRun:
    """Run one driver in its own working directory with `kubectl` and `aws` stubbed.

    The working directory is the operator's: `./site.yaml` and `./runs` are
    resolved against it, so nothing here writes into the checkout. ``programs``
    stubs a harness command as well, which shadows the installed one because
    the stub directory is first on `PATH`.
    """
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    (work / "site.yaml").write_text(site if site is not None else _filled_site())
    applied_dir = tmp_path / "applied"
    applied_dir.mkdir(exist_ok=True)
    calls = tmp_path / "kubectl-calls.log"
    calls.touch()
    aws_calls = tmp_path / "aws-calls.log"
    aws_calls.touch()
    job_log = tmp_path / "job.log"
    job_log.write_text(STAGE_JOB_LOG)
    stubs = _stub_bin(tmp_path / "bin", {"kubectl": KUBECTL_STUB, "aws": AWS_STUB, **(programs or {})})

    result = subprocess.run(
        [str(script), *arguments],
        cwd=work,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "STUB_LOG": str(calls),
            "STUB_AWS_LOG": str(aws_calls),
            "STUB_APPLIED_DIR": str(applied_dir),
            "STUB_JOB_LOG": str(job_log),
            **environment,
        },
    )
    documents = [
        _mapping(yaml.safe_load(path.read_text()))
        for path in sorted(applied_dir.glob("*.yaml"), key=lambda path: int(path.stem))
    ]
    return DriverRun(result=result, calls=calls.read_text(), aws_calls=aws_calls.read_text(), applied=documents)


def _job_command(document: dict[str, object]) -> str:
    """The single argument a Job's container carries, which is its whole command line."""
    containers = _sequence(_pod_spec(document)["containers"])
    return str(_sequence(_mapping(containers[0])["args"])[0])


def _pod_spec(document: dict[str, object]) -> dict[str, object]:
    return _mapping(_mapping(_mapping(document["spec"])["template"])["spec"])


@needs_shell_tools
def test_stage_reads_the_run_id_off_the_jobs_log_and_then_starts_the_engine(tmp_path: Path) -> None:
    """The run id comes from the Job, and the engine's documents come from the bucket.

    Only staging knows the run id — the stamp in it is the moment staging ran —
    so a driver that derived it a second time would name a different run every
    time the two calls straddled a second.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")

    spec = REPO_ROOT / "runs" / "smoke-flink.yaml"
    run = _run_driver(
        STAGE,
        [str(spec), "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_STAGE_DIR": str(staged)},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout.splitlines()[-1] == f"run_id: {RUN_ID}"

    fetched = tmp_path / "work" / "runs" / RUN_ID
    assert json.loads((fetched / "facts.json").read_text())["topic"] == RUN_ID
    assert run.aws_calls.strip() == f"s3 sync s3://a-bucket/runs/{RUN_ID}/stage/ ./runs/{RUN_ID}/"

    # Every call names the cluster and the namespace the site declares: a
    # `kubectl` that fell back to the caller's current context would apply a
    # run to whichever cluster was last selected.
    for line in run.calls.splitlines():
        assert line.startswith("--context a-cluster --namespace ingest-bench "), line

    # The spec and the site reach the Job as ConfigMaps under the names its
    # mounts expect, and both are deleted once the run directory is fetched.
    assert f"--from-file smoke-flink.yaml={spec}" in run.calls
    assert "--from-file site.yaml=./site.yaml" in run.calls
    for deleted in ("delete job stage-smoke-flink", "configmap stage-smoke-flink-spec", "stage-smoke-flink-site"):
        assert deleted in run.calls, deleted

    # The engine is started from the documents the run directory carries, the
    # ConfigMap first because the deployment mounts it.
    engine_applies = [line for line in run.calls.splitlines() if " apply -f ./runs/" in line]
    assert [line.rsplit("/", 1)[-1] for line in engine_applies] == [
        "flink-job-configmap.yaml",
        "flinkdeployment.yaml",
    ]
    assert f"get flinkdeployment/{RUN_OBJECT}" in run.calls

    jobs = [document for document in run.applied if document["kind"] == "Job"]
    assert len(jobs) == 1, "staging applies one Job"
    command = _job_command(jobs[0])
    assert "stage --spec /runs/smoke-flink.yaml --site /site/site.yaml --runs-dir /work/runs" in command
    assert "--image-tag abc1234" in command
    assert "--upload-prefix s3://a-bucket/runs" in command


@needs_shell_tools
@pytest.mark.parametrize("lead", [None, 42])
def test_launch_dates_the_epoch_ahead_of_itself_and_records_it(tmp_path: Path, lead: int | None) -> None:
    """The epoch is in the future by the lead, and the run directory says which.

    A first batch already due when the producer opened its first connection is
    acked late, and a late ack is read as the offer rather than the engine
    setting the rate — which voids the run. The lead is what buys a cold node
    and an image pull.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    before = int(time.time())
    run = _run_driver(
        LAUNCH,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {} if lead is None else {"EPOCH_LEAD_S": str(lead)},
    )
    after = int(time.time())
    assert run.result.returncode == 0, run.result.stderr

    expected_lead = 180 if lead is None else lead
    epoch = int(json.loads((run_dir / "facts.json").read_text())["epoch"])
    assert before + expected_lead <= epoch <= after + expected_lead
    assert (run_dir / "timeline.log").read_text().splitlines()[-1].endswith(f" launched epoch={epoch}")

    # The scorer is applied first: a producer publishing before the table was
    # read would have rows committed by the first sample, and the keep-up curve
    # would start part way up.
    assert [str(_mapping(document["metadata"])["name"]) for document in run.applied] == [
        f"scorer-{RUN_OBJECT}",
        f"producer-{RUN_OBJECT}",
    ]
    scorer, producer = (_job_command(document) for document in run.applied)

    assert f"--epoch {epoch}" in scorer and f"--epoch {epoch}" in producer
    assert f"--publish-logs s3://a-bucket/runs/{RUN_ID}/producer" in scorer
    assert f"--out /work/scores --upload-prefix s3://a-bucket/runs/{RUN_ID}/scores" in scorer
    assert "--idle-stop-s 600 --publish-shards 1" in scorer
    # Every catalog property the site declares, because the scorer reads the
    # table itself and no site config reaches a pod.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in scorer
    assert "--catalog-prop warehouse=123456789012" in scorer
    # The spec's scoring keys, so the run scored is the run the spec asks for.
    assert "--warmup-s 60" in scorer and "--freshness-bound-s 60" in scorer

    assert f"--topic {RUN_ID}" in producer
    assert "--shard $JOB_COMPLETION_INDEX --shards 1" in producer
    assert "--publish-log /work/publish_log-$JOB_COMPLETION_INDEX.jsonl" in producer
    assert f"--upload-prefix s3://a-bucket/runs/{RUN_ID}" in producer
    assert "--key-column user_id" in producer
    # The MSK IAM properties, the harness's own signing region among them.
    assert "--kafka-prop security.protocol=SASL_SSL" in producer
    assert "--kafka-prop aws.region=eu-west-1" in producer

    assert _mapping(run.applied[1]["spec"])["completions"] == 1


# `table-metadata` as the driver calls it, answering whatever the case under
# test needs: a location, the absent-table code, or a failure of its own.
TABLE_METADATA_STUB = """
printf '%s\\n' "$*" >>"$STUB_METADATA_LOG"
[[ -z ${STUB_METADATA_ERROR:-} ]] || printf '%s\\n' "$STUB_METADATA_ERROR" >&2
[[ -z ${STUB_METADATA_OUT:-} ]] || printf '%s\\n' "$STUB_METADATA_OUT"
exit "${STUB_METADATA_STATUS:-0}"
"""


@needs_shell_tools
def test_launch_refuses_a_site_whose_properties_it_cannot_read(tmp_path: Path) -> None:
    """A site `yq` cannot flatten stops the launch instead of dropping the properties.

    The catalog properties and the Kafka security block are the only way a pod
    is told how to reach either service, so a reader that answered "no
    properties" would apply a scorer that cannot open the table and a producer
    that cannot authenticate — minutes of pods to say what this says at once.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())

    # A nested map under a properties block: `yq` refuses to concatenate it into
    # `key=value` and exits non-zero.
    broken = _filled_site().replace(
        "    rest.signing-name: glue\n",
        "    rest.signing-name: glue\n    nested:\n      deeper: value\n",
    )
    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {}, site=broken)

    assert run.result.returncode != 0
    assert "could not read catalog.props out of ./site.yaml" in run.result.stderr
    assert run.applied == [], "nothing may be applied once the site cannot be read"


@needs_shell_tools
@pytest.mark.parametrize(
    "status, copied, refused",
    [
        (0, True, False),
        # The absent-table code: a run that failed before it created its table
        # has no document, and a teardown converges over that.
        (3, False, False),
        # Any other failure is a catalog this machine could not reach, which
        # says nothing about whether the document exists.
        (1, False, True),
    ],
)
def test_teardown_copies_a_metadata_document_or_says_why_it_could_not(
    tmp_path: Path, status: int, copied: bool, refused: bool
) -> None:
    """An absent table is not the same answer as an unreachable catalog.

    Reading every non-zero exit as "the table was never created" would report a
    teardown as clean while an expired credential, a missing harness or an
    unreachable catalog quietly cost the run its last artifact.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")
    (run_dir / "flink-job-configmap.yaml").write_text("# flink-job-configmap.yaml\n")
    metadata_calls = tmp_path / "table-metadata-calls.log"
    metadata_calls.touch()

    location = "s3://a-bucket/warehouse/ingest_bench/t_x/metadata/00003-abc.metadata.json"
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_METADATA_LOG": str(metadata_calls),
            "STUB_METADATA_STATUS": str(status),
            "STUB_METADATA_OUT": location if status == 0 else "",
            "STUB_METADATA_ERROR": "" if status == 0 else "could not reach the catalog",
        },
        programs={"table-metadata": TABLE_METADATA_STUB},
    )

    # Everything destructive happens before the document is read, so it happens
    # whatever the answer was.
    assert f"delete -f ./runs/{RUN_ID}/flinkdeployment.yaml" in run.calls
    assert f"delete job producer-{RUN_OBJECT}" in run.calls and f"delete job scorer-{RUN_OBJECT}" in run.calls
    drop = [document for document in run.applied if document["kind"] == "Job"]
    assert len(drop) == 1
    command = _job_command(drop[0])
    assert f"drop-topic --bootstrap {BOOTSTRAP} --topic {RUN_ID}" in command
    assert "--kafka-prop security.protocol=SASL_SSL" in command

    # The catalog is addressed with every property the site declares.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in metadata_calls.read_text()

    assert (f"s3 cp {location}" in run.aws_calls) is copied
    assert (run.result.returncode != 0) is refused
    if refused:
        assert "table-metadata exited 1" in run.result.stderr
        assert "could not reach the catalog" in run.result.stderr


@needs_shell_tools
def test_a_runs_kubernetes_objects_are_addressed_in_lower_case(tmp_path: Path) -> None:
    """Every object a teardown deletes is named by the lowercased run id.

    An RFC 1123 subdomain is lowercase and the `T`/`Z` in a run id's stamp are
    not, so an object named by the id as it stands is refused by the API server
    rather than by anything a driver can see. The topic and the run directory
    are the published identifier and stay as they are, which is why the two
    spellings have to be told apart per use rather than once per run.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")

    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_METADATA_LOG": str(tmp_path / "metadata.log"), "STUB_METADATA_STATUS": "3"},
        programs={"table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr

    for deleted in ("producer", "scorer", "drop-topic"):
        assert f"delete job {deleted}-{RUN_OBJECT}" in run.calls, deleted
        assert f"delete job {deleted}-{RUN_ID}" not in run.calls, deleted
    drop = [document for document in run.applied if document["kind"] == "Job"]
    assert [str(_mapping(document["metadata"])["name"]) for document in drop] == [f"drop-topic-{RUN_OBJECT}"]

    # The topic and the run directory are the run's published identifier, and
    # neither is a name Kubernetes reads.
    assert f"--topic {RUN_ID}" in _job_command(drop[0])
    assert f"delete -f ./runs/{RUN_ID}/flinkdeployment.yaml" in run.calls


@needs_shell_tools
def test_an_engine_that_failed_is_tailed_under_its_lower_case_name(tmp_path: Path) -> None:
    """The one place a driver reads the operator's own Deployment by name.

    A tail under the un-lowercased name answers "not found" and the refusal
    carries no jobmanager log, which is the whole of what says why the engine
    never ran.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_STAGE_DIR": str(staged), "STUB_FLINK_STATE": "FAILED"},
    )
    assert run.result.returncode != 0
    assert f"flinkdeployment/{RUN_OBJECT} went to FAILED" in run.result.stderr
    assert f"logs deploy/{RUN_OBJECT} --tail=40" in run.calls
