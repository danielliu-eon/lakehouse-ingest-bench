# SPDX-License-Identifier: Apache-2.0
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
import gzip
import importlib
import inspect
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import cast

import pytest
import yaml

from ingest_bench.k8s.render import MARKER_RE, render_template
from ingest_bench.specs import engines
from ingest_bench.specs.derive import TABLE_NAMESPACE
from ingest_bench.specs.kubernetes import FIELDS, NAME, for_name
from ingest_bench.specs.model import KubernetesConfig, load_site

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = REPO_ROOT / "scripts"
AWS_DEPLOY = REPO_ROOT / "deploy" / "aws"
K8S_LIB = SCRIPTS / "_k8s.sh"
SMOKE = SCRIPTS / "smoke.sh"
GEN_CORPUS = SCRIPTS / "gen-corpus.sh"
PUSH_IMAGES = SCRIPTS / "push-images.sh"
STAGE = SCRIPTS / "stage.sh"
LAUNCH = SCRIPTS / "launch.sh"
GATE = SCRIPTS / "gate.sh"
TEARDOWN = SCRIPTS / "teardown.sh"
FINISH = SCRIPTS / "finish.sh"
PURGE = SCRIPTS / "purge.sh"
MEASURE_PRODUCER = SCRIPTS / "measure-producer.sh"
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


def _engine_compose_files() -> list[Path]:
    """Each engine's Compose shape: how a run of it is started on one machine.

    The other half of the seam `specs/kubernetes.py` is. Sourced by `smoke.sh`
    for the run's engine alone, which is what keeps the engines' service names,
    env files and readiness probes out of `scripts/`.
    """
    return sorted((REPO_ROOT / "engines").glob("*/compose.sh"))


def _shell_files() -> list[Path]:
    return sorted(SCRIPTS.glob("*.sh")) + sorted(AWS_DEPLOY.glob("*.sh")) + _engine_compose_files()


def _sourced_shell_files() -> list[Path]:
    """The files that are sourced rather than run: the two libraries and the engines'."""
    return sorted([path for path in _shell_files() if path.name.startswith("_")] + _engine_compose_files())


def _shell_entrypoints() -> list[Path]:
    """The scripts meant to be run, which is everything that is not sourced."""
    sourced = set(_sourced_shell_files())
    return [path for path in _shell_files() if path not in sourced]


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


def _shell_function(path: Path, name: str) -> str:
    """One shell function out of a script, to be run on its own.

    Lifted rather than copied for the same reason as the block above: a copy
    would keep passing after the script's own version of it broke.

    The closing brace is the first line that is one, which these scripts'
    indentation makes the function's own — every brace inside a body is
    indented. `bash -n` over the lifted text is what anchors that: a body that
    broke the convention would otherwise be silently cut in half and the
    remainder run as if it were the whole function.
    """
    lines = path.read_text().splitlines()
    starts = [index for index, line in enumerate(lines) if line == f"{name}() {{"]
    assert len(starts) == 1, f"{path.name} does not hold exactly one {name}"
    ends = [index for index, line in enumerate(lines) if line == "}" and index > starts[0]]
    assert ends, f"{path.name}'s {name} does not close"
    lifted = "\n".join(lines[starts[0] : ends[0] + 1])
    parsed = subprocess.run(["bash", "-n"], input=lifted, capture_output=True, text=True)
    assert parsed.returncode == 0, f"{path.name}'s {name} did not lift out whole: {parsed.stderr}"
    return lifted


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
    sourced = _sourced_shell_files()
    assert [path.relative_to(REPO_ROOT).as_posix() for path in sourced] == [
        "engines/flink/compose.sh",
        "engines/spark/compose.sh",
        "scripts/_k8s.sh",
        "scripts/_lib.sh",
    ], "the two shared libraries and one Compose shape per engine"
    for script in entrypoints:
        assert os.access(script, os.X_OK), f"{script} is not executable"
    for script in sourced:
        # A sourced file that is executable invites being run, and neither of
        # these does anything on its own but set variables the caller needs.
        assert not os.access(script, os.X_OK), f"{script} is sourced, so it should not be executable"


@dataclass(frozen=True)
class WaitedJob:
    """One run of `k8s_wait_job`, and every `kubectl` call it made."""

    result: subprocess.CompletedProcess[str]
    calls: str


# The Job's one pod, and what the scheduler said about it. A pod that never
# scheduled has no log, so this line is the whole of what a timed-out wait has
# to explain itself with.
POD = "a-job-2xk4t"
SCHEDULER_REFUSAL = "Warning FailedScheduling 0/3 nodes are available: Insufficient cpu"


def _waited_job(answers: list[str], timeout_s: int = 1) -> WaitedJob:
    """`_k8s.sh`'s own wait, against a `kubectl` answering one reading at a time.

    Lifted and run rather than read, because every branch in it is a different
    end for a driver: a Job that completed, one that failed with its log to
    show, and one that is still going when the caller's budget runs out.
    """
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "kubectl-calls.log"
        calls.touch()
        conditions = Path(directory) / "conditions"
        conditions.write_text("".join(f"{answer}\n" for answer in answers))
        harness = f"""
            set -euo pipefail
            log() {{ printf 'log %s\\n' "$*"; }}
            die() {{ printf 'die %s\\n' "$*"; exit 3; }}
            KUBE_CONTEXT=a-cluster
            SITE_NAMESPACE=ingest-bench
            K8S_JOB_POLL_S=1
            kubectl() {{
                printf 'kubectl %s\\n' "$*" >>'{calls}'
                case "$*" in
                # Before the conditions arm below, which its own read would
                # otherwise match: a pod name is read with a jsonpath too.
                *"get pods"*) printf '{POD}\\n' ;;
                *"get events"*) printf '{SCHEDULER_REFUSAL}\\n' ;;
                *jsonpath*)
                    head -n 1 '{conditions}'
                    tail -n +2 '{conditions}' >'{conditions}.rest'
                    mv '{conditions}.rest' '{conditions}'
                    ;;
                *logs*) printf 'the job said this\\n' ;;
                esac
            }}
{_shell_function(K8S_LIB, "k8s_job_pod_events")}
{_shell_function(K8S_LIB, "k8s_job_tail")}
{_shell_function(K8S_LIB, "k8s_wait_job")}
            k8s_wait_job a-job {timeout_s}
        """
        result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
        return WaitedJob(result=result, calls=calls.read_text())


@needs_bash
@pytest.mark.parametrize(
    ("answers", "status", "said", "tailed"),
    [
        # A Job announces nothing until it has an end to announce, so the empty
        # reading is the normal first one.
        (["", "Complete"], 0, "log job/a-job completed", False),
        # Both conditions are read, and not `complete` alone: a failed Job
        # never gains that one, so a wait on it would spend the whole timeout —
        # hours, for a generation — to report a failure announced in seconds.
        (["Failed"], 3, "die job/a-job failed", True),
        (["", ""], 3, "die job/a-job did not complete within 1s", True),
    ],
)
def test_a_job_is_waited_for_until_it_reaches_one_of_its_two_ends(
    answers: list[str], status: int, said: str, tailed: bool
) -> None:
    waited = _waited_job(answers)
    assert waited.result.returncode == status, waited.result.stdout + waited.result.stderr
    assert said in waited.result.stdout, waited.result.stdout
    # Its own log, and only where the end was not the good one.
    assert ("logs job/a-job --tail=40" in waited.calls) is tailed, waited.calls
    assert ("the job said this" in waited.result.stderr) is tailed, waited.result.stderr
    # And its pods' events beside it: a pod that never scheduled has an empty
    # log, and the scheduler's refusal is only ever in the events.
    assert (f"get events --field-selector involvedObject.name={POD}" in waited.calls) is tailed, waited.calls
    assert (SCHEDULER_REFUSAL in waited.result.stderr) is tailed, waited.result.stderr
    # Only the conditions the API says are true, so a `Failed: False` cannot be
    # read as a failure.
    assert '{range .status.conditions[?(@.status=="True")]}' in waited.calls, waited.calls


# What an m6i.xlarge reports allocatable — four vCPU less the kubelet's own
# reservation — which is the node the shipped eksctl example builds.
NODE_ALLOCATABLE = "3920m"


def _node(name: str, taint: str | None = None) -> dict[str, object]:
    spec: dict[str, object] = {} if taint is None else {"taints": [{"key": "a-pool", "effect": taint}]}
    return {"metadata": {"name": name}, "spec": spec, "status": {"allocatable": {"cpu": NODE_ALLOCATABLE}}}


def _pod(node: str | None, *requests: str | None, phase: str = "Running") -> dict[str, object]:
    """One pod of ``requests``, a container each, with None for a container that asks for nothing."""
    containers = [{} if cpu is None else {"resources": {"requests": {"cpu": cpu}}} for cpu in requests]
    spec: dict[str, object] = {"containers": containers}
    if node is not None:
        spec["nodeName"] = node
    return {"spec": spec, "status": {"phase": phase}}


def _nodes_with_free_cpu(
    tmp_path: Path,
    nodes: list[dict[str, object]],
    pods: list[dict[str, object]],
    millicores: int,
    tolerations: str,
) -> subprocess.CompletedProcess[str]:
    """`_k8s.sh`'s own count, lifted out and run against two files.

    It takes files rather than reading the cluster precisely so this is
    runnable, and it is worth running rather than reading because the
    arithmetic is a jq program: a CPU quantity is `"2"`, `"500m"` or `"1.5"`
    depending on who wrote the manifest, and the answer decides whether an
    operator is warned that their run will not schedule.
    """
    (tmp_path / "nodes.json").write_text(json.dumps({"items": nodes}))
    (tmp_path / "pods.json").write_text(json.dumps({"items": pods}))
    harness = f"""
        set -euo pipefail
        log() {{ printf 'log %s\\n' "$*"; }}
        die() {{ printf 'die %s\\n' "$*"; exit 3; }}
        TOLERATIONS='{tolerations}'
{_shell_function(K8S_LIB, "k8s_nodes_with_free_cpu")}
        k8s_nodes_with_free_cpu {millicores} '{tmp_path / "nodes.json"}' '{tmp_path / "pods.json"}'
    """
    return subprocess.run(["bash", "-c", harness], capture_output=True, text=True)


@needs_shell_tools
@pytest.mark.parametrize(
    ("nodes", "pods", "millicores", "tolerations", "counted"),
    [
        # Requested and not used: a node whose cores are idle but asked for is
        # a node the scheduler fits nothing more onto. One 2-CPU pod and the
        # daemonsets leave an m6i.xlarge short of a second.
        (
            [_node("node-a"), _node("node-b")],
            [_pod("node-a", "2"), _pod("node-a", "250m", "100m"), _pod("node-b", "350m")],
            2000,
            "[]",
            1,
        ),
        # A pod that has reached an end has released its request, and one no
        # node is carrying yet was never holding a node's.
        (
            [_node("node-a"), _node("node-b")],
            [_pod("node-a", "2", phase="Succeeded"), _pod("node-b", "2", phase="Failed"), _pod(None, "2")],
            2000,
            "[]",
            2,
        ),
        # Both other spellings of a quantity, on the one node: 1500 + 500 is
        # 2000, which leaves it short. A `1.5` read as 1.5 millicores would
        # leave it the roomiest node on the cluster.
        ([_node("node-a"), _node("node-b")], [_pod("node-a", "1.5", "500m"), _pod("node-b", "1")], 2000, "[]", 1),
        # A container stating no request holds nothing.
        ([_node("node-a")], [_pod("node-a", None)], 2000, "[]", 1),
        # A NoSchedule taint keeps a pod off the node unless the site declares
        # a toleration; PreferNoSchedule keeps it off nothing.
        ([_node("node-a", "NoSchedule"), _node("node-b", "PreferNoSchedule")], [], 2000, "[]", 1),
        ([_node("node-a", "NoSchedule"), _node("node-b", "PreferNoSchedule")], [], 2000, '[{"operator":"Exists"}]', 2),
    ],
    ids=["requests-not-usage", "ended-and-unbound-pods", "quantity-spellings", "no-request", "taints", "tolerated"],
)
def test_a_nodes_free_cpu_is_its_allocatable_less_what_its_pods_request(
    tmp_path: Path,
    nodes: list[dict[str, object]],
    pods: list[dict[str, object]],
    millicores: int,
    tolerations: str,
    counted: int,
) -> None:
    out = _nodes_with_free_cpu(tmp_path, nodes, pods, millicores, tolerations)
    assert out.returncode == 0, out.stdout + out.stderr
    assert out.stdout.strip() == str(counted), out.stdout + out.stderr


@needs_shell_tools
def test_a_cluster_that_could_not_be_read_is_not_a_cluster_with_no_room(tmp_path: Path) -> None:
    """An unreadable answer must not count as zero free nodes.

    The count is advisory, and the two reads behind it are a `kubectl` an
    operator's kubeconfig may not be allowed to make. A document with no
    `items` answered as `0` would warn every launch from a namespace-scoped
    context that the run will not schedule.
    """
    (tmp_path / "nodes.json").write_text("")
    (tmp_path / "pods.json").write_text("")
    harness = f"""
        set -euo pipefail
        TOLERATIONS='[]'
{_shell_function(K8S_LIB, "k8s_nodes_with_free_cpu")}
        k8s_nodes_with_free_cpu 2000 '{tmp_path / "nodes.json"}' '{tmp_path / "pods.json"}'
    """
    out = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
    assert out.stdout.strip() == "", out.stdout


def test_both_workflows_install_the_same_checked_yq() -> None:
    """One pinned release, and the digest of the bytes behind it.

    A version tag names a release and not its contents, and this binary reads
    every site config a run is staged from — so the download is checked rather
    than trusted. The two workflows install it for the same tests, and a
    version bumped in one of them alone would leave them running different
    parsers.
    """
    installs = {
        path.name: (
            re.findall(r"yq/releases/download/(v[\d.]+)/yq_linux_amd64", path.read_text()),
            re.findall(r"^\s*([0-9a-f]{64}) \| sha256sum", path.read_text(), re.MULTILINE),
        )
        for path in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
    }
    installing = {name: pins for name, pins in installs.items() if pins[0]}
    assert set(installing) == {"ci.yml", "smoke.yml"}, installing
    for name, (versions, digests) in installing.items():
        assert len(versions) == 1 and len(digests) == 1, f"{name} pins {versions} and checks {digests}"
    assert len({pins for pins in map(str, installing.values())}) == 1, installing


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
def test_measure_producer_answers_before_it_starts_a_stack() -> None:
    """The one script whose work is a build, a 3 GB corpus and a full offer.

    Answering `--help` by starting that is the most expensive way in the
    repository to learn what a script does.
    """
    out = subprocess.run([str(MEASURE_PRODUCER), "--help"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "takes no arguments" in out.stdout

    refused = subprocess.run([str(MEASURE_PRODUCER), "--warmup"], capture_output=True, text=True)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --warmup" in refused.stderr


@needs_bash
@pytest.mark.parametrize("script", [GEN_CORPUS, PUSH_IMAGES, STAGE, LAUNCH, GATE, TEARDOWN, FINISH, PURGE])
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
        # MSK also publishes a minor line's latest patch as a trailing `x`;
        # it must win over a numeric minor that is actually older.
        ("3.6.0\t3.7.x", "3.7.x"),
        # An `x` patch sorts after every numeric patch of the same minor.
        ("3.7.x\t3.7.5", "3.7.x"),
        # A numeric minor still beats an `x` patch of an older minor.
        ("3.9.x\t3.10.0", "3.10.0"),
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
    `grep` matches nothing, taking the refusal below it with it.
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


# What MSK reports for a cluster, as `--output text` tab-separates the three
# fields `grow_broker_volume` reads in one call.
_CLUSTER_VERSION = "K3AEGXETSR30VB"


@dataclass(frozen=True)
class Growth:
    """One run of `grow_broker_volume`, and every `aws` call it made."""

    result: subprocess.CompletedProcess[str]
    calls: str


def _volume_growth(*, described: str, refusal: str = "", asked: int = 1000) -> Growth:
    """`setup.sh`'s own growth lines, against fixed answers from MSK.

    The path sits behind a live account, so the function is lifted out and run
    on its own. Worth running rather than reading: every branch in it exists to
    keep a second `setup.sh` run from failing on the growth the first one asked
    for, which is what the file's own header promises.

    The stub records its calls to a file rather than to a stream: the update is
    made inside a `"$(... 2>&1)"` capture, so anything it wrote to either
    stream would end up in the variable the script reads its error out of.
    """
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "aws-calls.log"
        calls.touch()
        stub = f"""
            printf 'aws %s\n' "$*" >>'{calls}'
            case "$*" in
            *update-broker-storage*)
                if [[ -n '{refusal}' ]]; then
                    printf '%s\n' '{refusal}' >&2
                    return 254
                fi
                ;;
            *describe-cluster*) printf '%s\n' '{described}' ;;
            esac
        """
        harness = f"""
            set -euo pipefail
            log() {{ printf 'log %s\n' "$*"; }}
            die() {{ printf 'die %s\n' "$*"; exit 3; }}
            aws() {{{stub}}}
            MSK_NAME=a-cluster
            MSK_ARN=arn:aws:kafka:eu-west-1:123456789012:cluster/a-cluster/aaaa-1
            MSK_VOLUME_GIB={asked}
{_shell_function(AWS_SETUP, "grow_broker_volume")}
            grow_broker_volume
        """
        result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
        return Growth(result=result, calls=calls.read_text())


@needs_bash
@pytest.mark.parametrize(
    ("state", "current", "grown"),
    [
        # Smaller and ready: an hour's offer needs the room, and a volume that
        # fills stops the offer rather than the engine.
        ("ACTIVE", 100, True),
        # Equal, and larger: a broker volume cannot shrink, so the only two
        # answers are grow it and leave it alone.
        ("ACTIVE", 1000, False),
        ("ACTIVE", 2000, False),
        # Smaller and not ready. A cluster applying an earlier update keeps
        # reporting the old size, so the size alone would ask for the same
        # growth a second time and MSK would refuse it — exiting the script on
        # the re-run its own header calls idempotent.
        ("UPDATING", 100, False),
        ("MAINTENANCE", 100, False),
    ],
)
def test_an_existing_broker_volume_is_grown_once_and_never_shrunk(state: str, current: int, grown: bool) -> None:
    grown_run = _volume_growth(described=f"{state}\t{_CLUSTER_VERSION}\t{current}")
    assert grown_run.result.returncode == 0, grown_run.result.stdout + grown_run.result.stderr
    issued = "update-broker-storage" in grown_run.calls
    assert issued is grown, grown_run.calls + grown_run.result.stdout
    if grown:
        assert "VolumeSizeGB=1000" in grown_run.calls, grown_run.calls
        # The version MSK reported, not a guess: an update carrying the wrong
        # one is refused, and the refusal is minutes into a setup.
        assert f"--current-version {_CLUSTER_VERSION}" in grown_run.calls, grown_run.calls
    else:
        assert "log msk broker volumes are" in grown_run.result.stdout, grown_run.result.stdout


@needs_bash
@pytest.mark.parametrize(
    "refusal",
    [
        # The two answers that mean "not now": a cluster that left ACTIVE
        # between the read and the call, and the cooldown MSK holds between
        # storage updates.
        "An error occurred (BadRequestException): The cluster must be in ACTIVE state",
        "An error occurred (BadRequestException): A previous storage update was performed in the last 6 hours",
    ],
)
def test_a_growth_msk_will_not_take_yet_leaves_the_setup_converging(refusal: str) -> None:
    """A re-run has to finish, because everything after this needs the cluster.

    Neither refusal changes what the volume already is, and the growth an
    earlier run asked for is either applying or already applied — so failing
    here would abandon a setup over a call with nothing left to do.
    """
    refused = _volume_growth(described=f"ACTIVE\t{_CLUSTER_VERSION}\t100", refusal=refusal).result
    assert refused.returncode == 0, refused.stdout + refused.stderr
    assert "log a-cluster will not take the growth to 1000 GiB yet" in refused.stdout, refused.stdout


@needs_bash
def test_a_growth_msk_refuses_for_any_other_reason_stops_the_setup() -> None:
    """A refusal nobody recognises is not one to shrug at.

    A volume above MSK's ceiling, a malformed request, a denied action: each is
    a setup that did not do what it said, and reporting it as converged would
    leave the growth a later run depends on silently undone.
    """
    refused = _volume_growth(
        described=f"ACTIVE\t{_CLUSTER_VERSION}\t100",
        refusal="An error occurred (AccessDeniedException): not authorized to perform kafka:UpdateBrokerStorage",
    ).result
    assert refused.returncode == 3, refused.stdout + refused.stderr
    assert "die could not grow a-cluster's broker volumes to 1000 GiB" in refused.stdout, refused.stdout
    assert "AccessDeniedException" in refused.stdout, refused.stdout


@needs_bash
def test_a_broker_volume_size_msk_would_not_report_is_refused() -> None:
    """No answer is not "it is big enough": an unread size cannot be compared.

    `--output text` prints `None` for a field the API left out, which an
    arithmetic comparison would read as zero and then try to grow a cluster
    whose shape nobody knows.
    """
    unread = _volume_growth(described=f"ACTIVE\t{_CLUSTER_VERSION}\tNone").result
    assert unread.returncode == 3, unread.stdout + unread.stderr
    assert "die a-cluster reports no broker volume size" in unread.stdout, unread.stdout


@dataclass(frozen=True)
class BucketStep:
    """One run of a bucket step, and every `aws` call and question it made."""

    result: subprocess.CompletedProcess[str]
    calls: str
    asked: str


def _bucket_step(script: Path, function: str, *, tags: str | None, answered: str = "yes") -> BucketStep:
    """One script's own bucket lines, against fixed answers from S3.

    Lifted and run rather than read, like the broker-volume growth above: the
    path sits behind a live account, and the branches in it are the ones that
    decide whether a bucket the operator keeps something else in is reconfigured
    or emptied. ``tags`` is None for a bucket that does not exist, empty for one
    with no tag of ours, and the tag's value otherwise.
    """
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "aws-calls.log"
        calls.touch()
        asked = Path(directory) / "asked.log"
        asked.touch()
        present = "1" if tags is not None else ""
        stub = f"""
            printf 'aws %s\n' "$*" >>'{calls}'
            case "$*" in
            *head-bucket*) [[ -n '{present}' ]] || return 1 ;;
            *get-bucket-tagging*)
                [[ -n '{tags or ""}' ]] || return 254
                printf '%s\n' '{tags or ""}'
                ;;
            *get-bucket-versioning*) printf 'None\n' ;;
            esac
        """
        harness = f"""
            set -euo pipefail
            log() {{ printf 'log %s\n' "$*"; }}
            die() {{ printf 'die %s\n' "$*"; exit 3; }}
            confirm() {{ printf '%s\n' "$*" >>'{asked}'; [[ '{answered}' == yes ]] || die "answered nothing"; }}
            aws() {{{stub}}}
            AWS_REGION=eu-west-1
            BUCKET=a-bucket
            TAG_KEY=lakehouse-ingest-bench
            ASSUME_YES=no
{_shell_function(script, "bucket_is_ours")}
{_shell_function(script, function)}
            {function}
        """
        result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
        return BucketStep(result=result, calls=calls.read_text(), asked=asked.read_text())


@needs_bash
def test_setup_refuses_to_reconfigure_a_bucket_it_did_not_create() -> None:
    """Neither of the two calls it would make is additive.

    PutBucketTagging replaces the whole tag set and PutBucketVersioning
    suspends versioning, so adopting a bucket would silently reconfigure one
    the operator keeps something else in — and `BUCKET` defaults to a name
    derived from the account id, which is exactly the name someone may have
    used already.
    """
    refused = _bucket_step(AWS_SETUP, "create_bucket", tags="")
    assert refused.result.returncode == 3, refused.result.stdout
    assert "carries no lakehouse-ingest-bench tag" in refused.result.stdout, refused.result.stdout
    for mutation in ("put-bucket-tagging", "put-bucket-versioning", "put-public-access-block"):
        assert mutation not in refused.calls, refused.calls


@needs_bash
def test_setup_creates_a_bucket_and_re_runs_over_its_own() -> None:
    """A bucket it created before is adopted, and an absent one is created."""
    created = _bucket_step(AWS_SETUP, "create_bucket", tags=None)
    assert created.result.returncode == 0, created.result.stdout + created.result.stderr
    assert "create-bucket --bucket a-bucket" in created.calls, created.calls
    assert "put-bucket-tagging" in created.calls

    adopted = _bucket_step(AWS_SETUP, "create_bucket", tags="true")
    assert adopted.result.returncode == 0, adopted.result.stdout + adopted.result.stderr
    assert "create-bucket" not in adopted.calls, adopted.calls
    assert "put-bucket-tagging" in adopted.calls


@needs_bash
def test_teardown_refuses_to_empty_a_bucket_it_did_not_create() -> None:
    """The tag check the security-group deletion has, on the more destructive step.

    `BUCKET` is a name derived from the account id, and what `--all` does to it
    is remove every corpus, every run's artifacts and the warehouse.
    """
    refused = _bucket_step(AWS_TEARDOWN, "remove_bucket", tags="")
    assert refused.result.returncode == 3, refused.result.stdout
    assert "carries no lakehouse-ingest-bench tag" in refused.result.stdout, refused.result.stdout
    assert "s3 rm" not in refused.calls and "delete-bucket" not in refused.calls, refused.calls
    assert refused.asked == "", "a bucket it will not empty is not one to ask about"


@needs_bash
def test_teardown_names_what_the_bucket_holds_and_asks_before_emptying_it() -> None:
    """Describing a deletion is not the same as asking for it.

    `purge.sh` argues the policy for the same class of data and implements the
    prompt; this is the same data, one level up.
    """
    asked = _bucket_step(AWS_TEARDOWN, "remove_bucket", tags="true", answered="no")
    assert asked.result.returncode == 3, asked.result.stdout
    assert "remove all of the above?" in asked.asked, asked.asked
    assert "every corpus generated into it" in asked.result.stdout, asked.result.stdout
    assert "s3 rm" not in asked.calls and "delete-bucket" not in asked.calls, asked.calls

    answered = _bucket_step(AWS_TEARDOWN, "remove_bucket", tags="true", answered="yes")
    assert answered.result.returncode == 0, answered.result.stdout + answered.result.stderr
    assert "s3 rm s3://a-bucket --recursive" in answered.calls, answered.calls
    assert "delete-bucket --bucket a-bucket" in answered.calls, answered.calls


@needs_bash
def test_teardown_leaves_a_bucket_that_is_already_gone_alone() -> None:
    """A re-run after a partial teardown finishes the job rather than failing."""
    gone = _bucket_step(AWS_TEARDOWN, "remove_bucket", tags=None)
    assert gone.result.returncode == 0, gone.result.stdout + gone.result.stderr
    assert "already gone" in gone.result.stdout, gone.result.stdout
    assert "s3 rm" not in gone.calls and gone.asked == ""


def test_the_spark_operator_is_installed_once_from_the_kubeflow_chart_at_the_pinned_version() -> None:
    """The chart, the pin, and the three values the install cannot be right without.

    `spark.jobNamespaces` tells the controller which namespaces to reconcile
    SparkApplications in; without the harness namespace in it, a staged run's
    object is created and never looked at, and staging waits out its whole
    timeout on a state nobody was going to report. The webhook is what grafts
    `spec.volumes` and the two `volumeMounts` onto the pods, so an install
    without it starts a driver that dies opening the run's job document. And
    the chart's own spark identity is off because a run's driver runs as the
    account Pod Identity is bound to.

    Counted rather than matched as substrings: a second copy of the install
    satisfies every `in` assertion while printing its own log lines into the
    values an operator copies, and two copies are two things to keep in step.
    """
    setup = AWS_SETUP.read_text()
    assert setup.count("get crd sparkapplications.sparkoperator.k8s.io") == 1
    assert setup.count(f'helm --kube-context "$KUBE_CONTEXT" install "{"$SPARK_OPERATOR_RELEASE"}"') == 1
    assert setup.count("SPARK_OPERATOR_REPO=https://kubeflow.github.io/spark-operator") == 1
    assert 'SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-' in setup, "the pin should be overridable"
    for value in (
        '--version "$SPARK_OPERATOR_VERSION"',
        '--set "spark.jobNamespaces={$NAMESPACE}"',
        "--set spark.serviceAccount.create=false",
        "--set spark.rbac.create=false",
        "--set webhook.enable=true",
    ):
        assert setup.count(value) == 1, value


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


def test_every_engine_s_image_is_pushed_and_has_a_repository_to_be_pushed_to() -> None:
    """One image name, stated in three places: the renderer, the push and the account.

    A renderer naming a repository nothing pushes leaves the operator waiting on
    an `ImagePullBackOff`, and a push to a repository `setup.sh` never created
    fails on a registry 404 — both minutes into a campaign, and both from a name
    that was only ever written down twice.
    """
    prefix = re.search(r"^IMAGE_REPOSITORY_PREFIX=(\S+)$", (SCRIPTS / "_k8s.sh").read_text(), re.M)
    assert prefix is not None, "_k8s.sh no longer states the registry path both images are pushed under"
    listed = re.search(r'^ECR_REPOSITORIES="([^"]+)"$', AWS_SETUP.read_text(), re.M)
    assert listed is not None, "setup.sh no longer states one list of ECR repositories"
    created = set(listed.group(1).split())

    pushed = set(
        re.findall(r'^\w+_REF="\$REGISTRY/\$IMAGE_REPOSITORY_PREFIX/(\S+):\$TAG"$', PUSH_IMAGES.read_text(), re.M)
    )
    assert pushed, "push-images.sh no longer builds any reference from the registry and the tag"
    assert {f"{prefix.group(1)}/{name}" for name in pushed} == created

    # The harness image is nobody's engine, so it is the one pushed repository
    # with no renderer behind it.
    rendered = {str(engines.knobs_for(engine).IMAGE_REPOSITORY) for engine in engines.MANAGED}
    assert rendered == created - {f"{prefix.group(1)}/harness"}


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


def test_the_namespace_manifest_renders_every_identity_and_each_engine_s_rbac() -> None:
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
    assert accounts == {
        site["harness_service_account"],
        site["flink_service_account"],
        site["spark_service_account"],
    }

    # Each engine raises its own fleet: a JobManager creates its TaskManager
    # pods and the ConfigMaps that configure them, and a Spark driver creates
    # its executors, their configuration and the Service they find it by. So
    # these are the resources a run cannot start without. The verbs are
    # enumerated rather than `*`, so a Role that widens to a wildcard is a
    # failure and not a silent grant of everything the API group ever gains.
    expected: dict[str, tuple[set[str], set[tuple[str, str]]]] = {
        str(site["flink_service_account"]): (
            {"get", "list", "watch", "create", "update", "patch", "delete"},
            {("", "pods"), ("", "configmaps"), ("apps", "deployments"), ("apps", "deployments/finalizers")},
        ),
        str(site["spark_service_account"]): (
            {"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"},
            {("", "pods"), ("", "configmaps"), ("", "persistentvolumeclaims"), ("", "services")},
        ),
    }
    roles = {str(_mapping(role["metadata"])["name"]): role for role in by_kind["Role"]}
    assert set(roles) == set(expected)
    for name, (verbs, resources) in expected.items():
        granted: set[tuple[str, str]] = set()
        for entry in _sequence(roles[name]["rules"]):
            rule = _mapping(entry)
            assert {str(verb) for verb in _sequence(rule["verbs"])} == verbs, name
            for group in _sequence(rule["apiGroups"]):
                for resource in _sequence(rule["resources"]):
                    granted.add((str(group), str(resource)))
        assert granted == resources, name

    # Each binding names its own Role and the account of the same name: a
    # binding pointing at the other engine's would grant a driver the rules a
    # JobManager needs and none of its own.
    bindings = {str(_mapping(binding["metadata"])["name"]): binding for binding in by_kind["RoleBinding"]}
    assert set(bindings) == set(expected)
    for name, binding in bindings.items():
        assert _mapping(binding["roleRef"])["name"] == name
        subject = _mapping(_sequence(binding["subjects"])[0])
        assert subject["name"] == name
        assert subject["namespace"] == "a-namespace"


def test_the_schema_registry_manifest_renders_a_deployment_and_a_service() -> None:
    """The two objects a `confluent` run reaches by service name.

    The Service's name is half of the URL `setup.sh` prints and an operator
    pastes into `site.yaml`, and its selector is what makes that name resolve
    to the registry pod rather than to nothing.
    """
    template = REPO_ROOT / "deploy" / "k8s" / "schema-registry.yaml.tmpl"
    rendered = render_template(
        template,
        {"NAMESPACE": "a-namespace", "NODE_SELECTOR": '{"kubernetes.io/arch": "amd64"}', "TOLERATIONS": "[]"},
    )
    objects = [_mapping(document) for document in yaml.safe_load_all(rendered) if document is not None]
    by_kind = {str(document["kind"]): document for document in objects}
    assert sorted(by_kind) == ["Deployment", "Service"]
    for kind, document in by_kind.items():
        metadata = _mapping(document["metadata"])
        assert metadata["name"] == "schema-registry", kind
        assert metadata["namespace"] == "a-namespace", kind

    deployment = _mapping(by_kind["Deployment"]["spec"])
    pod = _mapping(_mapping(deployment["template"])["spec"])
    assert pod["nodeSelector"] == {"kubernetes.io/arch": "amd64"} and pod["tolerations"] == []
    container = _mapping(_sequence(pod["containers"])[0])
    # Both probes, because the registry answers on its port before it will
    # serve a registration: readiness is what holds the endpoint back until a
    # `stage` against it can succeed.
    assert _mapping(_mapping(container["readinessProbe"])["httpGet"])["path"] == "/health/ready"
    assert _mapping(_mapping(container["livenessProbe"])["httpGet"])["path"] == "/health/live"

    service = _mapping(by_kind["Service"]["spec"])
    selector = _mapping(service["selector"])
    labels = _mapping(_mapping(_mapping(deployment["template"])["metadata"])["labels"])
    assert selector.items() <= labels.items(), "the Service selects labels the pod does not carry"
    assert _mapping(_sequence(service["ports"])[0])["port"] == 8080


def test_the_setup_script_substitutes_every_marker_the_registry_template_carries() -> None:
    """`setup.sh` renders that template with `sed`, so its list and the markers are one.

    A marker the template gains and the script does not substitute reaches the
    API server verbatim, which is refused there rather than here.
    """
    template = REPO_ROOT / "deploy" / "k8s" / "schema-registry.yaml.tmpl"
    setup = AWS_SETUP.read_text()
    markers = {match[2:-2] for match in MARKER_RE.findall(template.read_text())}
    assert markers == {"NAMESPACE", "NODE_SELECTOR", "TOLERATIONS"}
    for marker in markers:
        assert f"s|__{marker}__|$" in setup, f"setup.sh never substitutes __{marker}__"
    assert "WITH_SCHEMA_REGISTRY" in setup
    assert "schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7" in setup, "setup.sh never prints the registry URL"


def test_the_stack_and_the_cluster_run_the_same_registry_image() -> None:
    """One image in both places, so a local `confluent` run proves the cluster's.

    Two pins drift, and the one nobody rereads is the one a cluster runs.
    """
    template = (REPO_ROOT / "deploy" / "k8s" / "schema-registry.yaml.tmpl").read_text()
    compose = yaml.safe_load((REPO_ROOT / "deploy" / "compose" / "local" / "docker-compose.yml").read_text())
    image = str(_mapping(_mapping(_mapping(compose)["services"])["schema-registry"])["image"])
    assert f"image: {image}" in template


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
        spark_service_account="ingest-bench-spark",
        service_account_annotations={},
        registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        aws_region="eu-west-1",
        secret_name=None,
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


def _site_reader(site_file: Path, call: str) -> subprocess.CompletedProcess[str]:
    """One of `_k8s.sh`'s site readers, run against ``site_file``, its stdout captured."""
    program = "\n".join(
        [
            "set -euo pipefail",
            "PREREQ_DOC=deploy/aws/README.md",
            "source scripts/_lib.sh",
            f"SITE_FILE={site_file}",
            "source scripts/_k8s.sh",
            call,
        ]
    )
    return subprocess.run(["bash", "-c", program], cwd=REPO_ROOT, capture_output=True, text=True)


@needs_shell_tools
def test_a_sites_reference_reaches_a_pods_python_as_the_site_wrote_it(tmp_path: Path) -> None:
    """A `${env:NAME}` has to survive the shell that splits a Job's command line.

    The image's entrypoint is `/bin/sh -c`, so the whole command reaches a pod
    as one string that shell expands. Unquoted, dash refuses the form outright
    and a POSIX-mode bash expands it to nothing — either way the process that
    was meant to resolve it never sees it. Single-quoted, it arrives as the
    characters the site wrote, which is what `resolve_env_placeholders` reads.
    """
    reference = "${env:IB_KAFKA_PASSWORD}"
    site_file = tmp_path / "site.yaml"
    site_file.write_text(
        _filled_site().replace(
            "    aws.region:",
            f"    sasl.password: '{reference}'\n    sasl.username: 'a user with spaces'\n    aws.region:",
        )
    )
    # A stand-in for the harness command, printing one argument per line, so
    # what is asserted is what the Python process is handed.
    stubs = _stub_bin(tmp_path / "bin", {"produce": 'printf "%s\\n" "$@"'})
    # Built the way `launch.sh` builds it, then handed to `sh -c` as one
    # string — which is how the image's entrypoint runs it.
    built = _site_reader(
        site_file,
        f'PATH="{stubs}:$PATH"\nCOMMAND="produce$(site_flags \'.kafka.security\' --kafka-prop)"\nsh -c "$COMMAND"',
    )
    assert built.returncode == 0, built.stderr
    printed = built.stdout.splitlines()
    assert f"sasl.password={reference}" in printed, printed
    assert "sasl.username=a user with spaces" in printed, printed


@needs_shell_tools
@pytest.mark.parametrize("quote", ["'", '"'])
def test_a_property_a_quote_cannot_carry_is_refused_by_name(tmp_path: Path, quote: str) -> None:
    """Neither quote survives the round trip, and each fails somewhere else.

    A single quote ends the quoting that makes the value opaque to the pod's
    shell; a double quote survives that shell and then closes the YAML scalar
    the whole command line is rendered into, so `kubectl apply` reports a parse
    error rather than the value that caused it.
    """
    site_file = tmp_path / "site.yaml"
    property_line = f"    sasl.password: {json.dumps('a' + quote + 'b')}\n    aws.region:"
    site_file.write_text(_filled_site().replace("    aws.region:", property_line))
    refused = _site_reader(site_file, "site_flags '.kafka.security' --kafka-prop")
    assert refused.returncode != 0
    assert "holding a quote" in refused.stderr, refused.stderr


@needs_shell_tools
@pytest.mark.parametrize("secret", ["ingest-bench-env", None])
def test_the_env_a_pod_reads_a_secret_from_is_the_one_the_site_names(tmp_path: Path, secret: str | None) -> None:
    """One Secret for the site, or an empty list where the site names none.

    A key per property would be a statement, in this harness, of which of an
    operator's properties hold credentials. A Secret's keys are already a set
    of variable names, which is exactly what a `${env:NAME}` names.
    """
    site_file = tmp_path / "site.yaml"
    site = _filled_site()
    if secret is not None:
        site = site.replace("  aws_region:", f"  secret_name: {secret}\n  aws_region:")
    site_file.write_text(site)
    out = _site_reader(site_file, "site_env_from_json")
    assert out.returncode == 0, out.stderr
    expected = [] if secret is None else [{"secretRef": {"name": secret}}]
    assert json.loads(out.stdout) == expected


@needs_shell_tools
@pytest.mark.parametrize("path", [".corpus_root", ".runs_root", ".warehouse"])
def test_a_root_these_drivers_cannot_reach_is_refused_by_name(tmp_path: Path, path: str) -> None:
    """The corpus and the renderers serve `gs://`; the drivers do not.

    Every driver fetches a run directory, an artifact or a listing by shelling
    out to the `aws` CLI, so a GCS site gets a working corpus generator and a
    driver layer that cannot read what it wrote. Refused where the root is read,
    rather than surfacing as an `aws s3` error about a URI it could not parse.
    """
    site_file = tmp_path / "site.yaml"
    key = path.removeprefix(".")
    site_file.write_text(
        "\n".join(
            line if not line.startswith(f"{key}:") else f"{key}: gs://a-bucket/{key}"
            for line in _filled_site().splitlines()
        )
    )
    refused = _site_reader(site_file, f"site_root '{path}'")
    assert refused.returncode != 0
    assert "AWS-only today" in refused.stderr, refused.stderr
    assert key in refused.stderr, refused.stderr
    # And an S3 root is answered with itself.
    (tmp_path / "aws.yaml").write_text(_filled_site())
    answered = _site_reader(tmp_path / "aws.yaml", f"site_root '{path}'")
    assert answered.returncode == 0, answered.stderr
    assert answered.stdout.startswith("s3://a-bucket/"), answered.stdout


def test_the_shell_calls_the_harness_with_arguments_it_takes() -> None:
    """An inline `python -c` is a call site neither mypy nor a test would see.

    `measure-producer.sh` reaches into `kafka_admin` directly, so a change to
    one of those signatures leaves that script passing an arity nothing checks,
    and nothing short of running it can say so. Every call whose arguments are
    literals is bound against the real signature here.
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
    for key in ("speed", "seconds", "behind_max_ms", "compression"):
        assert f"yq '.producer.{key}'" in text, f"smoke.sh never reads producer.{key}"
    for flag in (
        "--speed $SPEED",
        "--seconds $REPLAY_SECONDS",
        "--behind-max-ms $BEHIND_MAX_MS",
        "--compression $COMPRESSION",
    ):
        assert flag in text, f"smoke.sh reads a producer key but never passes {flag.split()[0]}"
    assert "--speed 1" not in text, "smoke.sh still hardcodes a replay speed"


@pytest.mark.parametrize("script", (SMOKE, LAUNCH), ids=lambda path: path.name)
def test_both_drivers_tell_the_scorer_who_runs_the_ddl(script: Path) -> None:
    """An absent table is a phase of the run or a fault, and only the spec says which."""
    text = script.read_text()
    assert "yq '.table.managed_by'" in text, f"{script.name} never reads table.managed_by"
    assert "--table-managed-by $MANAGED_BY" in text, f"{script.name} reads it but never passes it"


@pytest.mark.parametrize("script", (SMOKE, LAUNCH), ids=lambda path: path.name)
def test_both_drivers_frame_the_values_the_way_staging_did(script: Path) -> None:
    """The encoding and the schema id come off `facts.json`, not off the spec.

    Staging is what registered the schema, so the id it was given is a fact
    about the run and not something a driver could derive. A driver that read
    the encoding and dropped the id would offer records whose header names
    schema zero.
    """
    text = script.read_text()
    for fact in ("value_encoding", "schema_id"):
        assert f"jq -r '.{fact} // empty'" in text, f"{script.name} never reads {fact} out of facts.json"
    assert "--value-encoding $VALUE_ENCODING" in text and "--schema-id $SCHEMA_ID" in text


@pytest.mark.parametrize("script", (SMOKE, LAUNCH), ids=lambda path: path.name)
def test_both_drivers_offer_the_codec_the_spec_asks_for(script: Path) -> None:
    """The codec comes off the spec, and is left off where the spec says nothing.

    A driver that restated the producer's default would be a second copy of it
    to keep in step; a driver that hardcoded one would offer records the spec
    does not describe.
    """
    text = script.read_text()
    assert "yq '.producer.compression'" in text, f"{script.name} never reads producer.compression"
    assert "--compression $COMPRESSION" in text, f"{script.name} reads the codec but never passes --compression"
    for codec in ("zstd", "lz4", "snappy", "gzip"):
        assert f"--compression {codec}" not in text, f"{script.name} hardcodes a codec"


def _compose_hooks() -> list[str]:
    """The hooks `smoke.sh` calls, read off the loop that refuses a missing one.

    Read rather than restated, so a fifth hook is a failure in every engine
    that has not declared it instead of a call into nothing.
    """
    match = re.search(r"for hook in ((?:engine_compose_\w+ ?)+); do", SMOKE.read_text())
    assert match is not None, "smoke.sh no longer states which hooks an engine declares"
    return match.group(1).split()


def _shell_functions(path: Path) -> set[str]:
    return set(re.findall(r"^(\w+)\(\) \{$", path.read_text(), flags=re.MULTILINE))


def test_every_engine_declares_the_whole_compose_contract() -> None:
    """A hook an engine did not declare is a call into nothing, minutes in.

    `smoke.sh` refuses it at source time for the same reason `engine-k8s`
    refuses a descriptor field no driver reads: the failure has to land before
    a corpus is generated, not after.
    """
    hooks = _compose_hooks()
    assert len(hooks) == 4, hooks
    files = _engine_compose_files()
    assert {path.parent.name for path in files} == set(engines.MANAGED), "one Compose shape per managed engine"
    for path in files:
        assert set(hooks) <= _shell_functions(path), f"{path} declares {sorted(_shell_functions(path) & set(hooks))}"


def test_the_smoke_names_no_engine_service_of_its_own() -> None:
    """The contract `docs/adding-an-engine.md` states: no engine branches in `scripts/`.

    Checked against the service names the engines' own compose files declare,
    so it is the engines that say what must not appear here rather than a list
    in this test. A third engine adds a directory, not a line under `scripts/`.
    """
    services = {
        service
        for path in (REPO_ROOT / "engines").glob("*/compose.yaml")
        for service in _mapping(yaml.safe_load(path.read_text())["services"])
    }
    assert services, "no engine compose file declares a service"
    for script in sorted(SCRIPTS.glob("*.sh")):
        text = script.read_text()
        named = sorted(service for service in services if service in text)
        assert not named, f"{script.name} names the engine service(s) {named}"


@needs_shell_tools
def test_the_stack_activates_every_profile_its_engines_declare() -> None:
    """A service invisible to the one command that needs it is the failure this avoids.

    Compose interpolates the whole model before it filters by profile, so
    naming them all costs nothing — and the names come from the compose files
    that declare them, which is what keeps them out of `_lib.sh`.
    """
    declared = {
        profile
        for path in (REPO_ROOT / "engines").glob("*/compose.yaml")
        for service in _mapping(yaml.safe_load(path.read_text())["services"]).values()
        for profile in _sequence(_mapping(service).get("profiles", []))
    }
    assert declared, "no engine compose file declares a profile"
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "docker-calls.log"
        program = "\n".join(
            [
                "set -euo pipefail",
                "source scripts/_lib.sh",
                f"docker() {{ printf '%s\\n' \"$*\" >>'{calls}'; }}",
                "compose config --services",
            ]
        )
        out = subprocess.run(["bash", "-c", program], cwd=REPO_ROOT, capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
        arguments = calls.read_text().split()
    activated = {arguments[index + 1] for index, word in enumerate(arguments) if word == "--profile"}
    assert activated == declared | {"tools"}, activated


def test_the_smoke_stages_the_spec_it_was_given() -> None:
    """`--spec` names a file under `runs/`, which is what the container mounts."""
    text = SMOKE.read_text()
    assert "--spec)" in text and 'SPEC_FILE="$REPO_ROOT/runs/smoke-$ENGINE.yaml"' in text
    assert '--spec /runs/$(basename "$SPEC_FILE")' in text
    assert (REPO_ROOT / "runs" / "smoke-external-confluent.yaml").exists()


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
*"jsonpath={.metadata.name}"*) printf '%s' "${STUB_OBJECT_NAME:-}" ;;
*"get pod -l"*) printf '%s\\n' "${STUB_ENGINE_IMAGE:-}" ;;
*"get job/"*) printf 'Complete\\n' ;;
*"logs job/scorer-"*) printf 'POLL t=0.1 prefix=0/0\\n' ;;
# The merge Job's own log, for a driver that reads two Jobs' logs in one run
# and is answered by what each of them wrote rather than by one text twice.
*"logs job/corpus-merge"*) cat "${STUB_MERGE_LOG:-$STUB_JOB_LOG}" ;;
*"logs job/"*) cat "$STUB_JOB_LOG" ;;
# The cluster's shape, for the check a launch makes before it applies a pod
# no node may have room for. Unset answers nothing, which is the cluster a
# driver could not read — and a driver that read none must warn about none.
*"get nodes -o json"*) cat "${STUB_NODES:-/dev/null}" ;;
*"get pods --all-namespaces -o json"*) cat "${STUB_CLUSTER_PODS:-/dev/null}" ;;
*"get pods -l"*) printf '%s\\n' "${STUB_PODS:-}" ;;
# The run object's error field, told apart from its state by the name of the
# field: both engines put their operator's rejection under one named for it.
# Unset answers empty, which is what an operator that rejected nothing reports.
*"jsonpath={.status."*error*) printf '%s\\n' "${STUB_ENGINE_ERROR:-}" ;;
# The lifecycle an operator reports beside that error, for the engine whose
# lifecycle is a field of its own. For the engine whose application state *is*
# its lifecycle the path is the state's, and the arm below answers it.
*"jsonpath={.status.lifecycleState}"*) printf '%s\\n' "${STUB_ENGINE_LIFECYCLE:-}" ;;
# `-` and not `:-`, so a test can name the empty state an operator that
# created no job reports, as against not naming one at all.
*"jsonpath={.status."*) printf '%s\\n' "${STUB_ENGINE_STATE-RUNNING}" ;;
esac
"""

# `s3 sync` stands in for a prefix the pods published, copying one directory
# when a test names it; every other subcommand succeeds silently. Guarded on
# that directory rather than on the subcommand alone, because the drivers sync
# several prefixes and only the staged one has a stand-in.
#
# `s3 ls` answers with the listing a test names, and refuses the one path a
# test names as absent — which is how a bucket that is missing a shard is
# expressed, since `aws s3 ls` exits non-zero over a path that matches nothing.
#
# `s3 cp` writes its destination, from `STUB_S3_CP_DIR/<basename>` when a test
# put a file there and empty otherwise: the drivers read what they fetch, and a
# `cp` that recorded the call and wrote nothing would leave them reading a file
# that is not there. `STUB_S3_CP_ABSENT` is one object a test names as missing,
# which is how an artifact the pods never published is expressed.
AWS_STUB = """
printf '%s\\n' "$*" >>"$STUB_AWS_LOG"
if [[ ${1:-} == s3 && ${2:-} == sync && -d ${STUB_STAGE_DIR:-} ]]; then
	mkdir -p "$4"
	cp -R "$STUB_STAGE_DIR/." "$4"
fi
if [[ ${1:-} == s3 && ${2:-} == cp ]]; then
	[[ -z ${STUB_S3_CP_ABSENT:-} || ${3:-} != *"$STUB_S3_CP_ABSENT"* ]] || exit 1
	if [[ -n ${STUB_S3_CP_DIR:-} && -f "$STUB_S3_CP_DIR/${3##*/}" ]]; then
		cp "$STUB_S3_CP_DIR/${3##*/}" "$4"
	else
		: >"$4"
	fi
fi
if [[ ${1:-} == s3 && ${2:-} == ls ]]; then
	[[ -z ${STUB_S3_LS_ABSENT:-} || ${3:-} != *"$STUB_S3_LS_ABSENT"* ]] || exit 1
	printf '%s\\n' "${STUB_S3_LS:-}"
fi
"""

# The stub `kubectl port-forward` returns at once rather than holding a tunnel
# open, so what stands in for a jobmanager answering through one is `curl`.
CURL_STUB = """
printf '%s\\n' "$*" >>"$STUB_CURL_LOG"
"""

# The engine check, whose answer the driver branches on: 0 verified, 3 drift,
# anything else an endpoint it could not read. One body for every engine's, since
# what a driver does with it is the same.
VERIFY_STUB = """
printf '%s\\n' "$*" >>"$STUB_VERIFY_LOG"
# The pod list too, and not only its path: the driver writes it to a temporary
# file and removes it on the way out, so what the check was handed is only
# readable from inside the check.
while [[ $# -gt 0 ]]; do
	case "$1" in
	--pods)
		cat "$2" >>"$STUB_VERIFY_LOG"
		shift 2
		;;
	*) shift ;;
	esac
done
# `STUB_VERIFY_STATUSES` answers the nth call with its nth word and every call
# after that with its last, so "4 4 0" is a fleet that finishes being placed.
# The calls are counted in a file of their own because the log above holds a
# line per pod list as well as one per call.
if [[ -n ${STUB_VERIFY_STATUSES:-} ]]; then
	printf 'x' >>"$STUB_VERIFY_LOG.calls"
	read -r -a statuses <<<"$STUB_VERIFY_STATUSES"
	index=$(($(wc -c <"$STUB_VERIFY_LOG.calls") - 1))
	((index < ${#statuses[@]})) || index=$((${#statuses[@]} - 1))
	exit "${statuses[index]}"
fi
exit "${STUB_VERIFY_STATUS:-0}"
"""

DROP_TABLE_STUB = """
printf '%s\\n' "$*" >>"$STUB_DROP_TABLE_LOG"
"""

RESULTS_TABLE_STUB = """
printf '%s\\n' "$*" >>"$STUB_RESULTS_TABLE_LOG"
"""

# `collect` writes the document its caller reads back: without `--out` the run
# directory's own, and with one a published result under the engine's directory.
COLLECT_STUB = """
printf '%s\\n' "$*" >>"$STUB_COLLECT_LOG"
run_dir=""
out=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--run-dir)
		run_dir="$2"
		shift 2
		;;
	--out)
		out="$2"
		shift 2
		;;
	*) shift ;;
	esac
done
if [[ -z $out ]]; then
	if [[ -n ${STUB_COLLECT_NO_ENGINE:-} ]]; then
		printf '{"run": {}}\\n' >"$run_dir/run.json"
	else
		printf '{"run": {"engine": "flink"}}\\n' >"$run_dir/run.json"
	fi
else
	mkdir -p "${out%/}"
	printf '{}\\n' >"${out%/}/a-published-result.json"
fi
"""

# `file-sizes` writes the geometry document the verdict block reads its last
# line out of, and answers the exit code a test asks it for.
FILE_SIZES_STUB = """
printf '%s\\n' "$*" >>"$STUB_FILE_SIZES_LOG"
out=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--out)
		out="$2"
		shift 2
		;;
	*) shift ;;
	esac
done
status="${STUB_FILE_SIZES_STATUS:-0}"
if [[ -n $out && $status == 0 ]]; then
	mkdir -p "$out"
	printf '%s\\n' "$STUB_GEOMETRY" >"$out/geometry.json"
fi
exit "$status"
"""

RUN_ID = "smoke-flink-20260908T120000Z"
# What the same run's Kubernetes objects are named, since an RFC 1123 name is
# lowercase and the stamp in a run id is not.
RUN_OBJECT = RUN_ID.lower()
BOOTSTRAP = SITE_AWS_FILLINGS["YOUR_MSK_IAM_BOOTSTRAP"] + ":9098"

# The corpus root the filled example declares, spelled out rather than built
# from the filling above it: the bucket is a fixture name the tree is allowed
# to hold, and a URI assembled around an expression is one the guard on what
# this repository may name cannot read.
#
# Then a sharded generation under it. A corpus directory is its preset's name
# and the hash of that preset, so every shard of one generation writes a
# directory of this one name.
CORPUS_ROOT = "s3://a-bucket/corpus"
SHARDED_PRESET = "events-100mbs-skew"
CORPUS_DIR = f"{SHARDED_PRESET}-7aa0f164"

# What a shard prefix holds once a second preset has been generated into the
# same bucket. The prefixes are shared, and the batches under them are part of
# each merged corpus, so this is the steady state rather than leftovers.
TWO_CORPORA = f"                           PRE {CORPUS_DIR}/\n                           PRE smoke-e13842f9/"


def _stage_job_log(run_id: str) -> str:
    """What the stage Job printed, in the shape `stage` prints it.

    The run id first, because that is the line the driver reads.
    """
    return (
        f"run_id: {run_id}\n"
        f"bootstrap: {BOOTSTRAP}\n"
        f"topic: {run_id}\n"
        f"table: ingest_bench.t_{run_id.replace('-', '_')}\n"
        f"run_dir: /work/runs/{run_id}\n"
    )


STAGE_JOB_LOG = _stage_job_log(RUN_ID)

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
    job_log: str | None = None,
) -> DriverRun:
    """Run one driver in its own working directory with `kubectl` and `aws` stubbed.

    The working directory is the operator's: `./site.yaml` and `./runs` are
    resolved against it, so nothing here writes into the checkout. ``programs``
    stubs a harness command as well, which shadows the installed one because
    the stub directory is first on `PATH`, and ``job_log`` is what the stage
    Job printed — the run id a driver reads is only ever that Job's answer.
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
    job_log_file = tmp_path / "job.log"
    job_log_file.write_text(job_log if job_log is not None else STAGE_JOB_LOG)
    stubs = _stub_bin(tmp_path / "bin", {"kubectl": KUBECTL_STUB, "aws": AWS_STUB, **(programs or {})})

    result = subprocess.run(
        [str(script), *arguments],
        cwd=work,
        capture_output=True,
        text=True,
        # No terminal and nothing to read, so a driver that asks before it
        # deletes gets the same answer here however these tests were started —
        # from a shell whose stdin is a TTY as much as from CI.
        stdin=subprocess.DEVNULL,
        env={
            **os.environ,
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "STUB_LOG": str(calls),
            "STUB_AWS_LOG": str(aws_calls),
            "STUB_APPLIED_DIR": str(applied_dir),
            "STUB_JOB_LOG": str(job_log_file),
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


def _named_job(run: DriverRun, name: str) -> dict[str, object]:
    """The one applied document of ``name``, so a driver's Jobs can be told apart."""
    matching = [document for document in run.applied if _mapping(document["metadata"])["name"] == name]
    assert len(matching) == 1, f"expected one {name}, found {len(matching)}"
    return matching[0]


def _sharded_generation(tmp_path: Path, environment: dict[str, str]) -> DriverRun:
    """A two-shard generation whose shard prefixes hold two presets' corpora."""
    # Both logs in the shape the harness prints them: a shard's line carries
    # its index between the URI and the figures, and the merge's the shard
    # count — so a driver that read either by position reads them both.
    merge_log = tmp_path / "merge.log"
    merge_log.write_text(f"wrote {CORPUS_ROOT}/{CORPUS_DIR} from 2 shards: 12000000 rows, 600000000 encoded bytes\n")
    return _run_driver(
        GEN_CORPUS,
        [SHARDED_PRESET, "--shards", "2", "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_S3_LS": TWO_CORPORA, "STUB_MERGE_LOG": str(merge_log), **environment},
        # Shard 1's, because `kubectl logs job/<indexed job>` answers with one
        # of its pods and a driver may not depend on which: every shard writes
        # a directory of the same name under a prefix of its own.
        job_log=(
            f"wrote {CORPUS_ROOT}/shards/1/{CORPUS_DIR} shard 1 of 2: "
            "6000000 rows, 300000000 encoded bytes, 250000000 stored bytes\n"
        ),
    )


@needs_shell_tools
def test_a_sharded_merge_names_the_corpus_its_generation_wrote(tmp_path: Path) -> None:
    """The shard prefixes are shared, so the directory is chosen by name.

    Every multi-shard preset generated into one bucket writes under the same
    `shards/<i>/` prefixes, and the batches there are part of each merged
    corpus rather than leftovers — so a shard prefix holds one directory per
    preset ever generated, and the merge cannot be the one directory it finds.
    The name comes from what the generation itself reported writing.
    """
    run = _sharded_generation(tmp_path, {})

    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout.strip() == f"{CORPUS_ROOT}/{CORPUS_DIR}"
    command = _job_command(_named_job(run, f"corpus-merge-{SHARDED_PRESET}"))
    assert command == (
        f"merge-corpus {CORPUS_ROOT}/shards/0/{CORPUS_DIR} {CORPUS_ROOT}/shards/1/{CORPUS_DIR} --out {CORPUS_ROOT}"
    )
    # Every shard's copy is read before a merge pod is paid for, and by the
    # document that says the shard finished rather than by its prefix existing.
    for shard in (0, 1):
        assert f"s3 ls {CORPUS_ROOT}/shards/{shard}/{CORPUS_DIR}/corpus.json" in run.aws_calls, run.aws_calls


@needs_shell_tools
def test_a_generation_missing_a_shard_is_refused_before_the_merge(tmp_path: Path) -> None:
    """A shard that wrote nothing is named, rather than merged around.

    The merge reads every shard's metadata and publishes one document over the
    lot, so a missing shard is a corpus short of its batches — and the figures
    the whole run is scored against would describe a workload nobody offered.
    """
    run = _sharded_generation(tmp_path, {"STUB_S3_LS_ABSENT": f"shards/1/{CORPUS_DIR}"})

    assert run.result.returncode != 0
    assert f"{CORPUS_ROOT}/shards/1/{CORPUS_DIR}" in run.result.stderr, run.result.stderr
    assert [document for document in run.applied if _mapping(document["metadata"])["name"] == "corpus-merge"] == []


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

    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()
    spec = REPO_ROOT / "runs" / "smoke-flink.yaml"
    run = _run_driver(
        STAGE,
        [str(spec), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
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

    # Once it is RUNNING, the job is read back through a tunnel to the REST
    # Service the operator names after the deployment, and checked against the
    # copied spec — by an absolute path, because the harness may be run from
    # the checkout, which resolves a relative one against its own root.
    assert f"port-forward svc/{RUN_OBJECT}-rest 18081:8081" in run.calls
    checked = verify_calls.read_text().strip()
    assert Path(checked.split()[1]) == (tmp_path / "work" / "runs" / RUN_ID / "spec.yaml").resolve()
    assert checked.endswith(f"--run-id {RUN_ID} --rest http://localhost:18081")

    jobs = [document for document in run.applied if document["kind"] == "Job"]
    assert len(jobs) == 1, "staging applies one Job"
    command = _job_command(jobs[0])
    assert "stage --spec /runs/smoke-flink.yaml --site /site/site.yaml --runs-dir /work/runs" in command
    assert "--image-tag abc1234" in command
    assert "--upload-prefix s3://a-bucket/runs" in command


@needs_shell_tools
@pytest.mark.parametrize(("status", "refusal"), [(3, "name every setting it dropped"), (2, "in 3 tries")])
def test_stage_refuses_a_run_whose_engine_it_could_not_hold_to_the_spec(
    tmp_path: Path, status: int, refusal: str
) -> None:
    """Drift and an unreadable endpoint are refused, and for different reasons.

    A result is only ever attributed to the spec it was staged from, so a job
    whose effective settings are not that spec's — and a job whose settings
    could not be read at all — are both runs not worth offering a corpus to.
    Drift is final; an endpoint that did not answer is tried again first.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
            "STUB_VERIFY_STATUS": str(status),
            # The retries are the only wait left in this path, and three of
            # them at the driver's default would hold the test for half a
            # minute to prove the same thing.
            "ENGINE_POLL_S": "0",
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode != 0
    assert refusal in run.result.stderr, run.result.stderr
    assert f"run_id: {RUN_ID}" not in run.result.stdout
    expected_tries = 1 if status == 3 else 3
    assert len(verify_calls.read_text().splitlines()) == expected_tries


@needs_shell_tools
@pytest.mark.parametrize(
    ("statuses", "succeeds", "tries"),
    [
        # A fleet still being placed: the check says so, is waited for, and
        # passes once the last pod has an image to start from.
        ("4 4 0", True, 3),
        # One that never finishes being placed gets the engine's own running
        # wait rather than the endpoint's tries, so the refusal names the pods.
        ("4", False, 1),
    ],
)
def test_stage_waits_out_a_fleet_that_is_still_being_placed(
    tmp_path: Path, statuses: str, succeeds: bool, tries: int
) -> None:
    """Placement is a wait; an endpoint that did not answer is a retry.

    An object reaches its running state before its last pod has been
    scheduled, so a check reporting a half-placed fleet is answered by waiting
    — against the engine's own running wait, which is what a cold node and an
    image pull are already budgeted against — and not by the three tries an
    unreadable endpoint gets.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
            "STUB_VERIFY_STATUSES": statuses,
            # A placement wait is counted in poll intervals, so a zero poll
            # would never reach the expiry the second case asks for.
            "ENGINE_POLL_S": "1",
            "ENGINE_RUNNING_WAIT_S": "60" if succeeds else "0",
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )

    assert (run.result.returncode == 0) is succeeds, run.result.stderr
    assert len(verify_calls.read_text().splitlines()) == tries, verify_calls.read_text()
    if succeeds:
        assert run.result.stdout.splitlines()[-1] == f"run_id: {RUN_ID}"
    else:
        assert "was not fully placed within 0s" in run.result.stderr, run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize("script", [LAUNCH, TEARDOWN])
def test_a_driver_addresses_the_topic_staging_named(tmp_path: Path, script: Path) -> None:
    """The topic comes off `facts.json`, because staging is what created it.

    It is named after the run id today, and a driver that rebuilt the name
    from the id would publish to — or drop — a topic of its own the day the
    two stop being the same string, leaving the run's own behind.
    """
    topic = f"{RUN_ID}-as-staged"
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps({**FACTS, "topic": topic}))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(
        script,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_METADATA_LOG": str(tmp_path / "table-metadata-calls.log"), "STUB_METADATA_STATUS": "3"},
        programs={"table-metadata": TABLE_METADATA_STUB},
    )

    assert run.result.returncode == 0, run.result.stderr
    commands = [_job_command(document) for document in run.applied]
    published = [command for command in commands if "--topic" in command]
    assert published, commands
    for command in published:
        assert f"--topic {topic}" in command, command


@needs_shell_tools
def test_a_failed_stage_takes_its_configmaps_with_it(tmp_path: Path) -> None:
    """The two ConfigMaps a stage Job mounts belong to that Job alone.

    They carry the operator's own site config, so an exit that never reached
    the deletion of them — a Job that failed, a run directory that could not
    be fetched — would leave it in the namespace for as long as the cluster
    lives.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))

    # A stage Job whose log carries no run id: the driver refuses once the
    # ConfigMaps exist and before it reaches the lines that delete them.
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_STAGE_DIR": str(staged)},
        job_log="the job printed nothing this driver reads\n",
    )

    assert run.result.returncode != 0
    assert "printed no run_id line" in run.result.stderr, run.result.stderr
    for deleted in ("delete configmap stage-smoke-flink-spec", "delete configmap stage-smoke-flink-site"):
        assert deleted in run.calls, run.calls


@needs_shell_tools
def test_launch_passes_the_scorers_read_width_only_when_it_is_set(tmp_path: Path) -> None:
    """A knob this driver does not default: set it and it reaches the scorer.

    How many of a commit's data files are read at once is the scorer's own
    default, and a second default here would be a number to keep in step with
    that one — so the flag is appended when an operator names a width and left
    off entirely otherwise.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"SCORER_READ_WORKERS": "8"})
    assert run.result.returncode == 0, run.result.stderr
    scorer = _job_command(run.applied[0])
    assert "--read-workers 8" in scorer


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
    # Unset, so the scorer's own default stands rather than one this driver restates.
    assert "--read-workers" not in scorer
    # Every catalog property the site declares, because the scorer reads the
    # table itself and no site config reaches a pod.
    # Quoted, because a pod's shell splits this line: an unquoted value
    # would lose a `${env:NAME}` reference before Python could resolve it.
    assert "--catalog-prop 'uri=https://glue.eu-west-1.amazonaws.com/iceberg'" in scorer
    assert "--catalog-prop 'warehouse=123456789012'" in scorer
    # The spec's scoring keys, so the run scored is the run the spec asks for.
    assert "--warmup-s 60" in scorer and "--freshness-bound-s 60" in scorer

    assert f"--topic {RUN_ID}" in producer
    assert "--shard $JOB_COMPLETION_INDEX --shards 1" in producer
    assert "--publish-log /work/publish_log-$JOB_COMPLETION_INDEX.jsonl" in producer
    assert f"--upload-prefix s3://a-bucket/runs/{RUN_ID}" in producer
    assert "--key-column user_id" in producer
    # The MSK IAM properties, the harness's own signing region among them.
    assert "--kafka-prop 'security.protocol=SASL_SSL'" in producer
    assert "--kafka-prop 'aws.region=eu-west-1'" in producer

    assert _mapping(run.applied[1]["spec"])["completions"] == 1


@needs_shell_tools
@pytest.mark.parametrize("nodes", [2, 6], ids=["too-few", "enough"])
def test_launch_says_when_the_cluster_has_no_room_for_its_own_pods(tmp_path: Path, nodes: int) -> None:
    """A pod nothing schedules has an empty log, so the wait for it explains nothing.

    The scorer and every producer shard ask for a whole node's worth of CPU on
    the node size the shipped cluster builds, and a cluster already holding an
    engine fleet may have room for none of them. Warned and not refused: a
    cluster with an autoscaler is one where the Pending pod is what buys the
    node, so the launch has to go on to apply them.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    spec["producer"] = {**spec["producer"], "shards": 3}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    # One node already carries a pod of this size and the rest are empty, so
    # `nodes - 1` of them have room for one more.
    (tmp_path / "nodes.json").write_text(json.dumps({"items": [_node(f"node-{index}") for index in range(nodes)]}))
    (tmp_path / "pods.json").write_text(json.dumps({"items": [_pod("node-0", "2")]}))

    run = _run_driver(
        LAUNCH,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_NODES": str(tmp_path / "nodes.json"), "STUB_CLUSTER_PODS": str(tmp_path / "pods.json")},
    )

    assert run.result.returncode == 0, run.result.stderr
    # Either way both Jobs are applied: the count is advice, not a gate.
    assert [str(_mapping(document["metadata"])["name"]) for document in run.applied] == [
        f"scorer-{RUN_OBJECT}",
        f"producer-{RUN_OBJECT}",
    ]
    if nodes == 2:
        # The scorer plus its three shards, against the nodes with room for one.
        warning = f"{nodes - 1} of this cluster's nodes have 2000m of CPU free, and this run needs 4"
        assert warning in run.result.stderr, run.result.stderr
        assert f"see {AWS_DEPLOY.relative_to(REPO_ROOT).as_posix()}/README.md §Sizing the cluster" in run.result.stderr
    else:
        assert "of CPU free" not in run.result.stderr, run.result.stderr


def test_the_cpu_a_launch_counts_against_is_what_its_pods_request() -> None:
    """The driver's figure and the two manifests' requests are one number.

    A template raised to three cores with the driver still counting nodes that
    have two free would warn about a run that fits and say nothing about one
    that does not.
    """
    stated = re.search(r"^POD_CPU_MILLICORES=(\d+)$", LAUNCH.read_text(), re.M)
    assert stated is not None, "launch.sh no longer states what the pods it applies request"
    for name in ("scorer-job.yaml.tmpl", "producer-job.yaml.tmpl"):
        template = _mapping(yaml.safe_load((REPO_ROOT / "deploy" / "k8s" / name).read_text()))
        container = _mapping(_sequence(_pod_spec(template)["containers"])[0])
        requests = _mapping(_mapping(container["resources"])["requests"])
        assert requests["cpu"] == str(int(stated.group(1)) // 1000), name


@needs_shell_tools
@pytest.mark.parametrize("managed_by", ["engine", None])
def test_launch_starts_the_producer_for_a_table_its_engine_will_create(tmp_path: Path, managed_by: str | None) -> None:
    """The scorer is told who runs the DDL, so an absent table is a phase or a fault.

    Under `managed_by: engine` the table does not exist until the offer starts,
    and the scorer is applied first because its first reading is the baseline —
    so unless it reads an absent table as empty, the launch waits out its whole
    budget for a reading, the producer never starts, and the engine never sees
    the record it would have created the table from. Left off where the spec
    says nothing, so the scorer applies its own default.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    if managed_by is not None:
        spec["table"] = {**spec["table"], "managed_by": managed_by}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {})

    assert run.result.returncode == 0, run.result.stderr
    scorer = _job_command(_named_job(run, f"scorer-{RUN_OBJECT}"))
    if managed_by is None:
        assert "--table-managed-by" not in scorer, scorer
    else:
        assert f"--table-managed-by {managed_by}" in scorer, scorer
    # And the producer is applied, which is what the ordering above exists to
    # reach.
    assert _job_command(_named_job(run, f"producer-{RUN_OBJECT}")).startswith("produce ")


@needs_shell_tools
def test_a_batch_sized_pod_can_be_given_the_memory_its_preset_needs(tmp_path: Path) -> None:
    """The two Jobs whose peak follows the preset's batch bytes, not the pod count.

    A generator holds a whole batch while it encodes one and a producer shard
    reads one whole batch object and decompresses it whole, so a 600 MB/s
    preset's batch needs gigabytes where the smoke one's needs hundreds of
    megabytes. Both requests come from the driver's own variable, so raising
    either is an environment variable and not an edit to this repository.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    launched = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"PRODUCER_MEMORY": "8Gi"})
    assert launched.result.returncode == 0, launched.result.stderr
    producer = _named_job(launched, f"producer-{RUN_OBJECT}")
    requests = _mapping(_mapping(_mapping(_sequence(_pod_spec(producer)["containers"])[0])["resources"])["requests"])
    assert requests["memory"] == "8Gi"

    generated = _run_driver(
        GEN_CORPUS,
        [SHARDED_PRESET, "--image-tag", "abc1234"],
        tmp_path,
        {"GEN_MEMORY": "6Gi"},
        job_log=f"wrote {CORPUS_ROOT}/{CORPUS_DIR}: 12000000 rows, 600000000 encoded bytes\n",
    )
    assert generated.result.returncode == 0, generated.result.stderr
    generator = _named_job(generated, f"corpus-gen-{SHARDED_PRESET}")
    requests = _mapping(_mapping(_mapping(_sequence(_pod_spec(generator)["containers"])[0])["resources"])["requests"])
    assert requests["memory"] == "6Gi"


@needs_shell_tools
def test_two_generations_of_different_presets_are_two_jobs(tmp_path: Path) -> None:
    """A driver deletes the Job of its own name on the way in, so the name is the preset's.

    Under one fixed name a second generation would delete the first hours into
    it, and the first driver would then wait out its budget on a Job that no
    longer exists.
    """
    run = _run_driver(
        GEN_CORPUS,
        [SHARDED_PRESET, "--image-tag", "abc1234"],
        tmp_path,
        {},
        job_log=f"wrote {CORPUS_ROOT}/{CORPUS_DIR}: 12000000 rows, 600000000 encoded bytes\n",
    )
    assert run.result.returncode == 0, run.result.stderr
    name = str(_mapping(run.applied[0]["metadata"])["name"])
    assert name == f"corpus-gen-{SHARDED_PRESET}", name
    assert f"delete job corpus-gen-{SHARDED_PRESET}" in run.calls, run.calls


@needs_shell_tools
def test_launch_offers_the_codec_the_spec_names(tmp_path: Path) -> None:
    """The producer Job carries `--compression`, so the wire is the spec's."""
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    spec["producer"] = {**spec["producer"], "compression": "lz4"}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {})

    assert run.result.returncode == 0, run.result.stderr
    producer = _job_command(run.applied[1])
    assert "--compression lz4" in producer


@needs_shell_tools
def test_the_jobs_a_launch_applies_name_a_credential_and_never_hold_one(tmp_path: Path) -> None:
    """The whole of the secret path, end to end through the two Jobs a launch applies.

    The site names a variable and the Secret that answers it. What is applied
    has to carry the reference and the Secret's name and nothing else: the Job
    documents are the objects a namespace-reader sees, and the same text is
    what `stage` published into the run's prefix in the bucket.
    """
    reference = "${env:IB_KAFKA_PASSWORD}"
    site = (
        _filled_site()
        .replace("    aws.region:", f"    sasl.password: '{reference}'\n    aws.region:")
        .replace("  aws_region:", "  secret_name: bench-env\n  aws_region:")
    )
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {}, site=site)

    assert run.result.returncode == 0, run.result.stderr
    for document in run.applied:
        container = _mapping(_sequence(_pod_spec(document)["containers"])[0])
        assert container["envFrom"] == [{"secretRef": {"name": "bench-env"}}]
    producer = _job_command(_named_job(run, f"producer-{RUN_OBJECT}"))
    assert f"--kafka-prop 'sasl.password={reference}'" in producer
    # And nothing applied resolved it: only the pod's own process may.
    assert "IB_KAFKA_PASSWORD}" in producer and "sasl.password=$" in producer


@needs_shell_tools
def test_launch_refuses_to_start_the_producer_once_its_lead_has_expired(tmp_path: Path) -> None:
    """The scorer's first-reading wait can eat EPOCH_LEAD_S; the producer must not start once it has.

    A producer applied with the epoch no longer safely ahead has its first
    batch due before its first connection opens, and the run voids as
    producer_bound — a wasted fleet the check turns into a named refusal
    instead.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"EPOCH_LEAD_S": "10"})

    assert run.result.returncode != 0
    assert "EPOCH_LEAD_S" in run.result.stderr
    # The scorer is applied before the lead is checked; only the producer —
    # the one that would be minutes late — must not start.
    assert [str(_mapping(document["metadata"])["name"]) for document in run.applied] == [f"scorer-{RUN_OBJECT}"]


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
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
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
    assert "--kafka-prop 'security.protocol=SASL_SSL'" in command

    # The catalog is addressed with every property the site declares.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in metadata_calls.read_text()

    assert (f"s3 cp {location}" in run.aws_calls) is copied
    assert (run.result.returncode != 0) is refused
    if refused:
        assert "table-metadata exited 1" in run.result.stderr
        assert "could not reach the catalog" in run.result.stderr


# `gate` as the driver calls it, answering the verdict a case asks for. The
# artifacts it would read are the scorer's, and the stub reads none of them.
GATE_STUB = """
printf '%s\\n' "$*" >>"$STUB_GATE_LOG"
exit "${STUB_GATE_STATUS:-0}"
"""


@needs_shell_tools
def test_a_teardown_that_failed_does_not_replace_the_verdict(tmp_path: Path) -> None:
    """The exit code is the gate's own, whatever the teardown it asked for did.

    A teardown replacing it would report a status this script never defines,
    and a caller that reads 0 PASS, 3 UNDERSIZED and 5 VOID would have to
    guess which of them a run had reached. The teardown here fails because
    there is no run directory for it to read, which is also the shape of the
    real failure: a run torn down twice.
    """
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()

    run = _run_driver(
        GATE,
        # `--breaches 1` so this one tick reaches the teardown: how many ticks
        # it waits for is the case beside this one.
        [RUN_ID, "--teardown", "--breaches", "1"],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_GATE_STATUS": "3"},
        programs={"gate": GATE_STUB},
    )

    assert run.result.returncode == 3, run.result.stdout + run.result.stderr
    assert "so its fleet may still be running" in run.result.stderr, run.result.stderr
    # The two artifacts the gate reads and the spec whose windows it judges by,
    # and no others: fetching the whole set every minute would pay for a run's
    # record to answer one question.
    assert [line.split("/")[-1] for line in run.aws_calls.splitlines()] == [
        "summary.json",
        "keepup_samples.jsonl",
        "spec.yaml",
    ]


@needs_shell_tools
def test_the_gate_reads_the_runs_own_windows_out_of_the_bucket(tmp_path: Path) -> None:
    """The windows come from the published spec, so the verdict is the run's own.

    Read from a local run directory instead, the verdict would change with the
    presence of a file: gating the same run from another machine, or after
    `RUNS_DIR` moved, would silently fall back to the gate's own defaults — and
    a run that asked for a longer adaptation precisely to survive its cold
    start would be judged at the shorter one and torn down.
    """
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    published = tmp_path / "published"
    published.mkdir()
    spec = yaml.safe_load((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    spec["scoring"] = {**spec["scoring"], "gate_adaptation_s": 300, "gate_window_s": 90}
    (published / "spec.yaml").write_text(yaml.safe_dump(spec))

    run = _run_driver(
        GATE,
        [RUN_ID],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_S3_CP_DIR": str(published)},
        programs={"gate": GATE_STUB},
    )

    assert run.result.returncode == 0, run.result.stdout + run.result.stderr
    assert "--adaptation-s 300" in gate_calls.read_text(), gate_calls.read_text()
    assert "--window-s 90" in gate_calls.read_text(), gate_calls.read_text()
    fetched = [line.split("/")[-1] for line in run.aws_calls.splitlines()]
    assert fetched == ["summary.json", "keepup_samples.jsonl", "spec.yaml"], run.aws_calls


@needs_shell_tools
def test_a_spec_the_gate_could_not_read_is_a_refusal_and_not_a_default(tmp_path: Path) -> None:
    """A window the gate guessed is a verdict about a run nobody asked for."""
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    run = _run_driver(
        GATE,
        [RUN_ID],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_S3_CP_ABSENT": "stage/spec.yaml"},
        programs={"gate": GATE_STUB},
    )
    assert run.result.returncode != 0
    assert "stage/spec.yaml" in run.result.stderr, run.result.stderr
    assert gate_calls.read_text() == "", "the gate was asked for a verdict with no windows to judge by"


def _gated(tmp_path: Path, arguments: list[str], status: str, gate_calls: Path) -> subprocess.CompletedProcess[str]:
    """One `gate.sh` tick, in a working directory the ticks share.

    Shared because the count of consecutive verdicts lives beside the run, so
    what is under test is what one tick leaves for the next.
    """
    run = _run_driver(
        GATE,
        [RUN_ID, *arguments],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_GATE_STATUS": status},
        programs={"gate": GATE_STUB},
    )
    return run.result


@needs_shell_tools
@pytest.mark.parametrize("required", [None, 1])
def test_a_teardown_waits_for_the_verdict_to_repeat(tmp_path: Path, required: int | None) -> None:
    """One tick is not a run's answer, and `--teardown` destroys the fleet.

    The gate judges the lag as of the last sample, so a fleet still working
    through a cold start, a checkpoint that took a moment, or a poll that read
    a stale prefix each produce a single breaching tick that the next one
    contradicts. Requiring the verdict to repeat is what separates those from
    a fleet that will never catch up. `--breaches 1` acts on the first
    breaching tick, for a caller that wants it.
    """
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    arguments = ["--teardown"] if required is None else ["--teardown", "--breaches", str(required)]
    ticks = 3 if required is None else 1

    for number in range(1, ticks):
        early = _gated(tmp_path, arguments, "3", gate_calls)
        assert early.returncode == 3, early.stdout + early.stderr
        assert f"breach {number} of {ticks}" in early.stderr, early.stderr
        assert "tearing" not in early.stderr, early.stderr

    acted = _gated(tmp_path, arguments, "3", gate_calls)
    assert acted.returncode == 3, acted.stdout + acted.stderr
    assert f"not been PASS for {ticks} consecutive" in acted.stderr, acted.stderr
    assert "so its fleet may still be running" in acted.stderr, acted.stderr


@needs_shell_tools
def test_a_pass_forgets_the_breaches_before_it(tmp_path: Path) -> None:
    """Consecutive means consecutive: one passing tick starts the count again.

    Otherwise a run that breached twice hours apart would be torn down by an
    unrelated third, which is the same false positive the requirement exists
    to remove.
    """
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()

    breached = _gated(tmp_path, ["--teardown"], "3", gate_calls)
    assert breached.returncode == 3 and "breach 1 of 3" in breached.stderr, breached.stderr
    passed = _gated(tmp_path, ["--teardown"], "0", gate_calls)
    assert passed.returncode == 0, passed.stdout + passed.stderr
    again = _gated(tmp_path, ["--teardown"], "3", gate_calls)
    assert "breach 1 of 3" in again.stderr, again.stderr


@needs_shell_tools
def test_a_gate_without_teardown_judges_and_leaves_the_fleet_alone(tmp_path: Path) -> None:
    """The verdict is the exit code either way; only `--teardown` acts on it."""
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    for status in ("0", "3", "5"):
        judged = _gated(tmp_path, [], status, gate_calls)
        assert judged.returncode == int(status), judged.stdout + judged.stderr
        assert "tearing" not in judged.stderr, judged.stderr


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
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
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
@pytest.mark.parametrize("engine", ["flink", "spark"])
def test_stage_addresses_an_engine_by_the_names_its_own_module_declares(tmp_path: Path, engine: str) -> None:
    """No engine's names are in the shell, so a third engine adds no line to it.

    The kind of object a run is, where its state sits, the Service that carries
    its API, the pods provenance is read off and whether its check is handed a
    pod list at all: each comes from the engine's descriptor, and each is what
    the driver would otherwise have had to hardcode per engine.
    """
    descriptor = engines.kubernetes_for(engine)
    run_id = f"smoke-{engine}-20260908T120000Z"
    run_object = run_id.lower()
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps({**FACTS, "run_id": run_id, "topic": run_id}))
    for name in ("spec.yaml", descriptor.document_file, descriptor.configmap_file):
        (staged / name).write_text(f"# {name}\n")
    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / f"smoke-{engine}.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
            "STUB_PODS": json.dumps({"items": [{"metadata": {"name": f"{run_object}-driver"}}]}),
        },
        programs={"curl": CURL_STUB, f"verify-{engine}": VERIFY_STUB},
        job_log=_stage_job_log(run_id),
    )
    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout.splitlines()[-1] == f"run_id: {run_id}"

    # Both documents the engine's own renderer wrote, the ConfigMap first
    # because the other one mounts it.
    applied = [line.rsplit("/", 1)[-1] for line in run.calls.splitlines() if " apply -f ./runs/" in line]
    assert applied == [descriptor.configmap_file, descriptor.document_file]

    assert f"get {descriptor.kind}/{run_object} -o jsonpath={descriptor.state_jsonpath}" in run.calls
    assert f"port-forward svc/{run_object}{descriptor.rest_service_suffix} 18081:{descriptor.rest_port}" in run.calls
    assert f"get pod -l {for_name(descriptor.provenance_selector, run_object)}" in run.calls

    checked = verify_calls.read_text()
    assert f"--run-id {run_id} --rest http://localhost:18081" in checked
    if descriptor.pods_selector:
        assert f"get pods -l {for_name(descriptor.pods_selector, run_object)} -o json" in run.calls
        # The document itself, echoed by the check: the driver writes it to a
        # temporary file it removes on the way out, so a path alone would not
        # say the pods had been read by the time the check ran.
        assert "--pods " in checked
        assert json.loads(checked.splitlines()[1])["items"][0]["metadata"]["name"] == f"{run_object}-driver"
    else:
        assert "get pods -l" not in run.calls
        assert "--pods" not in checked


def test_the_shell_reads_every_field_the_descriptor_prints() -> None:
    """A field no driver reads is a refusal, so adding one to the descriptor is a shell change.

    `k8s_read_engine` dies on a key it has no case for, and it would take every
    cluster run with it — at staging, after the topic and the table exist. So
    the two lists are held together here rather than by a run.
    """
    text = K8S_LIB.read_text()
    for field in FIELDS:
        assert f"\n\t\t{field}) ENGINE_" in text, f"scripts/_k8s.sh reads no {field}"


def test_the_shell_and_the_descriptor_mark_a_run_s_name_the_same_way() -> None:
    """The one name in the descriptor the driver has to substitute itself.

    Two spellings of the marker would leave a driver addressing an object
    called `<name>` — which the API server refuses, minutes into a staged run.
    """
    assert f"ENGINE_NAME_MARKER='{NAME}'" in (SCRIPTS / "_k8s.sh").read_text()


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
        {"STUB_STAGE_DIR": str(staged), "STUB_ENGINE_STATE": "FAILED"},
    )
    assert run.result.returncode != 0
    assert f"flinkdeployment/{RUN_OBJECT} went to FAILED" in run.result.stderr
    assert f"logs deploy/{RUN_OBJECT} --tail=40" in run.calls


@needs_shell_tools
@pytest.mark.parametrize("pods", [False, True])
def test_a_document_the_operator_rejected_ends_the_wait_at_once(tmp_path: Path, pods: bool) -> None:
    """A rejected document reports no state at all, so the state wait never ends.

    The state belongs to a job the operator never created; the rejection sits
    in the error field instead. Without reading it a driver would spend the
    whole running wait on a document that will never run — and the text there
    is the only statement of why it was rejected.

    A document rejected outright has no pods to have written a log, and one the
    operator gave up on after starting them does — so the tail is on the pods
    existing rather than on the kind of failure. `get pod -l` is what the stub
    answers, so a named image there stands for a fleet that was started.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    rejection = "the length must be no more than 45 characters"

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            # No state, which is what an operator that created no job reports,
            # and a running wait long enough that spending it would be the
            # failure rather than the assertion below.
            "STUB_ENGINE_STATE": "",
            "STUB_ENGINE_ERROR": rejection,
            # The operator has given up on the document, which is what makes
            # the error a refusal rather than a step on the way.
            "STUB_ENGINE_LIFECYCLE": "FAILED",
            "ENGINE_RUNNING_WAIT_S": "30",
            "ENGINE_POLL_S": "1",
            **({"STUB_ENGINE_IMAGE": f"a-registry/flink:abc1234 sha256:{'a' * 64}"} if pods else {}),
        },
    )
    assert run.result.returncode != 0
    assert rejection in run.result.stderr
    assert "gave up" in run.result.stderr and "FAILED" in run.result.stderr
    assert "did not reach" not in run.result.stderr, "the error is the refusal, not the timeout"
    assert f"get flinkdeployment/{RUN_OBJECT} -o jsonpath={{.status.error}}" in run.calls
    assert f"get flinkdeployment/{RUN_OBJECT} -o jsonpath={{.status.lifecycleState}}" in run.calls

    tailed = f"logs deploy/{RUN_OBJECT} --tail=40" in run.calls
    assert tailed is pods, "the log is read exactly when there are pods to have written one"


@needs_shell_tools
def test_an_error_the_operator_has_not_given_up_over_does_not_end_the_wait(tmp_path: Path) -> None:
    """A reconcile the operator will retry writes an error field too.

    So the error alone cannot be the refusal: an engine whose first reconcile
    hit a transient failure and whose second would have succeeded would be torn
    down over a document that was on its way up. The lifecycle beside the error
    is what separates the two, and until it says the operator has given up the
    error is reported once and waited out.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    transient = "could not get the deployment, retrying"

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_ENGINE_STATE": "",
            "STUB_ENGINE_ERROR": transient,
            # Still being reconciled, which is every lifecycle but the failed one.
            "STUB_ENGINE_LIFECYCLE": "DEPLOYED",
            "ENGINE_RUNNING_WAIT_S": "4",
            "ENGINE_POLL_S": "2",
        },
    )
    assert run.result.returncode != 0
    # The wait is what ends it, and the error it saw is named in that refusal
    # rather than being the refusal.
    assert "did not reach" in run.result.stderr
    assert f"last error: {transient}" in run.result.stderr
    assert "gave up" not in run.result.stderr
    # Reported when it appeared and not once per poll.
    assert run.result.stderr.count("has not given up over") == 1


# ---------------------------------------------------------------------------
# Geometry, collection and reclamation
# ---------------------------------------------------------------------------

# The site's warehouse root, as the filled example declares it, and where the
# run's table put its files under it. A location is read from the copied
# metadata document rather than derived, because a catalog places a table where
# it likes under its warehouse; and it is checked against that root, because
# every prefix at or above the root is other data.
WAREHOUSE_ROOT = "s3://a-bucket/warehouse"
TABLE_LOCATION = f"{WAREHOUSE_ROOT}/ingest_bench/t_smoke_flink_20260908T120000Z-1a2b"

# A geometry document shaped like `file-sizes` writes one, with only the fields
# the verdict's last line reads.
GEOMETRY_DOCUMENT = json.dumps(
    {
        "final": {
            "live": {
                "files": 137,
                "rows": 1000,
                "bytes": 5_000_000_000,
                "size_quantiles": {"p50": 40_100_000.0},
                "small_file_share_32mib": 0.2847,
            }
        }
    }
)


def _torn_down_run(
    tmp_path: Path, *, run_valid: bool = True, metadata: bool = True, location: str = TABLE_LOCATION
) -> Path:
    """A run directory as `teardown.sh` leaves one, in the operator's working directory."""
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    (run_dir / "scores").mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps({**FACTS, "epoch": 1757419200}))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "scores" / "summary.json").write_text(
        json.dumps(
            {
                "run_valid": run_valid,
                "state": "drained" if run_valid else "idle_stop",
                "reason": "",
                "producer_bound": False,
                "prefix": 10,
                "last_batch": 10,
                "committed_rows": 100,
                "offered_rows": 100,
                "freshness": {"window": {"p95": 3.0}},
                "exactness": {"exact": run_valid, "loss_rows": 0, "duplicate_rows": 0},
                "keepup": {"absorbed_at_offer_end": 0.9},
            }
        )
    )
    if metadata:
        (run_dir / "table-metadata.final.json").write_text(json.dumps({"location": location}))
    return run_dir


def _stub_logs(tmp_path: Path, names: dict[str, str]) -> dict[str, str]:
    """Log paths for the stubs, created empty.

    Created rather than left to the stub, so that "the stub never ran" reads as
    an empty file rather than as a missing one — the assertion that nothing was
    deleted is exactly that read.
    """
    environment = {}
    for variable, name in names.items():
        path = tmp_path / name
        path.touch()
        environment[variable] = str(path)
    return environment


def _finish_environment(tmp_path: Path) -> dict[str, str]:
    return {
        **_stub_logs(
            tmp_path,
            {
                "STUB_COLLECT_LOG": "collect.log",
                "STUB_FILE_SIZES_LOG": "file-sizes.log",
                "STUB_RESULTS_TABLE_LOG": "results-table.log",
            },
        ),
        "STUB_GEOMETRY": GEOMETRY_DOCUMENT,
    }


FINISH_PROGRAMS = {
    "collect": COLLECT_STUB,
    "file-sizes": FILE_SIZES_STUB,
    "results-table": RESULTS_TABLE_STUB,
}


@needs_shell_tools
def test_finish_measures_the_geometry_from_the_copied_document_and_reports_it(tmp_path: Path) -> None:
    """The document, the epoch and the spec's ladder, and the p50 on the verdict.

    Geometry is read from the copied document rather than through the catalog,
    because every figure in it is about files and a finished campaign may have
    dropped the table from its catalog already. The epoch is the ladder's
    origin and only the launch knew it, so it comes off the run's own facts.
    """
    _torn_down_run(tmp_path)
    run = _run_driver(FINISH, [RUN_ID], tmp_path, _finish_environment(tmp_path), programs=FINISH_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr

    measured = (tmp_path / "file-sizes.log").read_text()
    assert f"/work/runs/{RUN_ID}/table-metadata.final.json" in measured
    assert "--epoch 1757419200" in measured
    assert f"/work/runs/{RUN_ID}/scores" in measured
    # Absolute, for the reason `abs_path` gives.
    assert "--metadata /" in measured and "--out /" in measured
    # Every property the site declares, so the manifests the figures come from
    # are read with a region rather than against the global endpoint.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in measured
    # The shipped smoke spec sets no ladder, so the flag is left off and
    # `file-sizes` applies its own default instead of one restated in the shell.
    assert "--offsets" not in measured

    assert "geometry: p50 38.2 MiB, small (<32 MiB) 28.5%, 137 files" in run.result.stdout
    # Both sides of the run are fetched: the scorer's artifacts and the publish
    # logs the offered figures are derived from.
    assert f"s3 sync s3://a-bucket/runs/{RUN_ID}/scores/ ./runs/{RUN_ID}/scores/" in run.aws_calls
    assert f"s3 sync s3://a-bucket/runs/{RUN_ID}/producer/ ./runs/{RUN_ID}/producer/" in run.aws_calls


@needs_shell_tools
def test_finish_reports_a_table_that_never_committed_without_failing(tmp_path: Path) -> None:
    """`file-sizes`' no-geometry code is a fact about the run, not a failed read.

    A run whose engine never committed has no files to measure, and refusing
    there would cost it the document that says so — which is the one artifact
    that explains what happened.
    """
    _torn_down_run(tmp_path)
    run = _run_driver(
        FINISH,
        [RUN_ID],
        tmp_path,
        {**_finish_environment(tmp_path), "STUB_FILE_SIZES_STATUS": "4"},
        programs=FINISH_PROGRAMS,
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "no geometry: the table never committed" in run.result.stderr
    assert "geometry: p50" not in run.result.stdout
    assert (tmp_path / "collect.log").read_text().strip() != "", "the run is still collected"


@needs_shell_tools
def test_finish_refuses_to_publish_an_invalid_run_unless_told_to(tmp_path: Path) -> None:
    """`run_valid: false` is not a headline result, and `--publish-invalid` is the exception.

    Publishing is refused before anything is written, and the refusal names the
    flag: a result whose validity state is disclosed is publishable under
    publication rules, one that quietly stands beside the valid ones is not.
    """
    _torn_down_run(tmp_path, run_valid=False)
    refused = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results"],
        tmp_path,
        _finish_environment(tmp_path),
        programs=FINISH_PROGRAMS,
    )
    assert refused.result.returncode != 0
    assert "run_valid is false" in refused.result.stderr and "--publish-invalid" in refused.result.stderr
    # One collect, into the run directory: nothing reached the results tree.
    assert len((tmp_path / "collect.log").read_text().splitlines()) == 1
    assert not (tmp_path / "work" / "results").exists()
    assert (tmp_path / "results-table.log").read_text() == ""


@needs_shell_tools
def test_finish_publishes_an_invalid_run_when_told_to_and_still_refuses_it(tmp_path: Path) -> None:
    """The document is published and the verdict still fails.

    Two different questions: whether a result may be recorded with its state
    disclosed, and whether this run passed. `--publish-invalid` answers only
    the first, so the exit code has to stay non-zero — a sweep branching on it
    must not read a published invalid run as a passing one.
    """
    _torn_down_run(tmp_path, run_valid=False)
    run = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results", "--publish-invalid", "--variant", "hash-fanout"],
        tmp_path,
        _finish_environment(tmp_path),
        programs=FINISH_PROGRAMS,
    )
    assert run.result.returncode != 0, "an invalid run is still an invalid run"
    assert "run_valid is false; the block above says why" in run.result.stderr

    collected = (tmp_path / "collect.log").read_text().splitlines()
    assert len(collected) == 2, "one document for the run directory and one for the results tree"
    # The engine's own subdirectory, with the trailing separator that says the
    # path is a directory `collect` fills with the published name rather than a
    # file name the shell guessed at.
    assert "--out /" in collected[1] and "/work/results/flink/" in collected[1]
    assert "--variant hash-fanout" in collected[0] and "--variant hash-fanout" in collected[1]
    # The table is generated from the published documents, never hand-edited.
    rendered = (tmp_path / "results-table.log").read_text()
    assert "/work/results --out /" in rendered and "/work/results/RESULTS.md" in rendered


@needs_shell_tools
def test_finish_refuses_to_publish_a_document_that_names_no_engine(tmp_path: Path) -> None:
    """The engine names the directory the result is filed under.

    `jq -r` prints the string `null` for a field a document does not carry, so
    reading it without a refusal would file the result under `results/null/` —
    a directory the results table would then render a row out of.
    """
    _torn_down_run(tmp_path)
    run = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results"],
        tmp_path,
        {**_finish_environment(tmp_path), "STUB_COLLECT_NO_ENGINE": "1"},
        programs=FINISH_PROGRAMS,
    )
    assert run.result.returncode != 0
    assert "names no engine" in run.result.stderr
    assert not (tmp_path / "work" / "results" / "null").exists()
    assert len((tmp_path / "collect.log").read_text().splitlines()) == 1


@needs_shell_tools
@pytest.mark.skipif(
    "results-table = " in (REPO_ROOT / "pyproject.toml").read_text(),
    reason="this checkout declares results-table, so there is no absent-renderer branch to take",
)
def test_finish_leaves_the_results_table_alone_when_the_renderer_is_absent(tmp_path: Path) -> None:
    """A checkout without `results-table` still publishes the document.

    The guard is on the command being available to `harness_local`, not on it
    being on `PATH`: in a checkout `uv` provides it, and this project's script
    table is the only statement of which commands exist.
    """
    _torn_down_run(tmp_path)
    published = dict(FINISH_PROGRAMS)
    del published["results-table"]
    run = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results"],
        tmp_path,
        _finish_environment(tmp_path),
        programs=published,
    )
    assert run.result.returncode == 0, run.result.stderr
    assert len((tmp_path / "collect.log").read_text().splitlines()) == 2
    assert "no results-table command in this checkout" in run.result.stderr


@needs_shell_tools
def test_teardown_fetches_the_scores_and_collects_the_run(tmp_path: Path) -> None:
    """A torn-down run has a document even if nothing is ever done with it again.

    The pod that wrote the scores is gone by then, so they are fetched before
    the document is assembled from them; the metadata document is copied both
    beside the run and into the bucket, because the local copy is what
    `finish.sh` and `purge.sh` open and the other is what outlives this
    machine's working directory.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")

    location = f"{TABLE_LOCATION}/metadata/00003-abc.metadata.json"
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "0",
            "STUB_METADATA_OUT": location,
            "STUB_COLLECT_LOG": str(tmp_path / "collect.log"),
        },
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr

    assert f"s3 sync s3://a-bucket/runs/{RUN_ID}/scores/ ./runs/{RUN_ID}/scores/" in run.aws_calls
    assert f"s3 cp {location} ./runs/{RUN_ID}/table-metadata.final.json" in run.aws_calls
    assert (
        f"s3 cp ./runs/{RUN_ID}/table-metadata.final.json "
        f"s3://a-bucket/runs/{RUN_ID}/table-metadata.final.json" in run.aws_calls
    )

    # Absolute, for the reason `abs_path` gives.
    collected = (tmp_path / "collect.log").read_text()
    assert "--run-dir /" in collected and f"/work/runs/{RUN_ID}" in collected
    assert "--site /" in collected and "/work/site.yaml" in collected


@needs_shell_tools
def test_the_engine_image_is_recorded_off_the_jobmanager_pod(tmp_path: Path) -> None:
    """The digest the node pulled, not the tag it was pulled under.

    A floating tag repointed after a run would otherwise leave a result naming
    an image that is no longer the one measured. `app` and `component` are the
    operator's own labels on the pods it creates — the FlinkDeployment declares
    none — and both are lowercase because the run id's stamp is not.
    """
    staged = _staged_engine_run(tmp_path)
    image = "a-registry/lakehouse-ingest-bench/flink:abc1234"
    digest = f"{image.split(':')[0]}@sha256:{'a' * 64}"
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {**_verify_environment(tmp_path), "STUB_STAGE_DIR": str(staged), "STUB_ENGINE_IMAGE": f"{image} {digest}"},
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr

    assert f"get pod -l app={RUN_OBJECT},component=jobmanager" in run.calls
    recorded = json.loads((tmp_path / "work" / "runs" / RUN_ID / "engine-image.json").read_text())
    assert recorded == {"image": image, "digest": digest}


@needs_shell_tools
def test_a_pod_that_reports_no_digest_still_stages(tmp_path: Path) -> None:
    """Provenance is not worth failing a run over.

    A null digest is a recorded absence, which `collect` names in `missing`;
    inventing one from the tag would put a claim in a result that nothing
    checked.
    """
    staged = _staged_engine_run(tmp_path)
    image = "a-registry/lakehouse-ingest-bench/flink:abc1234"
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "runs" / "smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            **_verify_environment(tmp_path),
            "STUB_STAGE_DIR": str(staged),
            "STUB_ENGINE_IMAGE": f"{image} ",
            "ENGINE_IMAGE_WAIT_S": "0",
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    recorded = json.loads((tmp_path / "work" / "runs" / RUN_ID / "engine-image.json").read_text())
    assert recorded == {"image": image, "digest": None}


def _staged_engine_run(tmp_path: Path) -> Path:
    """What the stage Job published, as the AWS stub copies it into the run directory.

    `spec.yaml` among the documents because staging checks the started engine
    against the spec it was staged from before it goes any further.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    return staged


def _verify_environment(tmp_path: Path) -> dict[str, str]:
    """The logs the engine check and the tunnel behind it write to."""
    return _stub_logs(tmp_path, {"STUB_CURL_LOG": "curl-calls.log", "STUB_VERIFY_LOG": "verify-calls.log"})


def _purge_environment(tmp_path: Path) -> dict[str, str]:
    return _stub_logs(tmp_path, {"STUB_DROP_TABLE_LOG": "drop-table.log"})


PURGE_PROGRAMS = {"drop-table": DROP_TABLE_STUB}


@needs_shell_tools
@pytest.mark.parametrize("name", ["00003-abc.gz.metadata.json", "00003-abc.metadata.json"])
def test_a_compressed_metadata_document_is_stored_as_the_json_its_readers_parse(tmp_path: Path, name: str) -> None:
    """Iceberg allows a gzip-compressed metadata document, and `jq` cannot read one.

    The codec is a table property, so which of the two a run ends up with is
    the writer's choice and not the harness's. Both readers of the copied
    document parse it as JSON, and a gzip body reaches them as a syntax error
    against a table nothing can then reclaim — so it is decompressed on the way
    in and the file is JSON whichever way the table wrote it.

    Both names, because the body is what decides: `*.gz.metadata.json` is the
    convention the codec usually travels under, and a table that compressed its
    metadata without taking that name has to be read the same way.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    (run_dir / "scores").mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps({**FACTS, "epoch": 1757419200}))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "runs" / "smoke-flink.yaml").read_text())
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")

    published = tmp_path / "published"
    published.mkdir()
    document = {"location": TABLE_LOCATION, "format-version": 2}
    (published / name).write_bytes(gzip.compress(json.dumps(document).encode()))

    torn = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "0",
            "STUB_METADATA_OUT": f"{TABLE_LOCATION}/metadata/{name}",
            "STUB_S3_CP_DIR": str(published),
            "STUB_COLLECT_LOG": str(tmp_path / "collect.log"),
        },
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert torn.result.returncode == 0, torn.result.stderr

    stored = run_dir / "table-metadata.final.json"
    assert json.loads(stored.read_text()) == document, "the stored copy is the JSON, not the gzip"

    # purge parses this file for the location it removes, so a gzip body would
    # end it on a syntax error with the table intact.
    purged = _run_driver(PURGE, [RUN_ID, "--yes"], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS)
    assert purged.result.returncode == 0, purged.result.stderr
    assert f"s3 rm --recursive {TABLE_LOCATION}" in purged.aws_calls


@needs_shell_tools
def test_purge_names_what_it_would_remove_and_removes_nothing_unasked(tmp_path: Path) -> None:
    """Every deletion is stated first, and an unanswered prompt is not consent.

    This is the one script that deletes measured data, so "nothing answered"
    has to end it. Without a terminal there is nothing to answer with, which is
    also what a purge run from a script looks like — hence `--yes`.
    """
    _torn_down_run(tmp_path)
    run = _run_driver(PURGE, [RUN_ID], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS)
    assert run.result.returncode != 0
    assert "pass --yes to run unattended" in run.result.stderr

    assert str(FACTS["table"]) in run.result.stdout
    assert TABLE_LOCATION in run.result.stdout
    assert f"s3://a-bucket/runs/{RUN_ID}/" not in run.result.stdout, "the artifacts go only with --artifacts"

    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
def test_purge_refuses_while_the_run_is_still_being_scored(tmp_path: Path) -> None:
    """A scorer reading the table is what makes this a refusal rather than a race.

    Deleting the table underneath it would not stop it — it would make it
    report loss and corruption against a table it can no longer read, so the
    run's last artifacts would be a lie about the engine.
    """
    _torn_down_run(tmp_path)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {**_purge_environment(tmp_path), "STUB_OBJECT_NAME": f"scorer-{RUN_OBJECT}"},
        programs=PURGE_PROGRAMS,
    )
    assert run.result.returncode != 0
    assert f"job/scorer-{RUN_OBJECT} is still in ingest-bench" in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
def test_purge_asks_the_catalog_when_no_document_was_copied(tmp_path: Path) -> None:
    """An absent document is not an absent table, so the catalog is asked.

    A run torn down by hand, or never torn down at all, has no copied metadata
    document and may still have a table holding every byte it wrote. Reading
    the absence as "no table" would leave exactly that table behind while
    reporting a purge that succeeded — so the catalog answers the question, and
    its answer also carries the location, which is the thing that must never be
    guessed.
    """
    _torn_down_run(tmp_path, metadata=False)
    published = tmp_path / "published"
    published.mkdir()
    (published / "00007-live.metadata.json").write_text(json.dumps({"location": TABLE_LOCATION}))

    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "0",
            "STUB_METADATA_OUT": f"{TABLE_LOCATION}/metadata/00007-live.metadata.json",
            "STUB_S3_CP_DIR": str(published),
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert f"--table {FACTS['table']}" in (tmp_path / "metadata.log").read_text()
    # Dropped and removed exactly as a purge with the document already in hand.
    assert f"--table {FACTS['table']}" in (tmp_path / "drop-table.log").read_text()
    removals = [line for line in run.aws_calls.splitlines() if line.startswith("s3 rm")]
    assert removals == [
        f"s3 rm --recursive {TABLE_LOCATION}",
        f"s3 rm --recursive s3://a-bucket/runs/{RUN_ID}/",
    ]


@needs_shell_tools
def test_purge_reclaims_the_artifacts_of_a_run_the_catalog_holds_no_table_for(tmp_path: Path) -> None:
    """A run whose table was never created still has a prefix worth reclaiming.

    Its artifacts are what a failed run leaves — the staged documents, the
    scores it got as far as — and they are paid for whether or not a table was
    ever made. The catalog saying it holds no such table is what makes removing
    them alone the whole of the purge rather than half of one.
    """
    _torn_down_run(tmp_path, metadata=False)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            # What `table-metadata` exits for a table the catalog does not hold.
            "STUB_METADATA_STATUS": "3",
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "holds no" in run.result.stdout and str(FACTS["table"]) in run.result.stdout
    # The prefix it names is the one printed under it, which is the only line
    # in that block naming something that goes.
    listed = run.result.stdout.splitlines()
    assert "only the prefix below goes" in listed[1] and f"runs/{RUN_ID}/" in listed[2]

    removals = [line for line in run.aws_calls.splitlines() if line.startswith("s3 rm")]
    assert removals == [f"s3 rm --recursive s3://a-bucket/runs/{RUN_ID}/"]
    assert (tmp_path / "drop-table.log").read_text() == "", "there is no table to drop"


@needs_shell_tools
def test_purge_claims_no_purge_when_there_is_nothing_to_remove(tmp_path: Path) -> None:
    """No table in the catalog and no `--artifacts`: nothing goes, and it says so.

    Prompting over an empty list and then logging a purge would put a success
    line behind a script that removed nothing — and this is the one script that
    deletes measured data, so its report of what it did has to be worth
    trusting. It returns before the prompt, which is why no `--yes` is needed
    here.
    """
    _torn_down_run(tmp_path, metadata=False)
    run = _run_driver(
        PURGE,
        [RUN_ID],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "3",
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "removes nothing" in run.result.stdout and "--artifacts" in run.result.stdout
    assert "purged" not in run.result.stderr, "nothing was removed, so nothing is reported as purged"
    assert "remove all of the above?" not in run.result.stdout
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
def test_purge_refuses_when_it_cannot_ask_the_catalog(tmp_path: Path) -> None:
    """ "I could not ask" and "there is no table" have to be different answers.

    A catalog this script could not reach says nothing about whether the table
    is there, and a purge that read the failure as an absence would remove a
    run's artifacts — the copied document among them — and leave its table with
    nothing left pointing at where its files are.
    """
    _torn_down_run(tmp_path, metadata=False)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "1",
            "STUB_METADATA_ERROR": "could not reach the catalog",
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode != 0
    assert "scripts/teardown.sh" in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
@pytest.mark.parametrize(
    "location",
    [
        # The bucket, which is the corpus and every run's artifacts as well.
        "s3://a-bucket",
        "s3://a-bucket/",
        # The warehouse root, which is every table this site has ever held.
        WAREHOUSE_ROOT,
        f"{WAREHOUSE_ROOT}/",
        # Another site's warehouse, and a sibling prefix whose name starts the
        # same way — neither is under this site's root.
        "s3://another-bucket/warehouse/ingest_bench/t_x",
        "s3://a-bucket/warehouse-of-someone-else/t_x",
    ],
)
def test_purge_refuses_a_location_that_is_not_one_table(tmp_path: Path, location: str) -> None:
    """`aws s3 rm --recursive` takes a prefix and asks nothing.

    A metadata document naming the bucket or the warehouse root passes every
    check about the string being present, so the prefix itself has to be
    checked against the site's warehouse: under it, and naming something below
    it. Otherwise one malformed document removes the corpus, every other run's
    artifacts, or every table the site holds.
    """
    _torn_down_run(tmp_path, location=location)
    run = _run_driver(
        PURGE, [RUN_ID, "--yes", "--artifacts"], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS
    )
    assert run.result.returncode != 0
    assert WAREHOUSE_ROOT in run.result.stderr, "the refusal names the root it was checked against"
    assert "nothing is removed" in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
@pytest.mark.parametrize("artifacts", [False, True])
def test_purge_removes_the_table_then_its_files_then_the_artifacts(tmp_path: Path, artifacts: bool) -> None:
    """The catalog entry first, then the prefixes, and the run's own only on request.

    Dropping the table before its files means nothing can load a table whose
    data is on its way out. The run's artifacts are a separate flag because
    they are the record of what was measured, and a reclaimed table does not
    make them worthless.
    """
    _torn_down_run(tmp_path)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", *(["--artifacts"] if artifacts else [])],
        tmp_path,
        _purge_environment(tmp_path),
        programs=PURGE_PROGRAMS,
    )
    assert run.result.returncode == 0, run.result.stderr

    dropped = (tmp_path / "drop-table.log").read_text()
    assert f"--table {FACTS['table']}" in dropped
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in dropped

    removals = [line for line in run.aws_calls.splitlines() if line.startswith("s3 rm")]
    expected = [f"s3 rm --recursive {TABLE_LOCATION}"]
    if artifacts:
        expected.append(f"s3 rm --recursive s3://a-bucket/runs/{RUN_ID}/")
    assert removals == expected
