# SPDX-License-Identifier: Apache-2.0
"""Two properties of the tree a stranger clones, checked on every tracked file.

The first is that each script and module says which licence it is offered
under. A file copied out of the repository loses its `LICENSE` neighbour, so a
per-file SPDX line is what keeps the terms attached to the code.

The second is that nothing in the tree names where the benchmark came from. It
was extracted from a private tree, and it is run by strangers against their own
accounts: a company name, an internal codename, a real bucket or a real account
id is both a leak and a step nobody else can reproduce. Every one of those has a
shape, and the shapes are cheap to check, which is what makes this a test rather
than a review habit.
"""

from __future__ import annotations

import re
import subprocess
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# The guard has to name the terms it bans, so it is the one file it cannot
# scan. That is affordable only because the file holds patterns and nothing
# else — no addresses, no ids, no prose about a private run.
SELF = Path(__file__).resolve().relative_to(REPO_ROOT).as_posix()

SPDX_LINE = "# SPDX-License-Identifier: Apache-2.0"
LICENSED_SUFFIXES = (".py", ".sh")

# Words from the private tree: the company, its domain, its products and the
# internal campaigns whose workload shapes this benchmark reproduces. `rise` is
# also an ordinary English word, so prose has to reach for "increase" instead —
# a whole-word match keeps `sunrise` and `peony` out of it, but not a sentence
# that genuinely wants the verb.
INTERNAL_VOCABULARY = ("adevents", "eon", "maelstrom", "rise")

# The account id AWS reserves for its own documentation, and so the one 12-digit
# run in the tree that is a placeholder rather than somebody's account.
PLACEHOLDER_ACCOUNT_ID = "123456789012"

# The only email address the tree may carry: the commit-trailer address, should
# one ever be quoted in a document. Trailers themselves live in commit
# metadata, not in a tracked file.
PUBLIC_EMAIL = "noreply@anthropic.com"

# Bucket names the tree is allowed to name. Three of them are generic words the
# local compose stack mounts (`corpus`, `runs`, `warehouse`); the rest are
# fixture and negative-fixture names the tests assert redaction against. A new
# root has to be added here by hand, and that is the point of the list — it
# turns "somebody pasted their own bucket into a test" into a failing test
# rather than a published bucket name.
FIXTURE_BUCKETS = frozenset(
    {
        "a-bucket",
        "another-bucket",
        "b",
        "bench",
        "bench-bucket",
        "bucket",
        "corpus",
        "leaked-bucket",
        "override",
        "runs",
        "some-bucket-123456789012",
        "w",
        "warehouse",
    }
)

# The three shapes a runbook or a rendered manifest writes where a real bucket
# belongs: a shell variable, an angle-bracketed slot, and a shouting
# placeholder. None of them is a name, so none of them can leak one.
PLACEHOLDER_ROOT = re.compile(r"\$\{?[A-Za-z_]\w*\}?|<[^>]+>|[A-Z][A-Z0-9_]*")


@dataclass(frozen=True)
class Rule:
    """One shape a leak takes, and the texts of that shape that are allowed.

    `group` picks which part of the match is compared against `allowed` — the
    bucket rule matches a whole URI but only the root is a name.
    """

    name: str
    pattern: re.Pattern[str]
    remedy: str
    group: int = 0
    allowed: frozenset[str] = frozenset()
    allowed_shape: re.Pattern[str] | None = None

    def permits(self, found: str) -> bool:
        if found in self.allowed:
            return True
        return self.allowed_shape is not None and self.allowed_shape.fullmatch(found) is not None


RULES = (
    Rule(
        name="internal vocabulary",
        # `\beon\b` also catches the `eon.io` domain, whose dot is a boundary.
        pattern=re.compile(rf"\b(?:{'|'.join(INTERNAL_VOCABULARY)})\b", re.IGNORECASE),
        remedy="say what the thing is instead of naming the company, product or campaign it came from",
    ),
    Rule(
        name="internal product name",
        pattern=re.compile(r"native[-_ ]stream", re.IGNORECASE),
        remedy="name the engine by what it does, or by the public project it is built on",
    ),
    Rule(
        name="cloud account id",
        pattern=re.compile(r"(?<![0-9A-Za-z])\d{12}(?![0-9A-Za-z])"),
        remedy=f"use {PLACEHOLDER_ACCOUNT_ID}, the id AWS reserves for documentation",
        allowed=frozenset({PLACEHOLDER_ACCOUNT_ID}),
    ),
    Rule(
        name="GCP project id",
        pattern=re.compile(r"\b[a-z][a-z0-9-]*-\d{6}\b"),
        remedy="use a <project> slot; a project id is one account's and cannot be run by anyone else",
    ),
    Rule(
        name="email address",
        pattern=re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
        remedy="drop the address; a person is not part of how the benchmark is run",
        allowed=frozenset({PUBLIC_EMAIL}),
    ),
    Rule(
        name="object-store bucket",
        # A bucket name stops at the first character that cannot be in one, so
        # the class excludes the path separator, quoting, a shell `:` and the
        # backslash that starts an escape inside a Python string literal.
        pattern=re.compile(r"(?:s3a?|gs)://(<[^>]+>|[^/\s\"'`,)\]:\\]+)"),
        remedy="use $BUCKET, <bucket> or YOUR_BUCKET, or reuse one of the fixture names",
        group=1,
        allowed=FIXTURE_BUCKETS,
        allowed_shape=PLACEHOLDER_ROOT,
    ),
)


def tracked_files() -> tuple[Path, ...]:
    """Every file a clone gets.

    `git ls-files` rather than a walk, because the things that would otherwise
    dominate the scan are exactly the things a clone does not carry: the
    operator's own `site.yaml`, the run directories under `runs/`, and the
    virtualenv.
    """
    listing = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    return tuple(REPO_ROOT / name for name in listing.split("\0") if name)


def test_the_sweep_reads_the_whole_tracked_tree() -> None:
    """A guard that reads nothing passes, so the reach of the walk is asserted too.

    The named files are one per kind the tree holds — code, shell, documents,
    recorded results, run specs, manifests, workflow, schema — so a listing
    that lost a whole category fails here rather than passing quietly. One of
    them is under `runs/`, which `.gitignore` excludes by directory and
    re-includes by extension.
    """
    names = {path.relative_to(REPO_ROOT).as_posix() for path in tracked_files()}
    assert len(names) >= 150, f"the tracked listing collapsed to {len(names)} files"
    for expected in (
        ".github/workflows/ci.yml",
        "Dockerfile",
        "README.md",
        "deploy/aws/iam/harness-policy.json",
        "deploy/k8s/harness-job.yaml.tmpl",
        "docs/examples/external-flink/job.sql",
        "docs/examples/smoke-flink/summary.json",
        "docs/results-format.md",
        "engines/flink/job.py",
        "pyproject.toml",
        "results/RESULTS.md",
        "runs/smoke-flink.yaml",
        "scripts/smoke.sh",
        "site.example.yaml",
        "workloads/schemas/events.json",
    ):
        assert expected in names, f"the walk did not reach {expected}"


def test_every_script_and_module_declares_its_licence() -> None:
    """The SPDX line comes first, or second when the first line is a shebang.

    A `#!` has to stay on line one to keep the file executable, and in Python a
    comment above the module docstring leaves the docstring the first
    statement — so both orders put the line as early as it can go.
    """
    missing: list[str] = []
    for path in tracked_files():
        if path.suffix not in LICENSED_SUFFIXES:
            continue
        lines = path.read_text().splitlines()
        head = lines[1:2] if lines[:1] and lines[0].startswith("#!") else lines[:1]
        if head != [SPDX_LINE]:
            missing.append(path.relative_to(REPO_ROOT).as_posix())
    assert not missing, f"{len(missing)} file(s) do not open with {SPDX_LINE!r}:\n" + "\n".join(sorted(missing))


def test_nothing_in_the_tree_names_where_it_came_from() -> None:
    """Every rule against every tracked file, reporting all leaks rather than the first.

    A file that is not UTF-8 is skipped: none of the six shapes is a thing a
    reader can find in a binary asset, and crashing the guard on the first
    screenshot committed to `docs/` would take the other 179 files down with it.
    """
    leaks: list[str] = []
    for path in tracked_files():
        name = path.relative_to(REPO_ROOT).as_posix()
        if name == SELF:
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        for rule in RULES:
            for match in rule.pattern.finditer(text):
                found = match.group(rule.group)
                if rule.permits(found):
                    continue
                line = text.count("\n", 0, match.start()) + 1
                leaks.append(f"{name}:{line}: {rule.name} {found!r} — {rule.remedy}")
    assert not leaks, "the tree names where it came from:\n" + "\n".join(leaks)
