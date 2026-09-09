"""Render a Kubernetes manifest from a template of ``__NAME__`` markers.

The templates under ``deploy/k8s/`` are applied by the run drivers, which know
the site's registry, identities and placement and nothing about YAML. So the
substitution is deliberately dumb — no conditionals, no loops, no schema — and
every decision a manifest carries is made by the driver that renders it: a Job
that wants no ``AWS_REGION`` is given an empty env list rather than a template
that knows when a region exists.

Refusing an unmatched marker and an unused variable is the whole of the
validation, and it is what makes a typo cost one error message instead of a
Job that runs under the wrong identity. Nothing here talks to a cluster;
applying and waiting belong to ``scripts/_k8s.sh``.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from ingest_bench.catalog import parse_key_values

MARKER_RE = re.compile(r"__[A-Z_]+__")


def render_template(path: Path, variables: Mapping[str, str]) -> str:
    """The template at ``path`` with every marker replaced by its value.

    The markers are read off the template rather than off the result, so a
    value that itself contains the marker syntax stays the literal text the
    caller passed: substitution is one pass, and a command line is data.
    """
    text = path.read_text(encoding="utf-8")
    markers = {match[2:-2] for match in MARKER_RE.findall(text)}
    missing = sorted(markers - set(variables))
    if missing:
        raise ValueError(f"{path} has no value for {missing}; pass one --set for each")
    unused = sorted(set(variables) - markers)
    if unused:
        raise ValueError(f"{path} holds no marker for {unused}; a value that reaches no manifest is a typo")
    return MARKER_RE.sub(lambda match: variables[match.group(0)[2:-2]], text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="render-k8s", description="Render a Kubernetes manifest template, for a driver to apply."
    )
    parser.add_argument("template", metavar="PATH", help="the template to render")
    parser.add_argument(
        "--set",
        dest="values",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="a value for the __NAME__ marker, repeatable. Every marker needs one, and every one must match a marker",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    rendered = render_template(
        Path(str(parsed.template)), parse_key_values([str(value) for value in parsed.values], "--set")
    )
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
