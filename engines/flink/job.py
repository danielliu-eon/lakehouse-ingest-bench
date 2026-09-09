# SPDX-License-Identifier: Apache-2.0
"""Submit a rendered SQL script to Flink, from inside the Flink image.

This is the whole of the engine's code. The script it submits is rendered by
`knobs.py`, and everything the job then does — reading Kafka, encoding
Parquet, committing to Iceberg — is done by released connectors. Keeping this
to a submitter is what makes a result attributable to Flink.

PyFlink is installed in the image and not in this repository's environment, so
it is imported where it is used rather than at module scope: that keeps this
module importable — and therefore testable — without Flink.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

import yaml

from engines.flink.script import split_statements, substitute_env

__all__ = ["build_parser", "main", "split_statements", "substitute_env"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="job", description="Run a rendered SQL script on Flink.")
    parser.add_argument("--sql", required=True, metavar="PATH", help="the rendered script to submit")
    parser.add_argument("--conf", required=True, metavar="PATH", help="the rendered Flink settings, as YAML")
    parser.add_argument(
        "--wait",
        action="store_true",
        help="block until the job ends; needs an attached submission, and never returns for a streaming insert",
    )
    return parser


def read_conf(path: Path) -> dict[str, str]:
    """The settings at ``path``, every value as the string a Flink config takes."""
    loaded = yaml.safe_load(path.read_text())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must hold a mapping of Flink setting to value, got {type(loaded).__name__}")
    return {str(key): str(value) for key, value in loaded.items()}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # The rendered files name their secrets rather than holding them, so this is
    # where the environment of this container is read into them. Nothing
    # substituted here is written back to either file.
    conf = {key: substitute_env(value) for key, value in read_conf(Path(str(args.conf))).items()}
    statements = split_statements(substitute_env(Path(str(args.sql)).read_text()))
    if not statements:
        raise ValueError(f"{args.sql} holds no statements")

    from pyflink.table import EnvironmentSettings, TableEnvironment

    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    for key, value in conf.items():
        table_env.get_config().set(key, value)

    for statement in statements[:-1]:
        table_env.execute_sql(statement)
    # The last statement is the insert, and it is the only one that starts a
    # job, so it is the only one there is anything to wait for. A detached
    # submission is the default: the run is driven by the harness afterwards,
    # and a blocking submitter would hold a container open for the whole run.
    result = table_env.execute_sql(statements[-1])
    if args.wait:
        result.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
