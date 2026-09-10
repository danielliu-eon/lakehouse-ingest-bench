# SPDX-License-Identifier: Apache-2.0
"""Submit rendered SQL with the PyFlink Table API.

Released connectors handle Kafka reads, Parquet encoding, and Iceberg commits.
Import PyFlink inside main so this module remains testable outside the image.
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
        help="wait for completion; requires an attached submission and waits indefinitely for a streaming insert",
    )
    return parser


def read_conf(path: Path) -> dict[str, str]:
    """Read Flink settings from ``path``, converting all values to strings."""
    loaded = yaml.safe_load(path.read_text())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must hold a mapping of Flink setting to value, got {type(loaded).__name__}")
    return {str(key): str(value) for key, value in loaded.items()}


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    # Resolve secret references in memory; keep the rendered files unchanged.
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
    # Only the final INSERT starts a job. Submit detached by default so the
    # harness can drive the run without keeping this container open.
    result = table_env.execute_sql(statements[-1])
    if args.wait:
        result.wait()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
