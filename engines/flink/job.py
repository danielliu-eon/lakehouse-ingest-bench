"""Submit a rendered SQL script to Flink, from inside the Flink image.

This is the whole of the engine's code. The script it submits is rendered by
`knobs.py`, and everything the job then does — reading Kafka, encoding
Parquet, committing to Iceberg — is done by released connectors. Keeping this
to a submitter is what makes a result attributable to Flink.

PyFlink is installed in the image and not in this repository's environment, so
nothing in the harness imports this module.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path

import yaml
from pyflink.table import EnvironmentSettings, TableEnvironment

# The separator `render_sql` writes between statements. A property value may
# itself hold a `;` — a SASL configuration does — so the line end is what
# makes the split unambiguous.
STATEMENT_SEPARATOR = ";\n"


def statements_in(script: str) -> list[str]:
    """The script's statements, in the order they have to be submitted."""
    return [statement.strip() for statement in script.split(STATEMENT_SEPARATOR) if statement.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="job", description="Run a rendered SQL script on Flink.")
    parser.add_argument("--sql", required=True, metavar="PATH", help="the rendered script to submit")
    parser.add_argument("--conf", required=True, metavar="PATH", help="the rendered Flink settings, as YAML")
    parser.add_argument(
        "--wait",
        action="store_true",
        help="block until the job ends; without it submission returns as soon as the job is accepted",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    table_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())

    conf = yaml.safe_load(Path(str(args.conf)).read_text())
    if not isinstance(conf, dict):
        raise ValueError(f"{args.conf} must hold a mapping of Flink setting to value, got {type(conf).__name__}")
    for key, value in conf.items():
        table_env.get_config().set(str(key), str(value))

    statements = statements_in(Path(str(args.sql)).read_text())
    if not statements:
        raise ValueError(f"{args.sql} holds no statements")
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
