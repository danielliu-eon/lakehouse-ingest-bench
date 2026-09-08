"""Command line for creating and dropping a run's table.

The two commands bracket a run: one creates the table the engine writes into
and the other removes it, so a run leaves a catalog as it found it. `--ddl-only`
covers the engines that insist on creating their own table — it renders the same
schema and scheme as a statement for the engine to run, and touches no catalog.
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence

from ingest_bench.catalog import load_catalog_props, parse_key_values, table_identifier
from ingest_bench.corpus import metadata
from ingest_bench.table import ddl
from ingest_bench.table.create import create_table, drop_table, parse_partition


def add_catalog_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--table", required=True, metavar="NAMESPACE.NAME", help="the table to act on")
    parser.add_argument(
        "--catalog-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="a catalog property (uri=…, warehouse=…, token=…)",
    )
    parser.add_argument(
        "--catalog-prop-file",
        action="append",
        default=[],
        metavar="FILE",
        help="a file of catalog property KEY=VALUE lines. Preferred over --catalog-prop for a credential: "
        "on argv a token lands in every process listing and in the caller's own log",
    )


def build_create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="create-table", description="Create the Iceberg table a corpus is fed into.")
    add_catalog_arguments(parser)
    parser.add_argument(
        "--corpus",
        required=True,
        metavar="URI",
        help="the corpus the table will be fed from; its published schema is the table's column set and types",
    )
    parser.add_argument(
        "--partition",
        required=True,
        metavar="SPEC",
        help="identity(<column>), bucket(<N>, <column>) or unpartitioned. The engines read the scheme back "
        "from this table, so it is the one place a run sets it",
    )
    parser.add_argument(
        "--table-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="an Iceberg table property, applied at CREATE. Compression codec and tier, row-group sizing and "
        "dictionary switches all go here: an engine writing into a table it did not create applies none of its own",
    )
    parser.add_argument(
        "--location",
        metavar="URI",
        help="the table's location root, for a catalog that cannot derive one from its warehouse",
    )
    parser.add_argument(
        "--ddl-only",
        action="store_true",
        help="print the equivalent Spark SQL and exit, for an engine that creates its own table",
    )
    return parser


def build_drop_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="drop-table", description="Drop a run's table, or accept that it is gone.")
    add_catalog_arguments(parser)
    return parser


def create(argv: Sequence[str] | None = None) -> int:
    parser = build_create_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        # A malformed --table is an argument error rather than a catalog one, so
        # it is resolved here instead of surfacing from inside the create.
        table_identifier(table)
        partition = parse_partition(str(args.partition))
        properties = parse_key_values([str(prop) for prop in args.table_prop], "--table-prop")
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    meta = metadata.read(str(args.corpus))
    if args.ddl_only:
        print(ddl.spark_sql_ddl(meta, table, partition, properties))
        return 0
    location = None if args.location is None else str(args.location)
    created = create_table(props, table, meta, partition, properties, location=location)
    print(f"created {table} at {created.location()}")
    return 0


def drop(argv: Sequence[str] | None = None) -> int:
    parser = build_drop_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        table_identifier(table)
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    drop_table(props, table)
    print(f"dropped {table}")
    return 0
