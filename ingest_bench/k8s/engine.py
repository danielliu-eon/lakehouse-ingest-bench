# SPDX-License-Identifier: Apache-2.0
"""Print managed-engine Kubernetes descriptors for shell drivers.

One requested field prints its value. Multiple fields, or no selection,
print one ``field=value`` line per field.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from ingest_bench.specs.engines import MANAGED, kubernetes_for
from ingest_bench.specs.kubernetes import FIELDS


def render(engine: str, requested: Sequence[str]) -> str:
    """Render requested descriptor fields, rejecting unknown names."""
    texts = kubernetes_for(engine).texts()
    # Reject missing and unexpected descriptor fields.
    if set(texts) != set(FIELDS):
        raise ValueError(f"{engine}'s descriptor prints {sorted(texts)}, and its fields are {list(FIELDS)}")
    unknown = [field for field in requested if field not in texts]
    if unknown:
        raise ValueError(f"no such field of a managed engine's cluster shape: {unknown}; they are {list(FIELDS)}")
    if len(requested) == 1:
        return texts[requested[0]] + "\n"
    fields = list(requested) if requested else list(FIELDS)
    return "".join(f"{field}={texts[field]}\n" for field in fields)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="engine-k8s",
        description="Print how a managed engine's run is addressed on a cluster, for a driver to read.",
    )
    parser.add_argument("engine", metavar="ENGINE", help=f"a managed engine: {', '.join(sorted(MANAGED))}")
    parser.add_argument(
        "fields",
        metavar="FIELD",
        nargs="*",
        help=(
            "a field of the descriptor. One prints its value alone; several, or none, print `field=value` per line. "
            f"The fields are: {', '.join(FIELDS)}"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    try:
        sys.stdout.write(render(str(parsed.engine), [str(field) for field in parsed.fields]))
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
