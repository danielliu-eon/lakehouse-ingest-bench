# SPDX-License-Identifier: Apache-2.0
"""Print how a managed engine's run is addressed on a cluster.

The cluster drivers are shell, and what they need of an engine is a handful of
names: the kind of resource a run is, where its state sits, which Service
carries its HTTP API. This is how they ask, so no driver holds a name that
belongs to one engine.

One field prints its value alone, which is what a shell assigns to a variable.
Several — or none, meaning all of them — print `field=value` a line at a time,
so a driver reads the whole descriptor in one call rather than one call per
name.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from ingest_bench.specs.engines import MANAGED, kubernetes_for
from ingest_bench.specs.kubernetes import FIELDS


def render(engine: str, requested: Sequence[str]) -> str:
    """The requested fields of ``engine``'s descriptor, as text for a shell.

    A field this does not know is refused by name: a driver that read one and
    got an empty answer would go on to address a cluster with a name nobody
    printed.
    """
    texts = kubernetes_for(engine).texts()
    # Both directions: a descriptor missing a field leaves a driver with an
    # empty name, and one printing a field no driver reads is a name nobody
    # acts on. Either way it is not this descriptor.
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
