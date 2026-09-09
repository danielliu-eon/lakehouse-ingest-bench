"""The engines the harness drives itself, and where each one's knobs live.

An engine is registered by module name rather than imported here so that
reading a spec needs nothing the engine brings with it: a machine that only
loads and publishes specs has no Flink package installed, and a knobs module
that grew a heavy import would otherwise make every spec unreadable there.
The import happens when a run is staged, which is the first moment the knobs
are actually needed.
"""

from __future__ import annotations

import importlib
from types import ModuleType

MANAGED: dict[str, str] = {"flink": "engines.flink.knobs"}


def knobs_for(engine: str) -> ModuleType:
    """The knobs module of a managed engine.

    Every such module exposes ``validate(block, spec, meta) -> None`` and
    ``render(spec, site, derived, meta, *, image_tag) -> dict[str, str]``, the
    second returning the files to write into the run directory keyed by
    filename. ``image_tag`` is the tag of the images a run on a cluster starts,
    and is ``None`` for a site that declares no cluster.
    """
    if engine not in MANAGED:
        raise ValueError(f"{engine!r} is not a managed engine; the registered ones are {sorted(MANAGED)}")
    name = MANAGED[engine]
    try:
        return importlib.import_module(name)
    except ImportError as error:
        raise ValueError(
            f"managed engine {engine!r} declares knobs module {name!r}, which does not import: {error}"
        ) from error
