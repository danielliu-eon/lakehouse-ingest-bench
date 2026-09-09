"""The engines the harness drives itself, and where each one's modules live.

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

MANAGED: dict[str, str] = {"flink": "engines.flink.knobs", "spark": "engines.spark.knobs"}

KNOBS_MODULE = "knobs"
FLEET_MODULE = "fleet"
VERIFY_MODULE = "verify"


def _engine_module(engine: str, submodule: str) -> ModuleType:
    """One module of a managed engine's package, imported on demand.

    The package is taken from the registered knobs module rather than from a
    second registry per accessor: two registries keyed by engine let one carry
    an engine the other has never heard of, and the failure surfaces as a
    missing fleet or a missing verdict rather than as an unregistered engine.
    """
    if engine not in MANAGED:
        raise ValueError(f"{engine!r} is not a managed engine; the registered ones are {sorted(MANAGED)}")
    package = MANAGED[engine].rsplit(".", 1)[0]
    name = f"{package}.{submodule}"
    try:
        return importlib.import_module(name)
    except ImportError as error:
        raise ValueError(
            f"managed engine {engine!r} declares module {name!r}, which does not import: {error}"
        ) from error


def knobs_for(engine: str) -> ModuleType:
    """The knobs module of a managed engine.

    Every such module exposes ``validate(block, spec, meta) -> None`` and
    ``render(spec, site, derived, meta, *, image_tag) -> dict[str, str]``, the
    second returning the files to write into the run directory keyed by
    filename. ``image_tag`` is the tag of the images a run on a cluster starts,
    and is ``None`` for a site that declares no cluster.
    """
    return _engine_module(engine, KNOBS_MODULE)


def fleet_for(engine: str) -> ModuleType:
    """The fleet module of a managed engine.

    Every such module exposes ``fleet(spec) -> list[FleetRole]``: the compute
    the run asked for, in the vCPU and GiB a published result is costed in.
    """
    return _engine_module(engine, FLEET_MODULE)


def verify_for(engine: str) -> ModuleType:
    """The verify module of a managed engine.

    Every such module exposes ``verify(spec, run_id, fetch) -> list[str]``:
    one line per setting the running engine does not honour, and an empty list
    for a run it does.
    """
    return _engine_module(engine, VERIFY_MODULE)
