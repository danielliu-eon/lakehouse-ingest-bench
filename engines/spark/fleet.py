"""The compute a Spark run asked for, per role, for the cost column.

Requested and not observed: these are the numbers the spec chose, which is
what a published result is costed against. What the cluster actually granted
is `verify.py`'s question.
"""

from __future__ import annotations

from engines.spark.knobs import read
from ingest_bench.specs.model import FleetRole, RunSpec

_MB_PER_GIB = 1024


def fleet(spec: RunSpec) -> tuple[FleetRole, ...]:
    """The run's driver and executors as roles.

    The driver is a role of its own because it is charged for whether or not it
    does any of the writing: on a cluster it is a pod, and under
    `--master local[N]` it is the only process there is.
    """
    knobs = read(spec.engine_block)
    machine_type = "" if knobs.machine_type is None else knobs.machine_type
    return (
        FleetRole("driver", 1, knobs.driver_cores, knobs.driver_mem_mb / _MB_PER_GIB, machine_type),
        FleetRole("executor", knobs.executors, knobs.executor_cores, knobs.executor_mem_mb / _MB_PER_GIB, machine_type),
    )
