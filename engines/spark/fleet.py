# SPDX-License-Identifier: Apache-2.0
"""Report Spark's requested compute by role for cost calculations.

Costs use the spec's requests. verify.py checks the running fleet separately.
"""

from __future__ import annotations

from engines.spark.knobs import read
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, FleetRole, RunSpec

_MB_PER_GIB = 1024


def fleet(spec: RunSpec) -> tuple[FleetRole, ...]:
    """Return driver and executor roles, including the driver's compute cost."""
    knobs = read(spec.engine_block)
    # Use the shared missing-machine sentinel recognized by collect.validate.
    machine_type = knobs.machine_type or MACHINE_TYPE_UNSPECIFIED
    return (
        FleetRole("driver", 1, knobs.driver_cores, knobs.driver_mem_mb / _MB_PER_GIB, machine_type),
        FleetRole("executor", knobs.executors, knobs.executor_cores, knobs.executor_mem_mb / _MB_PER_GIB, machine_type),
    )
