# SPDX-License-Identifier: Apache-2.0
"""Report Flink's requested compute by role for cost calculations.

Translate the fleet knobs into vCPU-hours and GiB-hours. Costs use container
requests, regardless of how Kubernetes distributes the pods across nodes.
"""

from __future__ import annotations

from engines.flink.knobs import read
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, FleetRole, RunSpec

JOBMANAGER = "jobmanager"
TASKMANAGER = "taskmanager"

_MB_PER_GIB = 1024


def fleet(spec: RunSpec) -> list[FleetRole]:
    """Return one JobManager role and the requested TaskManager fleet."""
    knobs = read(spec.engine_block)
    machine_type = knobs.machine_type or MACHINE_TYPE_UNSPECIFIED
    return [
        FleetRole(JOBMANAGER, 1, knobs.jm_cpu, knobs.jm_mem_mb / _MB_PER_GIB, machine_type),
        FleetRole(TASKMANAGER, knobs.taskmanagers, knobs.tm_cpu, knobs.tm_mem_mb / _MB_PER_GIB, machine_type),
    ]
