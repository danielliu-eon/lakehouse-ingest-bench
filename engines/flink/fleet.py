# SPDX-License-Identifier: Apache-2.0
"""The compute a managed Flink run is given, as the cost column reads it.

The knobs size the fleet in the units Flink asks for — a jobmanager, some
number of taskmanagers, CPU and memory each — and a result is costed in vCPU
hours and GiB hours. This is the one translation between the two, kept beside
the knobs it reads so that adding a knob that changes the fleet's shape cannot
leave the cost describing the previous shape.

The container requests are what is reported, not the nodes they landed on: a
run is charged for the compute it asked for, which is the same number whether
the cluster packed it onto two machines or twenty.
"""

from __future__ import annotations

from engines.flink.knobs import read
from ingest_bench.specs.model import FleetRole, RunSpec

JOBMANAGER = "jobmanager"
TASKMANAGER = "taskmanager"

# What a run whose spec named no machine type reports. A published result has to
# disclose the machine, and an empty string in that column would read as a
# disclosure rather than as its absence.
UNSPECIFIED = "unspecified"

_MB_PER_GIB = 1024


def fleet(spec: RunSpec) -> list[FleetRole]:
    """The run's roles: one jobmanager, and the taskmanagers the knobs asked for."""
    knobs = read(spec.engine_block)
    machine_type = knobs.machine_type or UNSPECIFIED
    return [
        FleetRole(JOBMANAGER, 1, knobs.jm_cpu, knobs.jm_mem_mb / _MB_PER_GIB, machine_type),
        FleetRole(TASKMANAGER, knobs.taskmanagers, knobs.tm_cpu, knobs.tm_mem_mb / _MB_PER_GIB, machine_type),
    ]
