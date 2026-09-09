# SPDX-License-Identifier: Apache-2.0
"""What a run is asked for, where it runs, and the names that follow from both.

A run is described by two files: a spec, which is the comparable part — corpus,
partition scheme, Kafka shape, engine knobs — and a site config, which is the
part that differs between one operator's cluster and another's. Keeping them
apart is what lets the same spec be published as the thing that was measured
while the bucket names and credentials it ran against stay local.
"""
