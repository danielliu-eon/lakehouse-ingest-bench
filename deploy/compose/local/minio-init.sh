#!/bin/sh
# SPDX-License-Identifier: Apache-2.0
# The three buckets the stack's paths name: the corpus a run reads, the
# warehouse the catalog writes tables into, and the run directories a run
# publishes. Created here rather than by whichever tool touches one first,
# so a missing bucket is not reported as a permission error from inside a
# writer.
set -eu
mc alias set local http://minio:9000 admin password
# `--ignore-existing` so a second `up` is not a failure.
for b in corpus warehouse runs; do mc mb --ignore-existing "local/$b"; done
