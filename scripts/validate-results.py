#!/usr/bin/env python3
"""Entry point for the results/ publication checks.

The rules themselves live in `ingest_bench.collect.validate`, where they are
import-tested and type-checked with the rest of the package; this file is
what a contributor and CI actually run.
"""

from __future__ import annotations

import sys

from ingest_bench.collect.validate import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
