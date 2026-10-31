"""Shared test setup.

Every Spark test module runs the full sandbox pipeline in the same local driver JVM (one py4j
gateway per pytest process, whichever module starts it). With Spark's default 1 GB driver heap the
whole suite runs out of old-generation space part-way through and the JVM spends its time in full GC,
which looks like a hang. The gateway reads PYSPARK_SUBMIT_ARGS when it launches, so setting it here,
before any module builds a session, applies to every module regardless of order. An existing value
(CI or a developer's shell) is kept. Override the size with COCKPIT_TEST_DRIVER_MEMORY.
"""

from __future__ import annotations

import os

DRIVER_MEMORY = os.environ.get("COCKPIT_TEST_DRIVER_MEMORY", "3g")
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", f"--driver-memory {DRIVER_MEMORY} pyspark-shell")
