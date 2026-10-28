"""Hypothesis profiles shared by unit and integration property tests.

Select with ``HYPOTHESIS_PROFILE`` (``dev`` default, ``ci``, ``nightly``).
Integration properties pay several catalog round-trips per example, so the
example budgets are deliberately modest outside ``nightly``.
"""

from __future__ import annotations

import os

from hypothesis import HealthCheck, settings

_SLOW = [HealthCheck.too_slow, HealthCheck.function_scoped_fixture, HealthCheck.data_too_large]

settings.register_profile("dev", max_examples=15, deadline=None, suppress_health_check=_SLOW)
settings.register_profile(
    "ci", max_examples=40, deadline=None, suppress_health_check=_SLOW, print_blob=True
)
settings.register_profile(
    "nightly", max_examples=400, deadline=None, suppress_health_check=_SLOW, print_blob=True
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "dev"))
