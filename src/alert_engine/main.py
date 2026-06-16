# Compatibility wrapper for the async alert manager implementation.
#
# Tests and some legacy code expect `alert_engine.main.AlertManager`. This module
# bridges that name to the modern async implementation under
# `core.async_lib.alert_engine.main`.
from typing import Any, Dict

from core.async_lib.alert_engine.main import AlertManager as _AsyncAlertManager
from contracts.alerts import REQUIRED_ALERT_FIELDS

class AlertManager:
    # Lightweight façade delegated to the async implementation for validation
    # logic, keeping the public surface expected by existing tests.

    def __init__(self, worker_count: int = 1):
        self.worker_count = worker_count
        self._inner = None  # type: ignore[assignment]

    def _validate_alert(self, alert: Dict[str, Any]) -> bool:
        return all(field in alert for field in REQUIRED_ALERT_FIELDS)

# ---------------------------------------------------------
# © 2026 Rub3ck0r3 — Open Source (MIT License)
# Permission is granted to use, copy, modify, and distribute
# this software under the terms of the MIT License.
# See https://opensource.org/licenses/MIT for details.
# ---------------------------------------------------------
