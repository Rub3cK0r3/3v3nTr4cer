# Compatibility wrapper for the async event processor implementation.
# Tests and legacy code expect `processor.main.Processor`. We expose a thin
# adapter around `core.async_lib.processor.main.EventProcessor`.
from typing import Any, Dict
import asyncpg
from core.async_lib.processor.main import EventProcessor
class Processor:
    # Synchronous-looking façade used by existing unit tests.
    #
    # It wraps the async `EventProcessor` so that validation logic can be reused
    # without forcing callers to manage an event loop.
    def __init__(self, db_pool: asyncpg.pool.Pool | None = None, worker_count: int = 1):
        self._event_processor = EventProcessor(db_pool=db_pool) if db_pool else None
        self.worker_count = worker_count

    def _validate_event(self, event: Dict[str, Any]) -> bool:
        # Delegate validation to the underlying async processor's logic.
        from contracts.events import REQUIRED_EVENT_FIELDS
        return all(field in event for field in REQUIRED_EVENT_FIELDS)

# ---------------------------------------------------------
# © 2026 Rub3ck0r3 — Open Source (MIT License)
# Permission is granted to use, copy, modify, and distribute
# this software under the terms of the MIT License.
# See https://opensource.org/licenses/MIT for details.
# ---------------------------------------------------------
