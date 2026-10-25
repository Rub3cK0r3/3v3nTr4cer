import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from loguru import logger

from core.async_lib._async_manager import NativeAsyncManager


class AsyncManager:
    """Asyncio adapter for the native CPython worker queue."""

    def __init__(self, worker_count: int = 4, max_queue_size: int = 1000):
        if worker_count <= 0:
            raise ValueError("worker_count must be a positive integer.")
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")

        self.worker_count = worker_count
        self.max_queue_size = max_queue_size
        self.shutdown_event = asyncio.Event()
        self.queue: NativeAsyncManager | None = None
        self._slots: asyncio.Semaphore | None = None
        self._started = False

    async def start(self, handler: Callable[[Any], Awaitable[None]]) -> None:
        if self._started:
            raise RuntimeError("AsyncManager can only be started once")
        if not callable(handler):
            raise TypeError("handler must be callable")

        loop = asyncio.get_running_loop()
        slots = asyncio.Semaphore(self.max_queue_size)

        async def process(item: Any) -> None:
            try:
                await handler(item)
            except Exception as error:
                logger.bind(pipeline_stage="worker").exception(
                    "AsyncManager worker error: {}", error
                )

        def dispatch(item: Any) -> None:
            loop.call_soon_threadsafe(slots.release)
            asyncio.run_coroutine_threadsafe(process(item), loop).result()

        self.shutdown_event.clear()
        self._slots = slots
        self.queue = NativeAsyncManager(
            self.worker_count, self.max_queue_size, dispatch
        )
        self._started = True

    async def enqueue(self, item: Any) -> None:
        if not self._started or self.shutdown_event.is_set():
            raise RuntimeError("AsyncManager is not accepting items")

        slots = self._slots
        queue = self.queue
        if slots is None or queue is None:
            raise RuntimeError("AsyncManager has not been started")

        await slots.acquire()
        if self.shutdown_event.is_set():
            slots.release()
            raise RuntimeError("AsyncManager is not accepting items")
        try:
            queue.enqueue(item)
        except Exception:
            slots.release()
            raise

    async def stop(self) -> None:
        self.shutdown_event.set()
        if self.queue is not None:
            await asyncio.to_thread(self.queue.shutdown)
