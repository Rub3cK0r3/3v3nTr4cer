import asyncio
from loguru import logger

_ITEM_NOT_RETRIEVED = object()


class AsyncManager:
    """Deprecated asyncio.Queue implementation, retained for reference."""

    def __init__(self, worker_count: int = 4, max_queue_size: int = 1000):
        if worker_count <= 0:
            raise ValueError("worker_count must be a positive integer.")
        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")

        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.worker_count = worker_count
        self.workers = []
        self.shutdown_event = asyncio.Event()

    async def start(self, handler):
        for _ in range(self.worker_count):
            task = asyncio.create_task(self._worker_loop(handler))
            self.workers.append(task)

    async def _worker_loop(self, handler):
        while not self.shutdown_event.is_set() or not self.queue.empty():
            item = _ITEM_NOT_RETRIEVED
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1)
                await handler(item)
            except asyncio.TimeoutError:
                continue
            except Exception as error:
                logger.bind(pipeline_stage="worker").exception(
                    "AsyncManager worker error: {}", error
                )
            finally:
                if item is not _ITEM_NOT_RETRIEVED:
                    self.queue.task_done()

    async def enqueue(self, item):
        await self.queue.put(item)

    async def stop(self):
        self.shutdown_event.set()
        await self.queue.join()
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)