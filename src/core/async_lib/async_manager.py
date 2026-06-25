import asyncio
class AsyncManager:
    # Generic asynchronous task manager responsible for handling concurrent
    # event processing using an internal asyncio.Queue.
    # Responsibilities:
    #     - Manage an asyncio queue with optional max size.
    #     - Spawn and manage worker tasks.
    #     - Delegate item processing to an injected async handler.
    #     - Handle graceful shutdown and queue draining.
    # This class is infrastructure-only and contains no business logic.
    # Attributes:
    #     queue (asyncio.Queue): Internal queue for event buffering.
    #     worker_count (int): Number of concurrent worker tasks.
    #     workers (list[asyncio.Task]): Active worker tasks.
    #     shutdown_event (asyncio.Event): Signal used for graceful shutdown.
    #
    def __init__(self, worker_count: int = 4, max_queue_size: int = 1000):
        # Initializes the AsyncManager.
        # Args:
        #     worker_count (int): Number of concurrent worker tasks.
        #     max_queue_size (int): Maximum size of the internal queue.
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.worker_count = worker_count
        self.workers = []
        self.shutdown_event = asyncio.Event()

    async def start(self, handler):
        # Starts worker tasks using the provided async handler.
        # Args:
        #     handler (Callable[[Any], Awaitable[None]]):
        #         Async function responsible for processing each queued item.
        for _ in range(self.worker_count):
            task = asyncio.create_task(self._worker_loop(handler))
            self.workers.append(task)

    async def _worker_loop(self, handler):
        # Internal worker loop.
        # Continuously retrieves items from the queue and passes them
        # to the provided handler coroutine until shutdown is triggered
        # and the queue is fully drained.
        while not self.shutdown_event.is_set() or not self.queue.empty():
            item = None
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1)
                await handler(item)

            except asyncio.TimeoutError:
                # Allows periodic shutdown checks
                continue

            except Exception as e:
                print("AsyncManager worker error:", e)

            finally:
                if item is not None:
                    self.queue.task_done()

    async def enqueue(self, item):
        # Enqueues an item into the internal queue.
        # Args:
        #     item (Any): Object to enqueue.
        await self.queue.put(item)

    async def stop(self):
        # Performs a graceful shutdown:
        #     - Signals workers to stop accepting new work.
        #     - Waits for the queue to drain.
        #     - Cancels worker tasks.
        self.shutdown_event.set()

        # Wait for all queued tasks to complete
        await self.queue.join()

        # Cancel workers
        for worker in self.workers:
            worker.cancel()

        await asyncio.gather(*self.workers, return_exceptions=True)

