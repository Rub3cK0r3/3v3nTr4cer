import asyncio

# Not to use a flag to indicate that an item was not retrieved from the queue, we use a unique sentinel object. 
# I think this avoids potential issues with falsy values (like None, 0, or empty strings) that could be valid items in the queue.
# It will always be a unique object that cannot be confused with any valid item that might be enqueued.
_ITEM_NOT_RETRIEVED = object()

# IMPORTANT: This is a generic asynchronous task manager that uses an internal asyncio.Queue to manage concurrent event processing.
# It is designed to be infrastructure-only and does not contain any business logic. The class provides methods for starting worker tasks, enqueuing items, and performing a graceful shutdown.
# Also it is memorable to remind that this is an example of the producer-consumer design pattern, where the AsyncManager acts as the producer of tasks and the worker tasks act as consumers that process those tasks concurrently.

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
        if worker_count <= 0:
            raise ValueError("worker_count must be a positive integer.")

        if max_queue_size <= 0:
            raise ValueError("max_queue_size must be a positive integer.")

        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.worker_count = worker_count
        self.workers = []
        # Event to signal shutdown to worker tasks, it's a normal asyncio.Event that can be awaited by workers to know when to stop processing new items.
        # We could also use a simple boolean flag, but using an asyncio.Event allows for more flexible and responsive shutdown signaling in an asynchronous context.
        # ¿Does ShutDownEvent Exist? The Respose is No, there is no built-in `ShutdownEvent` in Python's asyncio library. But for now asyncio.Event works fine for our purpose.
        # If we wanted to implement a more specialized shutdown mechanism, we could create a custom event class that extends asyncio.Event, but for most use cases, the standard asyncio.Event is sufficient.
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
            item = _ITEM_NOT_RETRIEVED
            try:
                item = await asyncio.wait_for(self.queue.get(), timeout=1)
                await handler(item)

            except asyncio.TimeoutError:
                # Allows periodic shutdown checks
                continue

            except Exception as e:
                print("AsyncManager worker error:", e)
                # The handler owns retry logic, so we just log the error and continue processing other items.
            finally:
                if item is not _ITEM_NOT_RETRIEVED:
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

# REMINDER: This AsyncManager is a generic infrastructure component. It does not contain any business logic and is designed to be reusable across different applications.
# The handler function provided to the start method should encapsulate the specific business logic for processing items from the queue. Also, this implementation is
# an example of the producer-consumer design used in asynchronous programming, where the AsyncManager acts as the producer of
# tasks and the worker tasks act as consumers that process those tasks concurrently.
