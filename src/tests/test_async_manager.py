import asyncio
import unittest
from core.async_lib.async_manager import AsyncManager

class TestAsyncManager(unittest.IsolatedAsyncioTestCase):
    async def test_enqueue_and_worker_processes_items(self):
        manager = AsyncManager(worker_count=1, max_queue_size=10)
        processed = []

        async def handler(item):
            processed.append(item)

        await manager.start(handler)
        await manager.enqueue("first")
        await manager.enqueue("second")
        await asyncio.sleep(0.05)
        await manager.stop()

        self.assertEqual(processed, ["first", "second"])
        self.assertTrue(manager.queue.empty())

    async def test_stop_drains_queue_even_if_handler_raises(self):
        manager = AsyncManager(worker_count=1, max_queue_size=10)

        async def handler(item):
            raise RuntimeError("handler failed")

        await manager.start(handler)
        await manager.enqueue("bad")
        await asyncio.sleep(0.05)
        await manager.stop()

        self.assertTrue(manager.queue.empty())

    async def test_none_is_processed_and_queue_is_drained(self):
        manager = AsyncManager(worker_count=1, max_queue_size=10)
        processed = []

        async def handler(item):
            processed.append(item)

        await manager.start(handler)
        await manager.enqueue(None)
        await manager.stop()

        self.assertEqual(processed, [None])
        self.assertTrue(manager.queue.empty())

    def test_rejects_invalid_configuration(self):
        with self.assertRaises(ValueError):
            AsyncManager(worker_count=0)

        with self.assertRaises(ValueError):
            AsyncManager(max_queue_size=0)
