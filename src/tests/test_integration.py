import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from core.async_lib.async_manager import AsyncManager
from core.async_lib.processor.main import EventProcessor
from core.async_lib.alert_engine.main import AlertManager as AsyncAlertManager
class TestAsyncPipelineIntegration(unittest.TestCase):

    def _run(self, coro):
        return asyncio.run(coro)

    @patch("httpx.AsyncClient")
    def test_events_and_alerts_flow_through_pipeline(self, mock_client_cls):
        """
        Test that events and alerts flow through the pipeline correctly.
        We use a single patch on httpx.AsyncClient since both modules import it.
        """
        
        # Create a mock client that tracks all POST calls
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        
        # Setup the context manager
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_client)
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_client_cls.return_value = mock_cm

        async def scenario():
            manager = AsyncManager(worker_count=4, max_queue_size=100)
            processor = EventProcessor()
            alert_manager = AsyncAlertManager()

            await manager.start(processor.handle)

            # Enqueue events
            events = [{"id": f"evt-{i}", "app_name": "App", "type": "T", "payload": {"i": i}} for i in range(5)]
            for e in events:
                await manager.enqueue(e)

            # Process alerts
            alerts = [{"id": f"alert-{i}", "severity": "fatal", "resource": "res"} for i in range(3)]
            for a in alerts:
                await alert_manager.handle(a)

            # Allow async workers to run
            await asyncio.sleep(0.1)
            await manager.stop()

        self._run(scenario())

        # Assertions - check that both endpoints were called
        # The mock_client.post should have been called with both /internal/pipeline/alerts and /internal/pipeline/events
        alert_calls = [call for call in mock_client.post.call_args_list if '/alerts' in str(call)]
        event_calls = [call for call in mock_client.post.call_args_list if '/events' in str(call)]

        
        
        self.assertEqual(len(alert_calls), 3, f"Expected 3 alert calls, got {len(alert_calls)}")
        self.assertGreaterEqual(len(event_calls), 5, f"Expected at least 5 event calls, got {len(event_calls)}")

if __name__ == "__main__":
    unittest.main()
