import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from alert_engine.main import AlertManager
from core.async_lib.alert_engine.main import AlertManager as AsyncAlertManager

class TestAlertManagerValidation(unittest.TestCase):
    def test_validate_alert(self):
        am = AlertManager()
        valid_alert = {"id": "1", "severity": "fatal", "resource": "res"}
        invalid_alert = {"id": "2", "severity": "info"}  # missing resource

        self.assertTrue(am._validate_alert(valid_alert))
        self.assertFalse(am._validate_alert(invalid_alert))

class TestAsyncAlertManagerThresholds(unittest.TestCase):
    def setUp(self):
        # Ensure we start from a clean environment configuration
        self._original_min_severity = os.environ.get("ALERT_MIN_SEVERITY")
        if "ALERT_MIN_SEVERITY" in os.environ:
            del os.environ["ALERT_MIN_SEVERITY"]

    def tearDown(self):
        # Restore previous configuration
        if self._original_min_severity is not None:
            os.environ["ALERT_MIN_SEVERITY"] = self._original_min_severity
        elif "ALERT_MIN_SEVERITY" in os.environ:
            del os.environ["ALERT_MIN_SEVERITY"]

    def _run(self, coro):
        return asyncio.run(coro)

    @patch("core.async_lib.alert_engine.main.httpx.AsyncClient")
    def test_default_threshold_sends_error_and_fatal(self, mock_client_cls):
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_response = MagicMock(status_code=200)
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        mock_client_cls.return_value = mock_client

        am = AsyncAlertManager()

        error_alert = {"id": "1", "severity": "error", "resource": "res"}
        fatal_alert = {"id": "2", "severity": "fatal", "resource": "res"}
        warning_alert = {"id": "3", "severity": "warning", "resource": "res"}

        self._run(am._process_alert(error_alert))
        self._run(am._process_alert(fatal_alert))
        self._run(am._process_alert(warning_alert))

        # By default, min severity is "error" → error and fatal trigger, warning does not.
        self.assertEqual(mock_client.post.call_count, 2)

    @patch("core.async_lib.alert_engine.main.httpx.AsyncClient")
    def test_configurable_min_severity(self, mock_client_cls):
        mock_client = AsyncMock()
        mock_client.__aenter__.return_value = mock_client
        mock_client.post.return_value = AsyncMock(status_code=200)
        mock_client_cls.return_value = mock_client

        os.environ["ALERT_MIN_SEVERITY"] = "warning"

        am = AsyncAlertManager()

        info_alert = {"id": "1", "severity": "info", "resource": "res"}
        warning_alert = {"id": "2", "severity": "warning", "resource": "res"}

        self._run(am._process_alert(info_alert))
        self._run(am._process_alert(warning_alert))

        # With threshold "warning", only warning and higher should trigger
        self.assertEqual(mock_client.post.call_count, 1)

if __name__ == "__main__":
    unittest.main()
