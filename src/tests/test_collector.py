import unittest
from core.async_lib.collector.main import AsyncCollector
from contracts.events import REQUIRED_EVENT_FIELDS
class TestCollectorValidation(unittest.TestCase):
    def test_validate_event_success(self):
        # db_dsn is required by the async implementation but unused for validation
        collector = AsyncCollector(db_dsn="postgresql://user:pass@localhost:5432/testdb")

        valid_event = {
            "id": "evt-123",
            "app_name": "my_service",
            "type": "test_event",
            "payload": {"foo": "bar"},
        }

        self.assertTrue(collector._validate_event(valid_event))

    def test_validate_event_failure_missing_fields(self):
        collector = AsyncCollector(db_dsn="postgresql://user:pass@localhost:5432/testdb")

        invalid_event = {
            "type": "test_event",
        }

        # Missing required fields
        self.assertFalse(collector._validate_event(invalid_event))

    def test_required_event_fields_constant(self):
        # Sanity check that the required fields are defined as expected
        self.assertIn("type", REQUIRED_EVENT_FIELDS)
        self.assertIn("payload", REQUIRED_EVENT_FIELDS)

if __name__ == "__main__":
    unittest.main()
