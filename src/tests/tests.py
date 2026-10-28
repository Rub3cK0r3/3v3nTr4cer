import asyncio
import json
import os
import random
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from alert_engine.main import AlertManager
from contracts.alerts import Alert
from contracts.events import (
    REQUIRED_EVENT_FIELDS,
    DeadLetterEvent,
    Event,
    EventCreate,
    EventOutbox,
)
from contracts.internal import DeadLetterEventIn, PipelineEventIn
from core.async_lib.alert_engine.main import AlertManager as AsyncAlertManager
from core.async_lib.async_manager import AsyncManager
from core.async_lib.collector.main import AsyncCollector
from core.async_lib.outbox.publisher import OutboxPublisher
from core.async_lib.processor.main import (
    EventProcessor,
    NonRecoverableError,
    RecoverableError,
)
from core.backend import alert_service, dlq_service, event_service, main as backend
from processor.main import Processor


NUM_EVENTS = 5000
NUM_ALERTS = 500
APP_NAMES = ["Pizzeria Frontend", "Pizzeria Backend", "Pizzeria Mobile"]
SEVERITIES = ["debug", "info", "warning", "error", "fatal"]
APP_STAGES = ["development", "testing", "staging", "production"]


def generate_event(index):
    timestamp = int(time.time() * 1000)
    return {
        "id": f"evt-{uuid4()}",
        "severity": random.choice(SEVERITIES),
        "stack": "ReferenceError: x is not defined" if random.random() < 0.1 else None,
        "type": random.choice(["ReferenceError", "TypeError", "NetworkError", "Heartbeat", "DebugInfo"]),
        "payload": {"simulation": True},
        "timestamp": timestamp,
        "received_at": timestamp,
        "resource": f"/endpoint/{index % 10}",
        "referrer": f"https://example.com/page{index % 10}",
        "app_name": random.choice(APP_NAMES),
        "app_version": f"{random.randint(1, 5)}.{random.randint(0, 10)}.{random.randint(0, 20)}",
        "app_stage": random.choice(APP_STAGES),
        "tags": {"env": "load-test"},
        "endpoint_id": str(uuid4()),
        "endpoint_language": "en-US",
        "endpoint_platform": random.choice(["MacIntel", "Linux x86_64", "iPhone", "Windows NT"]),
        "endpoint_os": random.choice(["MacOS", "Ubuntu", "iOS", "Windows"]),
        "endpoint_os_version": "1.0",
        "endpoint_runtime": "NodeJs",
        "endpoint_runtime_version": "18.15.0",
        "endpoint_country": random.choice(["US", "GB", "FR", "ES"]),
        "endpoint_user_agent": "MockAgent/1.0",
        "endpoint_device_type": random.choice(["Desktop", "Mobile"]),
    }


def generate_alert(index):
    timestamp = int(time.time() * 1000)
    return {
        "id": f"alert-{uuid4()}",
        "severity": random.choice(["error", "fatal"]),
        "resource": f"/alert/{index % 5}",
        "timestamp": timestamp,
        "received_at": timestamp,
        "app_name": random.choice(APP_NAMES),
        "endpoint_id": str(uuid4()),
    }


def make_event(event_id="event-1", severity="info"):
    return EventCreate(
        id=event_id,
        severity=severity,
        type="client.error",
        timestamp=1700000000000,
        app_name="demo",
        endpoint_id="endpoint-1",
    )


class TestCollector(unittest.TestCase):
    def test_validation_requires_every_declared_field(self):
        collector = AsyncCollector("postgresql://user:pass@localhost/testdb")
        valid_event = {
            "id": "evt-1",
            "app_name": "service",
            "type": "test",
            "payload": {},
        }

        self.assertTrue(collector._validate_event(valid_event))
        for field in REQUIRED_EVENT_FIELDS:
            with self.subTest(missing_field=field):
                self.assertFalse(collector._validate_event(
                    {key: value for key, value in valid_event.items() if key != field}
                ))

    def test_required_event_fields_are_stable(self):
        self.assertEqual(REQUIRED_EVENT_FIELDS, ["id", "app_name", "type", "payload"])


class TestAsyncCollector(unittest.IsolatedAsyncioTestCase):
    async def test_insert_event_posts_payload_and_internal_token(self):
        with patch.dict(os.environ, {"INTERNAL_TOKEN": "collector-secret"}):
            with patch("core.async_lib.collector.main.httpx.AsyncClient") as client_class:
                client = AsyncMock()
                client.__aenter__.return_value = client
                response = MagicMock()
                client.post.return_value = response
                client_class.return_value = client

                collector = AsyncCollector("postgresql://user:pass@localhost/testdb")
                event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {"x": 1}}
                await collector._insert_event_api(event)

        client.post.assert_awaited_once_with(
            "/internal/pipeline/events",
            json={"type": "t", "payload": event},
            headers={"X-Internal-Token": "collector-secret"},
        )

    async def test_insert_event_contains_http_failure(self):
        with patch("core.async_lib.collector.main.httpx.AsyncClient") as client_class:
            client = AsyncMock()
            client.__aenter__.return_value = client
            client.post.side_effect = httpx.ConnectError("backend unavailable")
            client_class.return_value = client

            collector = AsyncCollector("postgresql://user:pass@localhost/testdb")
            await collector._insert_event_api({"id": "evt-1", "type": "t"})

        client.post.assert_awaited_once()

    async def test_notify_fastapi_sends_to_each_connection(self):
        class DummyWebSocket:
            def __init__(self):
                self.sent = []

            async def send_json(self, payload):
                self.sent.append(payload)

        app = MagicMock()
        app.active_connections = [DummyWebSocket(), DummyWebSocket()]
        collector = AsyncCollector(
            "postgresql://user:pass@localhost/testdb", fastapi_app=app
        )
        event = {"id": "evt-1", "type": "t"}

        await collector._notify_fastapi(event)

        self.assertEqual([socket.sent for socket in app.active_connections], [[event], [event]])

    async def test_notify_fastapi_continues_after_connection_failure(self):
        broken_socket = MagicMock()
        broken_socket.send_json = AsyncMock(side_effect=RuntimeError("disconnected"))
        healthy_socket = MagicMock()
        healthy_socket.send_json = AsyncMock()
        app = SimpleNamespace(active_connections=[broken_socket, healthy_socket])
        collector = AsyncCollector(
            "postgresql://user:pass@localhost/testdb", fastapi_app=app
        )
        event = {"id": "evt-1", "type": "t"}

        await collector._notify_fastapi(event)

        healthy_socket.send_json.assert_awaited_once_with(event)

    async def test_notify_callback_ignores_invalid_json_and_events(self):
        collector = AsyncCollector("postgresql://user:pass@localhost/testdb")
        collector._validate_event = lambda event: False

        with patch("core.async_lib.collector.main.asyncio.create_task") as create_task:
            collector._notify_callback(None, None, None, "not-json")
            collector._notify_callback(None, None, None, json.dumps({"id": "evt-1"}))

        create_task.assert_not_called()

    async def test_notify_callback_enqueues_valid_event(self):
        collector = AsyncCollector("postgresql://user:pass@localhost/testdb")
        collector.async_manager.enqueue = AsyncMock()
        event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {}}

        collector._notify_callback(None, None, "events_channel", json.dumps(event))
        await asyncio.sleep(0)

        collector.async_manager.enqueue.assert_awaited_once_with(event)


class TestProcessor(unittest.TestCase):
    def test_compatibility_processor_validates_required_fields(self):
        processor = Processor()
        valid_event = {"id": "1", "app_name": "App", "type": "T", "payload": {}}
        self.assertTrue(processor._validate_event(valid_event))
        for field in REQUIRED_EVENT_FIELDS:
            with self.subTest(missing_field=field):
                invalid_event = {key: value for key, value in valid_event.items() if key != field}
                self.assertFalse(processor._validate_event(invalid_event))


class TestEventProcessor(unittest.IsolatedAsyncioTestCase):
    async def test_invalid_event_is_not_sent(self):
        processor = EventProcessor()
        processor._insert_event = AsyncMock()

        await processor.handle({"id": "evt-1"})

        processor._insert_event.assert_not_awaited()

    async def test_valid_event_is_sent_once(self):
        processor = EventProcessor()
        processor._insert_event = AsyncMock()
        event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {}}

        await processor.handle(event)

        processor._insert_event.assert_awaited_once_with(event)

    async def test_recoverable_failure_retries_then_succeeds(self):
        processor = EventProcessor(max_retries=3, retry_backoff_seconds=0.25)
        processor._insert_event = AsyncMock(side_effect=[RecoverableError("temporary"), None])
        processor._persist_dead_letter = AsyncMock()
        event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {}}

        with patch("core.async_lib.processor.main.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await processor.handle(event)

        self.assertEqual(processor._insert_event.await_count, 2)
        sleep.assert_awaited_once_with(0.25)
        processor._persist_dead_letter.assert_not_awaited()

    async def test_retries_use_capped_exponential_backoff(self):
        processor = EventProcessor(max_retries=4, retry_backoff_seconds=2)
        processor._insert_event = AsyncMock(side_effect=[RecoverableError("temporary")] * 4)
        processor._persist_dead_letter = AsyncMock()
        event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {}}

        with patch("core.async_lib.processor.main.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await processor.handle(event)

        self.assertEqual([call.args[0] for call in sleep.await_args_list], [2, 4, 8])
        processor._persist_dead_letter.assert_awaited_once_with(
            event, retries=4, error="temporary"
        )

    async def test_backoff_is_capped_at_one_minute(self):
        processor = EventProcessor(max_retries=3, retry_backoff_seconds=50)
        processor._insert_event = AsyncMock(side_effect=RecoverableError("temporary"))
        processor._persist_dead_letter = AsyncMock()

        with patch("core.async_lib.processor.main.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await processor.handle(
                {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {}}
            )

        self.assertEqual([call.args[0] for call in sleep.await_args_list], [50, 60.0])

    async def test_nonrecoverable_failure_goes_directly_to_dead_letter(self):
        processor = EventProcessor(max_retries=5)
        processor._insert_event = AsyncMock(side_effect=NonRecoverableError("invalid"))
        processor._persist_dead_letter = AsyncMock()
        event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {}}

        with patch("core.async_lib.processor.main.asyncio.sleep", new_callable=AsyncMock) as sleep:
            await processor.handle(event)

        processor._insert_event.assert_awaited_once_with(event)
        processor._persist_dead_letter.assert_awaited_once_with(
            event, retries=1, error="invalid"
        )
        sleep.assert_not_awaited()

    async def test_retry_count_is_at_least_one(self):
        self.assertEqual(EventProcessor(max_retries=0).max_retries, 1)
        self.assertEqual(EventProcessor(max_retries=-2).max_retries, 1)

    async def test_insert_event_sends_internal_header_and_payload(self):
        with patch.dict(os.environ, {"INTERNAL_TOKEN": "processor-secret"}):
            with patch("core.async_lib.processor.main.httpx.AsyncClient") as client_class:
                client = AsyncMock()
                client.__aenter__.return_value = client
                client.post.return_value = MagicMock()
                client_class.return_value = client
                processor = EventProcessor(backend_base_url="http://backend")
                event = {"id": "evt-1", "app_name": "svc", "type": "t", "payload": {}}

                await processor._insert_event(event)

        args, kwargs = client.post.call_args
        self.assertEqual(args[0], "/internal/pipeline/events")
        self.assertEqual(kwargs["json"]["payload"], event)
        self.assertEqual(kwargs["headers"], {"X-Internal-Token": "processor-secret"})


class TestAlertManager(unittest.TestCase):
    def test_compatibility_manager_validates_required_fields(self):
        manager = AlertManager()
        self.assertTrue(manager._validate_alert({"id": "1", "severity": "fatal", "resource": "/"}))
        self.assertFalse(manager._validate_alert({"id": "2", "severity": "info"}))


class TestAsyncAlertManager(unittest.IsolatedAsyncioTestCase):
    async def test_severity_threshold_matrix(self):
        with patch.dict(os.environ, {"ALERT_MIN_SEVERITY": "warning"}):
            with patch("core.async_lib.alert_engine.main.httpx.AsyncClient") as client_class:
                client = AsyncMock()
                client.__aenter__.return_value = client
                client.post.return_value = MagicMock()
                client_class.return_value = client
                manager = AsyncAlertManager()

                for severity in ("debug", "info", "warning", "error", "fatal", "unknown"):
                    with self.subTest(severity=severity):
                        await manager.handle({
                            "id": severity,
                            "severity": severity,
                            "resource": "/resource",
                        })

        self.assertEqual(client.post.await_count, 3)
        sent_severities = [call.kwargs["json"]["severity"] for call in client.post.await_args_list]
        self.assertEqual(sent_severities, ["warning", "error", "fatal"])

    async def test_default_threshold_is_error(self):
        with patch.dict(os.environ):
            os.environ.pop("ALERT_MIN_SEVERITY", None)
            with patch("core.async_lib.alert_engine.main.httpx.AsyncClient") as client_class:
                client = AsyncMock()
                client.__aenter__.return_value = client
                client.post.return_value = MagicMock()
                client_class.return_value = client
                manager = AsyncAlertManager()

                for severity in ("warning", "error", "fatal"):
                    await manager.handle({"id": severity, "severity": severity, "resource": "/"})

        self.assertEqual(client.post.await_count, 2)

    async def test_invalid_alert_is_not_sent(self):
        with patch("core.async_lib.alert_engine.main.httpx.AsyncClient") as client_class:
            manager = AsyncAlertManager()
            await manager.handle({"id": "missing-fields"})

        client_class.assert_not_called()

    async def test_alert_request_contains_internal_token(self):
        with patch.dict(os.environ, {"INTERNAL_TOKEN": "alerts-secret", "ALERT_MIN_SEVERITY": "error"}):
            with patch("core.async_lib.alert_engine.main.httpx.AsyncClient") as client_class:
                client = AsyncMock()
                client.__aenter__.return_value = client
                client.post.return_value = MagicMock()
                client_class.return_value = client
                manager = AsyncAlertManager()
                alert = {"id": "alert-1", "severity": "fatal", "resource": "/api"}

                await manager.handle(alert)

        args, kwargs = client.post.call_args
        self.assertEqual(args[0], "/internal/pipeline/alerts")
        self.assertEqual(kwargs["json"]["payload"], alert)
        self.assertEqual(kwargs["headers"], {"X-Internal-Token": "alerts-secret"})


class TestAsyncManager(unittest.IsolatedAsyncioTestCase):
    async def test_enqueue_and_worker_processes_items(self):
        manager = AsyncManager(worker_count=1, max_queue_size=10)
        processed = []
        two_items_processed = asyncio.Event()

        async def handler(item):
            processed.append(item)
            if len(processed) == 2:
                two_items_processed.set()

        await manager.start(handler)
        await manager.enqueue("first")
        await manager.enqueue("second")
        await asyncio.wait_for(two_items_processed.wait(), timeout=2)
        await manager.stop()

        self.assertEqual(processed, ["first", "second"])
        self.assertTrue(manager.queue.empty())

    async def test_stop_drains_queue_even_if_handler_raises(self):
        manager = AsyncManager(worker_count=1, max_queue_size=10)
        handler_finished = asyncio.Event()

        async def handler(item):
            handler_finished.set()
            raise RuntimeError("handler failed")

        await manager.start(handler)
        await manager.enqueue("bad")
        await asyncio.wait_for(handler_finished.wait(), timeout=2)
        await manager.stop()

        self.assertTrue(manager.queue.empty())

    async def test_none_is_processed_and_queue_is_drained(self):
        manager = AsyncManager(worker_count=1, max_queue_size=10)
        processed = asyncio.Event()
        values = []

        async def handler(item):
            values.append(item)
            processed.set()

        await manager.start(handler)
        await manager.enqueue(None)
        await asyncio.wait_for(processed.wait(), timeout=2)
        await manager.stop()

        self.assertEqual(values, [None])
        self.assertTrue(manager.queue.empty())

    async def test_enqueue_waits_when_native_queue_is_full(self):
        manager = AsyncManager(worker_count=1, max_queue_size=1)
        handler_started = asyncio.Event()
        release_handler = asyncio.Event()
        processed = []

        async def handler(item):
            processed.append(item)
            if item == "first":
                handler_started.set()
                await release_handler.wait()

        await manager.start(handler)
        await manager.enqueue("first")
        await asyncio.wait_for(handler_started.wait(), timeout=2)
        await manager.enqueue("second")
        blocked_enqueue = asyncio.create_task(manager.enqueue("third"))
        await asyncio.sleep(0)
        self.assertFalse(blocked_enqueue.done())

        release_handler.set()
        await asyncio.wait_for(blocked_enqueue, timeout=2)
        await manager.stop()

        self.assertEqual(processed, ["first", "second", "third"])

    async def test_rejects_enqueue_before_start_and_after_stop(self):
        manager = AsyncManager()
        with self.assertRaises(RuntimeError):
            await manager.enqueue("early")

        await manager.start(AsyncMock())
        await manager.stop()
        with self.assertRaises(RuntimeError):
            await manager.enqueue("late")

    async def test_start_rejects_invalid_handler_and_second_start(self):
        manager = AsyncManager()
        with self.assertRaises(TypeError):
            await manager.start(None)

        await manager.start(AsyncMock())
        with self.assertRaises(RuntimeError):
            await manager.start(AsyncMock())
        await manager.stop()

    async def test_rejects_invalid_configuration(self):
        with self.assertRaises(ValueError):
            AsyncManager(worker_count=0)
        with self.assertRaises(ValueError):
            AsyncManager(max_queue_size=0)


class TestLoadSimulation(unittest.IsolatedAsyncioTestCase):
    async def test_native_queue_processes_all_generated_events_and_alerts(self):
        manager = AsyncManager(worker_count=4, max_queue_size=128)
        processed_ids = []

        async def handler(item):
            processed_ids.append(item["id"])

        await manager.start(handler)
        expected_ids = set()
        for index in range(NUM_EVENTS):
            event = generate_event(index)
            expected_ids.add(event["id"])
            await manager.enqueue(event)
        for index in range(NUM_ALERTS):
            alert = generate_alert(index)
            expected_ids.add(alert["id"])
            await manager.enqueue(alert)
        await asyncio.wait_for(manager.stop(), timeout=20)

        self.assertEqual(len(processed_ids), NUM_EVENTS + NUM_ALERTS)
        self.assertEqual(set(processed_ids), expected_ids)
        self.assertTrue(manager.queue.empty())


class TestOutbox(unittest.TestCase):
    def test_create_event_adds_event_and_outbox_together(self):
        db = MagicMock()
        db.get.return_value = None

        event = event_service.create_event(db, make_event("event-atomic-1"))

        self.assertIsInstance(event, Event)
        added_objects = [call.args[0] for call in db.add.call_args_list]
        self.assertTrue(any(isinstance(item, Event) for item in added_objects))
        outbox_items = [item for item in added_objects if isinstance(item, EventOutbox)]
        self.assertEqual(len(outbox_items), 1)
        self.assertEqual(outbox_items[0].event_id, "event-atomic-1")
        self.assertEqual(outbox_items[0].status, "pending")
        self.assertEqual(outbox_items[0].payload["payload"]["id"], "event-atomic-1")
        db.commit.assert_called_once()

    def test_duplicate_event_does_not_create_another_outbox_item(self):
        existing = MagicMock(spec=Event)
        db = MagicMock()
        db.get.return_value = existing

        result = event_service.create_event(db, make_event("duplicate"))

        self.assertIs(result, existing)
        db.add.assert_not_called()
        db.commit.assert_not_called()

    def test_alert_severity_event_creates_alert_in_same_transaction(self):
        db = MagicMock()
        db.get.return_value = None

        event_service.create_event(db, make_event("fatal-event", severity="fatal"))

        added_objects = [call.args[0] for call in db.add.call_args_list]
        self.assertEqual(sum(isinstance(item, Alert) for item in added_objects), 1)
        self.assertEqual(sum(isinstance(item, EventOutbox) for item in added_objects), 1)

    def test_commit_race_returns_existing_event_after_rollback(self):
        existing = MagicMock(spec=Event)
        db = MagicMock()
        db.get.side_effect = [None, existing]
        db.commit.side_effect = IntegrityError("insert", {}, RuntimeError("duplicate"))

        result = event_service.create_event(db, make_event("race"))

        self.assertIs(result, existing)
        db.rollback.assert_called_once()

    def test_duplicate_alert_and_dead_letter_are_idempotent(self):
        alert = MagicMock(spec=Alert)
        dead_letter = MagicMock(spec=DeadLetterEvent)
        alert_db = MagicMock()
        alert_db.get.return_value = alert
        dead_letter_db = MagicMock()
        dead_letter_db.get.return_value = dead_letter

        self.assertIs(alert_service.create_alert(alert_db, alert), alert)
        self.assertIs(dlq_service.create_dead_letter(dead_letter_db, dead_letter), dead_letter)
        alert_db.add.assert_not_called()
        dead_letter_db.add.assert_not_called()


class TestOutboxPublisher(unittest.IsolatedAsyncioTestCase):
    def _session_context(self, session):
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.return_value = None
        return context

    async def test_publish_marks_message_published(self):
        row = SimpleNamespace(
            id="outbox-1", event_id="event-1", payload={"id": "event-1"}, attempts=1,
            status="processing", published_at=None, last_error="old",
        )
        session = MagicMock()
        session.get.return_value = row
        publisher = OutboxPublisher(AsyncMock(), poll_interval=0.01)
        publisher.claim_pending = MagicMock(return_value=["outbox-1"])

        with patch("core.async_lib.outbox.publisher.Session", return_value=self._session_context(session)):
            self.assertEqual(await publisher.publish_batch(), 1)

        publisher.handler.assert_awaited_once_with({"id": "event-1"})
        self.assertEqual(row.status, "published")
        self.assertIsNotNone(row.published_at)
        self.assertIsNone(row.last_error)
        session.commit.assert_called_once()

    async def test_publish_marks_message_failed_and_keeps_error(self):
        row = SimpleNamespace(
            id="outbox-2", event_id="event-2", payload={"id": "event-2"}, attempts=2,
            status="processing", published_at=None, last_error=None,
        )
        session = MagicMock()
        session.get.return_value = row
        publisher = OutboxPublisher(
            AsyncMock(side_effect=RuntimeError("consumer unavailable")), poll_interval=0.01
        )
        publisher.claim_pending = MagicMock(return_value=["outbox-2"])

        with patch("core.async_lib.outbox.publisher.Session", return_value=self._session_context(session)):
            self.assertEqual(await publisher.publish_batch(), 0)

        self.assertEqual(row.status, "failed")
        self.assertEqual(row.last_error, "consumer unavailable")
        session.commit.assert_called_once()

    async def test_missing_claimed_row_is_skipped(self):
        session = MagicMock()
        session.get.return_value = None
        publisher = OutboxPublisher(AsyncMock())
        publisher.claim_pending = MagicMock(return_value=["deleted-row"])

        with patch("core.async_lib.outbox.publisher.Session", return_value=self._session_context(session)):
            self.assertEqual(await publisher.publish_batch(), 0)

        publisher.handler.assert_not_awaited()

    async def test_rejects_nonpositive_settings(self):
        for options in (
            {"batch_size": 0},
            {"poll_interval": 0},
            {"stale_after_seconds": 0},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                OutboxPublisher(AsyncMock(), **options)


class TestBackend(unittest.TestCase):
    def test_internal_auth_rejects_missing_or_wrong_token(self):
        with patch.object(backend, "INTERNAL_TOKEN", "expected"):
            with self.assertRaises(HTTPException) as missing:
                backend.require_internal_token(None)
            with self.assertRaises(HTTPException) as wrong:
                backend.require_internal_token("incorrect")

        self.assertEqual(missing.exception.status_code, 401)
        self.assertEqual(wrong.exception.status_code, 401)

    def test_internal_auth_accepts_configured_token(self):
        with patch.object(backend, "INTERNAL_TOKEN", "expected"):
            self.assertIsNone(backend.require_internal_token("expected"))

    def test_empty_internal_token_rejects_all_requests(self):
        with patch.object(backend, "INTERNAL_TOKEN", ""):
            with self.assertRaises(HTTPException):
                backend.require_internal_token("")

    def test_health_does_not_depend_on_database(self):
        self.assertEqual(backend.health(), {"status": "ok"})

    def test_readiness_reports_database_failure_as_service_unavailable(self):
        with patch.object(backend.engine, "connect", side_effect=RuntimeError("offline")):
            with self.assertRaises(HTTPException) as error:
                backend.ready()

        self.assertEqual(error.exception.status_code, 503)

    def test_readiness_succeeds_when_database_is_reachable(self):
        connection = MagicMock()
        context = MagicMock()
        context.__enter__.return_value = connection
        context.__exit__.return_value = None
        with patch.object(backend.engine, "connect", return_value=context):
            self.assertEqual(backend.ready(), {"status": "ok"})
        connection.execute.assert_called_once()

    def test_internal_event_ingestion_is_idempotent(self):
        db = MagicMock()
        db.get.return_value = None
        payload = PipelineEventIn(
            id="api-event-1",
            type="client.error",
            payload={"id": "api-event-1", "app_name": "demo"},
        )

        event = backend.ingest_pipeline_event(payload, db)

        self.assertEqual(event.id, "api-event-1")
        self.assertEqual(event.app_name, "demo")
        self.assertEqual(event.endpoint_id, "unknown-endpoint")
        db.add.assert_called_once_with(event)
        db.commit.assert_called_once()
        db.refresh.assert_called_once_with(event)

        db.get.return_value = event
        duplicate = backend.ingest_pipeline_event(payload, db)
        self.assertIs(duplicate, event)
        db.add.assert_called_once()

    def test_dead_letter_ingestion_preserves_failure_context(self):
        db = MagicMock()
        db.get.return_value = None
        payload = DeadLetterEventIn(
            id="failed-1",
            type="client.error",
            payload={"id": "original-1", "app_name": "demo"},
            retries=3,
            last_error="backend timeout",
        )

        with patch("core.backend.main.create_dead_letter", side_effect=lambda session, item: item):
            item = backend.ingest_dead_letter_event(payload, db)

        self.assertEqual(item.original_event_id, "original-1")
        self.assertEqual(item.retries, 3)
        self.assertEqual(item.last_error, "backend timeout")


class TestPipelineIntegration(unittest.TestCase):
    def test_events_and_alerts_flow_through_async_pipeline(self):
        with patch("httpx.AsyncClient") as client_class:
            client = AsyncMock()
            response = MagicMock()
            client.post = AsyncMock(return_value=response)
            context = MagicMock()
            context.__aenter__ = AsyncMock(return_value=client)
            context.__aexit__ = AsyncMock(return_value=None)
            client_class.return_value = context

            async def scenario():
                manager = AsyncManager(worker_count=2, max_queue_size=10)
                processor = EventProcessor(retry_backoff_seconds=0)
                alert_manager = AsyncAlertManager()
                await manager.start(processor.handle)

                events = [
                    {"id": f"evt-{index}", "app_name": "App", "type": "T", "payload": {}}
                    for index in range(5)
                ]
                for event in events:
                    await manager.enqueue(event)

                for index in range(3):
                    await alert_manager.handle({
                        "id": f"alert-{index}",
                        "severity": "fatal",
                        "resource": "/resource",
                    })

                await manager.stop()

            asyncio.run(scenario())

        paths = [call.args[0] for call in client.post.await_args_list]
        self.assertEqual(paths.count("/internal/pipeline/events"), 5)
        self.assertEqual(paths.count("/internal/pipeline/alerts"), 3)
        self.assertEqual(len(paths), 8)


if __name__ == "__main__":
    unittest.main()