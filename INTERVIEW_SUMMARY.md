# Interview Summary

## What it is

`3v3nTr4cer` is a FastAPI and PostgreSQL event-observability MVP. It authenticates
clients with JWT, validates and stores application events, creates severity-based
alerts, and processes durable Outbox messages asynchronously.

## Technical achievements

- Kept FastAPI as the single public event-ingestion boundary.
- Used a PostgreSQL Outbox instead of adding a broker prematurely.
- Wrote event and Outbox records atomically in one transaction.
- Implemented `FOR UPDATE SKIP LOCKED` batch claiming and stale-claim recovery.
- Added capped exponential retries and direct DLQ routing for permanent failures.
- Preserved at-least-once delivery with event-ID idempotency.
- Kept `AsyncManager` generic and free of business/database logic.

## Failure handling

Transient processor failures retry with a capped exponential delay. Permanent HTTP
client failures go directly to the DLQ. Publisher failures are recorded on the
Outbox row, while stale processing records can be reclaimed after a crash.

## Honest limitations

The demo publisher runs inside the backend process, internal service authentication
uses a configurable shared token, and the current deployment is intended for local
demonstration. Production work would add secret management, metrics/tracing,
independent publisher scaling, and a stricter migration rollout process.

## Typical questions

**Why Outbox instead of Kafka or RabbitMQ?** PostgreSQL is already the system of
record, so the Outbox solves the dual-write problem with less operational overhead.

**What does at-least-once mean here?** A message can be delivered again after a
publisher crash, so consumers use `event_id` to avoid duplicate effects.

**Why is AsyncManager not the durable queue?** Its queue is process-local memory;
the Outbox is the durable boundary that survives restarts.

**What happens after a worker exhausts retries?** The original payload and failure
metadata are persisted in `dead_letter_events` for inspection and replay decisions.
