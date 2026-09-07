# Changelog

## Unreleased

### Phase 4: PostgreSQL Outbox

- Added the `EventOutbox` model and Alembic migration.
- Added atomic public event plus Outbox persistence.
- Added `OutboxPublisher` with row locking, stale-claim recovery, delivery state, and failure metadata.
- Kept consumer idempotency keyed by `event_id`.

### Phase 3: Maintainability

- Split event, alert, and DLQ persistence into explicit backend services.
- Added typed internal pipeline contracts.

### Phase 2: Reliability

- Added event idempotency and capped exponential retry backoff.
- Added recoverable and non-recoverable processor failures.

### Phase 1: Demo readiness

- Added health/readiness endpoints and internal token authentication.
- Replaced runtime `print()` calls with structured Loguru logging.
- Documented Docker startup, event ingestion, and AI-assisted development.
