# 3v3nTr4cer

Python event-ingestion backend with a FastAPI API and PostgreSQL storage. The
Collector's bounded worker queue is implemented in C++17 and exposed to Python
through CPython and pybind11. Event handling and business logic remain async
Python.

## Architecture

Public event writes and Outbox records share a database transaction. The
in-process publisher delivers pending records with at-least-once semantics.
The AsyncManager queue is process-local; PostgreSQL Outbox is the durable
handoff.

`core.async_lib.async_manager.AsyncManager` preserves the Python async API:
`start(handler)`, `enqueue(item)`, `stop()`, and `shutdown_event`. Native worker
threads consume a bounded FIFO queue and dispatch handlers to the active asyncio
event loop. Queue-full producers wait asynchronously, shutdown drains queued
work, and handler exceptions are logged without stopping the pool.

Python callbacks still run under the CPython GIL, so this manager does not
parallelize CPU-bound Python handlers. Native workers coordinate queue operations
and async/I/O handlers.

The former pure-Python implementation is archived at
[`src/core/async_lib/legacy/async_manager.py`](src/core/async_lib/legacy/async_manager.py).
It is deprecated and is not a runtime fallback.

## Requirements

- Python 3.11+
- C++17 compiler and Python development headers for local builds
- PostgreSQL 14+ for local runs, or Docker Compose

## Install

```bash
git clone https://github.com/Rub3cK0r3/3v3nTr4cer.git
cd 3v3nTr4cer
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r deploy/requirements.txt
pip install -e .
```

`pip install -e .` builds `core.async_lib._async_manager` from
`src/core/async_lib/systems_impl/async_manager.cpp`. Build requirements are in
`pyproject.toml`; the extension configuration is in `setup.py`. The Collector
Dockerfile installs a compiler and builds the extension in the image.

## Run

Start the backend and PostgreSQL with:

```bash
docker compose -f deploy/compose.yml up -d --build
```

Stop without deleting the database volume:

```bash
docker compose -f deploy/compose.yml down
```

For the local demo, run `./demo.sh`. For a local backend, initialize PostgreSQL
and set the required environment variables:

```bash
createdb eventdb
psql -d eventdb -f db/init/V1_schema_dev.sql
export DATABASE_URL="postgresql://devuser:devpass@localhost:5432/eventdb"
export SECRET_KEY="replace-me"
export INTERNAL_TOKEN="replace-me"
PYTHONPATH=src uvicorn core.backend.main:app --host 0.0.0.0 --port 8000
```

## API

- `POST /v1/events` and `GET /v1/events` require a Bearer token.
- `POST /internal/pipeline/events` and `POST /internal/pipeline/alerts` require
  `X-Internal-Token`.
- `GET /health` and `GET /ready` report service and database readiness.
- Browser `fetchTrace` examples are in [`examples/`](examples).

## CI validation and migrations

The canonical project verification is the automated CI pipeline, not a video or
a graphical demo: this project is focused on communications and servers. GitHub
Actions builds the native extension and runs the test suite on pushes and pull
requests targeting `main`, `master`, or `develop`. A green pipeline is the
reproducible evidence that the checked-in suite passed.

To reproduce the same test command locally, install the extension and run:

```bash
pip install -e .
PYTHONPATH=src pytest src/tests/tests.py -q
PYTHONPATH=src alembic upgrade head
```

The pipeline definition is in [`.github/workflows/ci.yml`](.github/workflows/ci.yml).
Database migrations are a separate operation and are not run by this test job.

## Project structure

```text
src/core/async_lib/
  async_manager.py                 Python async adapter
  systems_impl/async_manager.cpp   CPython extension and native worker queue
  legacy/async_manager.py          Deprecated Python implementation
setup.py                           Native extension build definition
pyproject.toml                     Python build-system requirements
```

## License

MIT License. See [LICENSE](LICENSE).
