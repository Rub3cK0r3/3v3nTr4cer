# 3v3nTr4cer

![Release](https://img.shields.io/github/v/release/Rub3cK0r3/3v3nTr4cer)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-in%20development-orange)

> Modular event-observability SaaS foundation for backend event ingestion, processing, persistence and alerting.

`3v3nTr4cer` is a **backend-first, modular SaaS foundation** for handling application event pipelines in Python. Its capabilities are intentionally separated into independently structured modules so they can be deployed together as one platform or evolved into standalone products: event ingestion, asynchronous processing, durable storage, alerting and failure handling.

The current version is an MVP. The backend is the primary ingestion boundary and persists events in PostgreSQL. The collector, processor and alert engine provide separate processing capabilities, while the current in-memory queue and PostgreSQL-backed dead-letter table leave a clear path toward a durable broker such as RabbitMQ or Kafka.

## 📋 Table of Contents

- [Features](#-features)
- [Requirements](#-requirements)
- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Usage Examples](#-usage-examples)
- [Project Structure](#-project-structure)
- [Configuration](#-configuration)
- [Contributing](#-contributing)
- [Security](#-security)
- [License](#-license)

## ✨ Features

- **Modular SaaS architecture** – Use the complete platform or evolve capabilities independently
- **Backend ingestion boundary** – FastAPI validates and persists application events
- **Asynchronous processing** – Concurrent workers through an `asyncio.Queue`
- **Event and alert persistence** – PostgreSQL models for events, alerts, users and failures
- **Configurable alerting** – Severity thresholds for warning, error and fatal events
- **Retry and dead-letter handling** – Failed processor deliveries are retained for inspection
- **JWT authentication** – Protected public event query and creation endpoints
- **Docker Compose deployment** – Reproducible local multi-service environment


![Arquitectura del sistema](assets/3v3nTracer-diagram.png)

## Modular product model

The platform is designed around capabilities that can be combined or packaged separately:

| Module | Responsibility | Potential product form |
|---|---|---|
| Backend API | Authentication, validation, event queries and PostgreSQL persistence | Core event-ingestion and observability API |
| Collector | Receives or observes events before asynchronous processing | Client SDK, browser integration or ingestion service |
| Event Processor | Validation, delivery retries and dead-letter routing | Processing and reliability service |
| Alert Engine | Severity evaluation and alert persistence | Independent alerting and notification module |

This modularity is a product and ownership decision, not a claim that every component already runs as a fully independent production service. The current MVP uses the backend as the official entry point:

```text
Client or fetchTrace
  |
  v
FastAPI backend
  |
  v
PostgreSQL
```

The asynchronous components are prepared for later integration through a durable queue when scale, replayability or horizontal deployment becomes a requirement.

## 📋 Requirements

### System Requirements

- **Python**: 3.11 or higher
- **PostgreSQL**: 14 or higher (or use Docker)
- **Docker**: 20.10+ (optional, for containerized deployment)
- **Docker Compose**: 2.0+ (optional)

### Supported Platforms

- Linux (Ubuntu 20.04+, Debian 11+, CentOS 8+)
- macOS (12.0+)
- Windows 10/11 (with WSL2 recommended)

## 🚀 Installation

### Option 1: Local Installation

```bash
git clone https://github.com/Rub3cK0r3/3v3nTr4cer.git
cd 3v3nTr4cer
```

### 3.2 Option A: Local Python install

```bash
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate  # Windows
pip install --upgrade pip
pip install -r deploy/requirements.txt
```

### 3.3 Option B: Docker (recommended)

```bash
cd deploy
docker compose up -d
```

Check service status:

```bash
docker compose ps
```

Stop services:

```bash
docker compose down -v
```

### 3.4 Database initialization

The Docker Compose service `db` already provisions `eventdb` and initializes `db/init/V1_schema_dev.sql`.

For local PostgreSQL:

```bash
createdb eventdb
psql -d eventdb -f db/init/V1_schema_dev.sql
```

### 3.5 Environment variables (optional)

```bash
export POSTGRES_USER=devuser
export POSTGRES_PASSWORD=devpass
export POSTGRES_DB=eventdb
export DB_HOST=localhost
export DATABASE_URL="postgresql://devuser:devpass@localhost:5432/eventdb"
export SECRET_KEY="your-secret-key"
export ALERT_MIN_SEVERITY="error"
```

## 4. Usage with examples and commands

### 4.1 Start manual components (non-Docker)

```bash
export PYTHONPATH=src:$PYTHONPATH
uvicorn core.backend.main:app --host 0.0.0.0 --port 8000
python -m core.async_lib.collector.main
python -m core.async_lib.processor.main
python -m core.async_lib.alert_engine.main
```

### 4.2 Authenticate & token endpoint

1. Ensure a user exists in `users` table (with hashed password).  
2. Request token:

```bash
curl -X POST "http://localhost:8000/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=secret"
```

Response:

```json
{
  "access_token": "...",
  "token_type": "bearer"
}
```

### 4.3 API endpoints (requires Bearer token)

- `GET /v1/events` – list events
- `GET /v1/events/{event_id}` – get a single event
- `POST /v1/events` – create event
- `POST /internal/pipeline/events` – ingest pipeline event
- `POST /internal/pipeline/alerts` – ingest pipeline alert

### 4.4 Event location fields

Events may include both `resource` and `referrer`. They describe different parts of the request context:

| Field | Meaning | Example |
|---|---|---|
| `resource` | The application resource affected by the event, such as a route, API endpoint, file, service, or component. | `/checkout` or `/api/orders` |
| `referrer` | The page or URL that led the client to the affected resource. It describes the request origin, not the failing resource. | `https://example.com/cart` |

For example, an event with `resource: "/checkout"` and `referrer: "https://example.com/cart"` means that the problem occurred on the checkout resource after the client came from the cart page. The `referrer` may be absent when the client did not provide one or when the event was not generated by a browser navigation.

#### Example create event request

```bash
curl -X POST "http://localhost:8000/v1/events" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "severity": "info",
    "timestamp": 1700000000000,
    "app_name": "my-app",
    "endpoint_id": "client-123"
  }'
```

### 4.4 Internal pipeline example

```bash
curl -X POST "http://localhost:8000/internal/pipeline/events" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "event",
    "payload": {"id":"evt-100","app_name":"my-app","endpoint_id":"client-123","timestamp":1700000000000}
  }'
```

```bash
curl -X POST "http://localhost:8000/internal/pipeline/alerts" \
  -H "Content-Type: application/json" \
  -d '{
    "severity": "error",
    "resource": "service-A",
    "payload": {"id":"alert-1","message":"error triggered"}
  }'
```

### 4.5 JavaScript and TypeScript `fetchTrace` examples

The [`examples/`](examples) directory contains browser reference helpers that wrap `fetch`:

- [`fetch-trace.js`](examples/fetch-trace.js) – JavaScript version
- [`fetch-trace.ts`](examples/fetch-trace.ts) – TypeScript version with typed options and events

The TypeScript helper can be used as follows:

```typescript
import { createFetchTrace } from "./examples/fetch-trace";

const fetchTrace = createFetchTrace({
  appName: "checkout-web",
  appVersion: "1.0.0",
  appStage: "production",
  traceEndpoint: "http://localhost:8000/internal/pipeline/events"
});

const response = await fetchTrace("/api/orders");
```

Both helpers report network failures and non-success HTTP responses using the pipeline event contract while preserving the original `fetch` behavior. `resource` identifies the affected route or endpoint, while `referrer` identifies the previous page that led to the request. The current collector consumes PostgreSQL notifications; configure `traceEndpoint` to use a dedicated collector HTTP route when one becomes available.

## 5. Contribution and collaborators guide

### 5.1 Workflow

1. Fork repo
2. Create branch `feature/<name>` or `fix/<name>`
3. Implement changes and tests
4. Run tests
5. Submit PR with description and context

### 5.2 Coding standards

- Keep clean Python style (PEP 8)
- Document functions and modules
- Avoid hardcoded credentials
- Use existing layers: `core.async_lib` for async logic, `core.backend` for API

### 5.3 Recommended checks

```bash
pip install black flake8
black .
flake8 src
```

## 6. Tests

### 6.1 Run test suite

```bash
cd /home/ruben/Desktop/github_repos/3v3nTr4cer
source .venv/bin/activate
PYTHONPATH=src python -m unittest discover -s src/tests -v
```

### 6.2 Included tests

- `src/tests/test_collector.py` – event validation
- `src/tests/test_alert_manager.py` – alert threshold and validation
- `src/tests/test_integration.py` – pipeline integration
- `src/tests/test_processor.py` – processor validator wrapper
- `src/tests/test_async_manager.py` – async queue management
- `src/tests/test_collector_async.py` – async collector API/WebSocket handling
- `src/tests/test_processor_async.py` – async processor handling

## 7. License

MIT License. See [LICENSE](LICENSE).

## 8. Project structure

```
.
├── CONTRIBUTING.md
├── examples
│   ├── fetch-trace.js
│   └── fetch-trace.ts
├── db
│   ├── commands
│   └── init
├── deploy
│   ├── compose.yml
│   ├── docker
│   └── requirements.txt
├── installer.sh
├── LICENSE
├── logs.txt
├── README.md
├── setup.sh
├── src
│   ├── alert_engine
│   ├── collector
│   ├── contracts
│   ├── core
│   ├── processor
│   └── tests
└── systemd
    ├── 3v3nTr4cer.service
    └── setup.sh
```

## Next Steps 🏆
We now have a **functional MVP**. 
The next steps will focus on making it 
work **independently** and eventually **developing**
multiple __fully functional, "market-ready" products__.
