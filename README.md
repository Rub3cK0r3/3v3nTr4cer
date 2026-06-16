# 3v3nTr4cer

![Release](https://img.shields.io/github/v/release/Rub3cK0r3/3v3nTr4cer)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-in%20development-orange)

> CLI-first system for backend event ingestion, processing, and alerting with a focus on security and auditability.

`3v3nTr4cer` is a **backend-first framework** for handling **event-pipelines** in Python. It enables collecting events from enterprise systems, processing them asynchronously, storing secure logs, and generating alerts — all from the command line. Designed to be secure, deployable via Docker, with full traceability for auditing.

---

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

---

## ✨ Features

- **Asynchronous Event Ingestion** – Reliable async event collection
- **Event Normalization & Processing** – Transform and process events in real-time
- **Secure PostgreSQL Storage** – Tamper-evident logging with SQLAlchemy/SQLModel
- **Configurable Alert Engine** – CLI alerts, email simulation, SIEM hooks
- **RBAC on All Endpoints** – Role-based access control built-in
- **Docker & Kubernetes Ready** – Deploy with Docker Compose or K8s
- **Security-First Design** – Pentesting and hardening from day one


![Arquitectura del sistema](assets/3v3nTracer-diagram.png)

---

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

---

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
cd src
python -m unittest discover -s tests -p "test_*.py"
```

### 6.2 Included tests

- `src/tests/test_collector.py` – event validation
- `src/tests/test_alert_manager.py` – alert threshold and validation
- `src/tests/test_integration.py` – pipeline integration
- `src/tests/test_processor.py` – processor validator wrapper

## 7. License

MIT License. See [LICENSE](LICENSE).

## 8. Project structure

```
.
├── CONTRIBUTING.md
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
