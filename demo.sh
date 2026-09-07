#!/usr/bin/env bash
set -euo pipefail

compose=(docker compose -f deploy/compose.yml)

"${compose[@]}" up -d --build

for attempt in {1..30}; do
    if curl --fail --silent http://localhost:8000/health >/dev/null; then
        break
    fi
    if [[ "$attempt" == "30" ]]; then
        echo "Backend health check failed" >&2
        exit 1
    fi
    sleep 2
done

echo "Health:"
curl --fail --silent http://localhost:8000/health
echo
echo "Readiness:"
curl --fail --silent http://localhost:8000/ready
echo

PYTHONPATH=src .venv/bin/python -m unittest discover -s src/tests -v 2>&1 | tee test_results.txt
