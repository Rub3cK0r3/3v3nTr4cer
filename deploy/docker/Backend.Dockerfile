FROM python:3.12-slim
WORKDIR /app
COPY deploy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ /app
COPY migrations/ /app/migrations/
COPY alembic.ini /app/alembic.ini
ENV PYTHONPATH=/app:$PYTHONPATH
CMD ["sh", "-c", "alembic upgrade head && uvicorn core.backend.main:app --host 0.0.0.0 --port 8000"]
