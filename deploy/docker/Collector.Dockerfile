FROM python:3.12-slim
WORKDIR /app
RUN apt-get update \
	&& apt-get install -y --no-install-recommends build-essential \
	&& rm -rf /var/lib/apt/lists/*
COPY deploy/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pyproject.toml setup.py README.md ./
COPY src/ ./src/
RUN pip install --no-cache-dir .
CMD ["python", "-m", "core.async_lib.collector.main"]
