FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir polars pyarrow duckdb

COPY pyproject.toml .
COPY tse_tick/ ./tse_tick/

RUN pip install --no-cache-dir -e .

ENTRYPOINT ["tse-tick"]
