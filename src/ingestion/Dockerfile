FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tải sẵn các extension của DuckDB ngay lúc build image
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs; INSTALL aws; INSTALL iceberg; INSTALL avro; INSTALL parquet;')"

COPY ingest.py .

ENTRYPOINT ["python", "ingest.py"]
