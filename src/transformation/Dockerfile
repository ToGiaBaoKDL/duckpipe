FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/app

WORKDIR /app

# Cài đặt dbt-duckdb
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tải sẵn các extension của DuckDB ngay lúc build image
# (giúp Pod khi chạy trong K8s không cần tốn mạng tải lại extension)
RUN python -c "import duckdb; con = duckdb.connect(); con.execute('INSTALL httpfs; INSTALL aws; INSTALL iceberg; INSTALL avro; INSTALL parquet;')"

# Copy toàn bộ dbt project vào container
COPY . .

# Chạy dbt build (vừa build model vừa chạy test tự động theo thứ tự DAG) khi container bật lên
ENTRYPOINT ["dbt", "build"]
