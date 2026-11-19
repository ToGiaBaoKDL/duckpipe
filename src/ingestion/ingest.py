import logging
import os
import random
import sys
from datetime import timezone
import duckdb
from faker import Faker
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("duckpipe-ingestion")


def get_env_variable(var_name: str, default: str | None = None) -> str:
    val = os.getenv(var_name, default)
    if not val:
        logger.error(f"Thiếu biến môi trường bắt buộc: {var_name}")
        sys.exit(1)
    return val


def generate_sample_data(num_customers: int = 200, num_orders: int = 1000):
    fake = Faker()
    Faker.seed(42)
    random.seed(42)

    logger.info(f"Đang sinh {num_customers} bản ghi khách hàng (Customers)...")
    customers = [
        {
            "customer_id": f"CUST_{i:05d}",
            "full_name": fake.name(),
            "email": fake.email(),
            "country": fake.country_code(),
            "created_at": fake.date_time_between(
                start_date="-1y", end_date="now", tzinfo=timezone.utc
            ).isoformat(),
        }
        for i in range(1, num_customers + 1)
    ]
    df_customers = pd.DataFrame(customers)

    logger.info(f"Đang sinh {num_orders} bản ghi đơn hàng (Orders)...")
    statuses = ["COMPLETED", "PROCESSING", "CANCELLED", "REFUNDED"]
    payment_methods = ["CREDIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"]

    orders = []
    for i in range(1, num_orders + 1):
        cust = random.choice(customers)
        orders.append(
            {
                "order_id": f"ORD_{i:07d}",
                "customer_id": cust["customer_id"],
                "order_date": fake.date_time_between(
                    start_date="-6m", end_date="now", tzinfo=timezone.utc
                ).isoformat(),
                "status": random.choices(statuses, weights=[0.7, 0.15, 0.1, 0.05])[0],
                "total_amount": round(random.uniform(10.0, 500.0), 2),
                "payment_method": random.choice(payment_methods),
            }
        )
    df_orders = pd.DataFrame(orders)

    return df_customers, df_orders


def write_iceberg_table(
    con: duckdb.DuckDBPyConnection,
    catalog_name: str,
    schema_name: str,
    table_name: str,
    source_view: str,
    existing_tables: list[str],
) -> None:
    """Ghi DataFrame đã register vào bảng Iceberg (INSERT nếu đã tồn tại, ngược lại CREATE)."""
    fqn = f"{catalog_name}.{schema_name}.{table_name}"
    try:
        logger.info(f"Đang ghi bảng Iceberg: {fqn} ...")
        if table_name in existing_tables:
            con.execute(f"INSERT INTO {fqn} SELECT * FROM {source_view};")
        else:
            con.execute(f"CREATE TABLE {fqn} AS SELECT * FROM {source_view};")
        logger.info(f"--> Đã tạo/ghi thành công bảng: {schema_name}.{table_name}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi bảng {table_name}: {e}")
        sys.exit(1)


def main():
    logger.info("=== BẮT ĐẦU TẦNG INGESTION: GHI ICEBERG VÀO PROJECT NESSIE ===")

    aws_region = get_env_variable("AWS_REGION", "ap-southeast-1")
    s3_bucket = get_env_variable("S3_BUCKET", "timo-prod-dl-analytics-vt77zs")
    target_database = os.getenv("TARGET_SCHEMA", "duckpipe_bronze")
    nessie_endpoint = os.getenv("NESSIE_ENDPOINT", "http://nessie:19120/iceberg")
    catalog_name = os.getenv("CATALOG_NAME", "nessie_catalog")
    warehouse_name = os.getenv("NESSIE_WAREHOUSE", "lakehouse")

    logger.info(f"Cấu hình: Region = {aws_region}, Bucket = {s3_bucket}")
    logger.info(f"Catalog = {catalog_name}, Warehouse = {warehouse_name}, Schema = {target_database}")
    logger.info(f"Nessie Endpoint: {nessie_endpoint}")

    df_customers, df_orders = generate_sample_data(num_customers=200, num_orders=1000)

    try:
        con = duckdb.connect()
        con.execute("LOAD httpfs; LOAD aws; LOAD iceberg; LOAD avro; LOAD parquet;")
        con.execute(f"CREATE SECRET (TYPE S3, PROVIDER credential_chain, REGION '{aws_region}');")
        con.execute(
            f"ATTACH '{warehouse_name}' AS {catalog_name} ("
            f"    TYPE ICEBERG, "
            f"    URI '{nessie_endpoint}', "
            f"    AUTHORIZATION_TYPE 'none'"
            f");"
        )
        logger.info("--> Đã kết nối thành công tới Nessie Iceberg REST Catalog qua DuckDB.")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{target_database};")
    except Exception as e:
        logger.error(f"Lỗi kết nối Nessie Catalog qua DuckDB: {e}")
        sys.exit(1)

    con.register("df_customers", df_customers)
    con.register("df_orders", df_orders)
    logger.info("--> Đã đăng ký (register) df_customers và df_orders vào DuckDB session.")

    try:
        existing_tables = [
            r[0].lower()
            for r in con.execute(f"SHOW TABLES FROM {catalog_name}.{target_database};").fetchall()
        ]
        logger.info(f"Danh sách bảng hiện có trong {target_database}: {existing_tables}")
    except Exception as e:
        logger.warning(f"Không thể lấy danh sách bảng (mặc định coi như chưa có): {e}")
        existing_tables = []

    write_iceberg_table(con, catalog_name, target_database, "customers", "df_customers", existing_tables)
    write_iceberg_table(con, catalog_name, target_database, "orders", "df_orders", existing_tables)

    logger.info("=== HOÀN THÀNH TẦNG INGESTION: TOÀN BỘ BRONZE ĐÃ ĐƯỢC ĐĂNG KÝ VÀO NESSIE ===")


if __name__ == "__main__":
    main()
