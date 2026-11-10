import os
import sys
import logging
from datetime import datetime, timezone
import pandas as pd
import duckdb
from faker import Faker
import random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("duckpipe-ingestion")

def get_env_variable(var_name: str, default: str = None) -> str:
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
    customers = []
    for i in range(1, num_customers + 1):
        customers.append({
            "customer_id": f"CUST_{i:05d}",
            "full_name": fake.name(),
            "email": fake.email(),
            "country": fake.country_code(),
            "created_at": fake.date_time_between(start_date="-1y", end_date="now", tzinfo=timezone.utc).isoformat()
        })
    df_customers = pd.DataFrame(customers)

    logger.info(f"Đang sinh {num_orders} bản ghi đơn hàng (Orders)...")
    statuses = ["COMPLETED", "PROCESSING", "CANCELLED", "REFUNDED"]
    payment_methods = ["CREDIT_CARD", "PAYPAL", "BANK_TRANSFER", "CRYPTO"]
    
    orders = []
    for i in range(1, num_orders + 1):
        cust = random.choice(customers)
        orders.append({
            "order_id": f"ORD_{i:07d}",
            "customer_id": cust["customer_id"],
            "order_date": fake.date_time_between(start_date="-6m", end_date="now", tzinfo=timezone.utc).isoformat(),
            "status": random.choices(statuses, weights=[0.7, 0.15, 0.1, 0.05])[0],
            "total_amount": round(random.uniform(10.0, 500.0), 2),
            "payment_method": random.choice(payment_methods)
        })
    df_orders = pd.DataFrame(orders)

    return df_customers, df_orders

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

    # 1. Sinh dữ liệu mẫu
    df_customers, df_orders = generate_sample_data(num_customers=200, num_orders=1000)

    # 2. Khởi tạo DuckDB engine để ghi trực tiếp bảng Iceberg vào Nessie
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
        logger.info(f"--> Đã kết nối thành công tới Nessie Iceberg REST Catalog qua DuckDB.")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{target_database};")
    except Exception as e:
        logger.error(f"Lỗi kết nối Nessie Catalog qua DuckDB: {e}")
        sys.exit(1)

    # 3. Đăng ký rõ ràng biến DataFrame vào DuckDB catalog để quản lý bộ nhớ và tường minh query
    con.register("df_customers", df_customers)
    con.register("df_orders", df_orders)
    logger.info("--> Đã đăng ký (register) df_customers và df_orders vào DuckDB session.")

    # 4. Kiểm tra các bảng đã tồn tại trong database duckpipe_bronze
    try:
        existing_tables = [
            r[0].lower() 
            for r in con.execute(f"SHOW TABLES FROM {catalog_name}.{target_database};").fetchall()
        ]
        logger.info(f"Danh sách bảng hiện có trong {target_database}: {existing_tables}")
    except Exception as e:
        logger.warning(f"Không thể lấy danh sách bảng (mặc định coi như chưa có): {e}")
        existing_tables = []

    # 5. Ghi dữ liệu Khách hàng vào bảng duckpipe_bronze.customers
    cust_table = "customers"
    try:
        logger.info(f"Đang ghi bảng Iceberg: {catalog_name}.{target_database}.{cust_table} ...")
        if cust_table in existing_tables:
            con.execute(f"INSERT INTO {catalog_name}.{target_database}.{cust_table} SELECT * FROM df_customers;")
        else:
            con.execute(f"CREATE TABLE {catalog_name}.{target_database}.{cust_table} AS SELECT * FROM df_customers;")
        logger.info(f"--> Đã tạo/ghi thành công bảng: {target_database}.{cust_table}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi bảng {cust_table}: {e}")
        sys.exit(1)

    # 6. Ghi dữ liệu Đơn hàng vào bảng duckpipe_bronze.orders
    orders_table = "orders"
    try:
        logger.info(f"Đang ghi bảng Iceberg: {catalog_name}.{target_database}.{orders_table} ...")
        if orders_table in existing_tables:
            con.execute(f"INSERT INTO {catalog_name}.{target_database}.{orders_table} SELECT * FROM df_orders;")
        else:
            con.execute(f"CREATE TABLE {catalog_name}.{target_database}.{orders_table} AS SELECT * FROM df_orders;")
        logger.info(f"--> Đã tạo/ghi thành công bảng: {target_database}.{orders_table}")
        logger.info("=== HOÀN THÀNH TẦNG INGESTION: TOÀN BỘ BRONZE ĐÃ ĐƯỢC ĐĂNG KÝ VÀO NESSIE ===")
    except Exception as e:
        logger.error(f"Lỗi khi ghi bảng {orders_table}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
