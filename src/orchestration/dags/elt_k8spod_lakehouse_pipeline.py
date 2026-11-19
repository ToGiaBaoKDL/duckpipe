"""
ELT Lakehouse Pipeline DAG (Apache Airflow 3 Task SDK)
======================================================
Naming convention: <task>_<engine>_<description>
  - task: elt (Ingest Bronze -> dbt Silver/Gold)
  - engine: k8spod (KubernetesPodOperator, deferrable trên node workload=batch)
  - description: lakehouse_pipeline
"""

from datetime import datetime, timedelta
import logging
import os
from pathlib import Path
import requests

from airflow.exceptions import AirflowNotFoundException
from airflow.sdk import BaseHook, Variable, dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

logger = logging.getLogger(__name__)

# Đọc dag_id động theo tên file để đồng bộ giữa filesystem, UI và Kustomize
DAG_ID = Path(__file__).stem

DEFAULT_ARGS = {
    "owner": "bao.to",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

# Cấu hình dùng chung cho các batch pod trên node agent-1 (workload=batch)
# env_from đã nạp sẵn AWS_REGION, S3_BUCKET, NESSIE_ENDPOINT, NESSIE_WAREHOUSE từ aws-config
COMMON_K8S_POD_KWARGS = {
    "namespace": "duckpipe",
    "image_pull_policy": os.getenv("AIRFLOW_VAR_K8S_POD_IMAGE_PULL_POLICY", "Always"),
    "node_selector": {"workload": "batch"},
    "env_from": [
        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="aws-config")),
        k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="aws-credentials")),
    ],
    "get_logs": True,
    "deferrable": True,
    "on_finish_action": "delete_succeeded_pod",
}


def _get_clickhouse_client_config() -> tuple[str, tuple[str, str] | None]:
    """Lấy base URL và thông tin xác thực ClickHouse từ Airflow Connection hoặc Variable."""
    try:
        conn = BaseHook.get_connection("clickhouse_default")
        scheme = "https" if conn.conn_type == "https" else "http"
        port_suffix = f":{conn.port}" if conn.port else ""
        base_url = f"{scheme}://{conn.host}{port_suffix}"
        auth = (conn.login, conn.password) if conn.login and conn.password else None
        return base_url, auth
    except AirflowNotFoundException:
        base_url = Variable.get(
            "clickhouse_http_url",
            default="http://clickhouse:8123",
        ).rstrip("/")
        return base_url, None


@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Orchestrate Lakehouse: Nessie + S3 Iceberg + dbt + ClickHouse Zero-Copy",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["elt", "k8spod", "lakehouse", "iceberg", "dbt", "clickhouse"],
)
def elt_k8spod_lakehouse_pipeline():
    @task
    def check_infrastructure_health():
        """Kiểm tra Nessie REST Catalog và ClickHouse sẵn sàng trước khi chạy batch jobs."""
        nessie_url = Variable.get(
            "nessie_health_url",
            default="http://nessie:19120/api/v1/config",
        )
        clickhouse_base, _ = _get_clickhouse_client_config()

        with requests.Session() as session:
            logger.info("Checking Nessie REST Catalog at %s", nessie_url)
            resp_nessie = session.get(nessie_url, timeout=10)
            resp_nessie.raise_for_status()
            logger.info(
                "Nessie Catalog ready (defaultBranch=%s)",
                resp_nessie.json().get("defaultBranch", "main"),
            )

            ch_ping_url = f"{clickhouse_base}/ping"
            logger.info("Checking ClickHouse HTTP API at %s", ch_ping_url)
            resp_ch = session.get(ch_ping_url, timeout=10)
            resp_ch.raise_for_status()
            if resp_ch.text.strip() != "Ok.":
                raise RuntimeError(f"Unexpected ClickHouse ping response: {resp_ch.text}")
            logger.info("ClickHouse Query Engine ready.")

    ingest_bronze = KubernetesPodOperator(
        task_id="ingest_bronze_iceberg",
        name="duckpipe-ingest-task",
        image=os.getenv(
            "AIRFLOW_VAR_DUCKPIPE_INGEST_IMAGE",
            "localhost:5001/duckpipe-ingest:latest",
        ),
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "128Mi"},
            limits={"cpu": "500m", "memory": "512Mi"},
        ),
        **COMMON_K8S_POD_KWARGS,
    )

    transform_silver_gold = KubernetesPodOperator(
        task_id="dbt_transform_and_test",
        name="duckpipe-dbt-task",
        image=os.getenv(
            "AIRFLOW_VAR_DUCKPIPE_DBT_IMAGE",
            "localhost:5001/duckpipe-dbt:latest",
        ),
        env_vars=[
            k8s.V1EnvVar(name="DO_NOT_TRACK", value="1"),
        ],
        volumes=[
            k8s.V1Volume(
                name="duckdb-scratch",
                empty_dir=k8s.V1EmptyDirVolumeSource(),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(
                name="duckdb-scratch",
                mount_path="/tmp/duckdb_scratch",
            ),
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "200m", "memory": "256Mi"},
            limits={"cpu": "1000m", "memory": "512Mi"},
        ),
        **COMMON_K8S_POD_KWARGS,
    )

    @task
    def sync_clickhouse_serving():
        """Làm mới Iceberg metadata cache trên ClickHouse và kiểm tra dữ liệu bảng Gold."""
        clickhouse_base, ch_auth = _get_clickhouse_client_config()
        ch_query_url = f"{clickhouse_base}/"

        with requests.Session() as session:
            drop_cache_query = "SYSTEM DROP ICEBERG METADATA CACHE;"
            logger.info("Executing ClickHouse query: %s", drop_cache_query)
            resp = session.post(ch_query_url, data=drop_cache_query, auth=ch_auth, timeout=15)
            resp.raise_for_status()

            verify_query = (
                "SELECT count() AS total_records, round(sum(total_revenue), 2) AS grand_total "
                "FROM lakehouse.duckpipe_gold.fct_daily_sales "
                "FORMAT JSONEachRow;"
            )
            logger.info("Executing verification query on Gold layer...")
            resp_check = session.post(ch_query_url, data=verify_query, auth=ch_auth, timeout=15)
            resp_check.raise_for_status()
            logger.info("Gold fct_daily_sales verification: %s", resp_check.text.strip())

    check_infrastructure_health() >> ingest_bronze >> transform_silver_gold >> sync_clickhouse_serving()


elt_k8spod_lakehouse_pipeline()
