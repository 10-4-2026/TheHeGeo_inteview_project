"""Airflow DAG chạy hằng ngày: chờ file landing -> Bronze -> Silver -> DQ gate -> Gold -> Dashboard.

Thiết kế:
  - Mỗi stage 1 task, gọi CLI `lakehouse.run_pipeline` (task mỏng, logic nằm trong package -> test được).
  - Bronze nạp đúng file của ngày {{ ds }}; chạy lại / backfill 1 ngày không tạo trùng (dynamic partition overwrite).
  - Task quality trả exit code 1 nếu vi phạm rule mức "error" -> Gold không được build.
  - Trên K8s: thay BashOperator bằng KubernetesPodOperator / SparkSubmitOperator, giữ nguyên lệnh.
Tương thích Airflow 2.x và 3.x.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.sensors.python import PythonSensor
except ImportError:  # Airflow 2
    from airflow.operators.bash import BashOperator
    from airflow.sensors.python import PythonSensor

PROJECT_DIR = "/opt/airflow/gov-lakehouse"   # chỉnh theo nơi deploy
CLI = f"cd {PROJECT_DIR} && PYTHONPATH=src python -m lakehouse.run_pipeline --run-id {{{{ run_id }}}}"


def _landing_file_exists(ds_nodash: str) -> bool:
    return Path(PROJECT_DIR, "data/landing/applications", f"applications_{ds_nodash}.csv").exists()


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="gov_lakehouse_daily",
    description="Lakehouse hồ sơ thủ tục hành chính: Bronze/Silver/Gold",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * *",          # 01:00 hằng ngày, sau khi hệ thống nguồn đẩy file của ngày hôm trước
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["lakehouse", "govtech"],
) as dag:
    wait_for_file = PythonSensor(
        task_id="wait_for_landing_file",
        python_callable=_landing_file_exists,
        op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        poke_interval=300, timeout=6 * 3600, mode="reschedule",
    )
    bronze = BashOperator(task_id="bronze_ingest", bash_command=f"{CLI} --stage bronze --batch-date {{{{ ds }}}}")
    silver = BashOperator(task_id="silver_transform", bash_command=f"{CLI} --stage silver")
    quality = BashOperator(task_id="quality_gate", bash_command=f"{CLI} --stage quality --as-of {{{{ ds }}}}")
    gold = BashOperator(task_id="gold_build", bash_command=f"{CLI} --stage gold --as-of {{{{ ds }}}}")
    dashboard = BashOperator(task_id="dashboard_refresh", bash_command=f"{CLI} --stage dashboard")

    wait_for_file >> bronze >> silver >> quality >> gold >> dashboard
