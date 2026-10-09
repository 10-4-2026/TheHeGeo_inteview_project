"""Data Quality ở mức DATASET (bổ sung cho rule mức bản ghi trong silver.py).

Mỗi check có severity:
  - error : vi phạm -> quality gate FAIL -> Airflow dừng, không build Gold (không đẩy số sai lên dashboard)
  - warn  : ghi nhận vào báo cáo, vẫn cho chạy tiếp
Kết quả ghi ra reports/dq_report.json để theo dõi theo thời gian.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import MAX_QUARANTINE_RATIO, REPORTS, SILVER


def _check(name: str, severity: str, value, threshold, passed: bool, description: str) -> dict:
    return {"check": name, "severity": severity, "value": value, "threshold": threshold,
            "passed": bool(passed), "description": description}


def run_checks(apps: DataFrame, quarantine: DataFrame, services: DataFrame, as_of: date) -> dict:
    n = apps.count()
    n_q = quarantine.count()
    stats = apps.agg(
        F.countDistinct("app_id").alias("distinct_ids"),
        F.sum(F.col("fee_vnd").isNull().cast("int")).alias("fee_null"),
        F.sum(((F.col("status") != "RECEIVED") & F.col("completed_at").isNull()).cast("int")).alias("done_no_ts"),
        F.max("batch_date").alias("max_batch"),
    ).first()
    orphan_services = apps.join(services, "service_code", "left_anti").count()
    q_ratio = round(n_q / (n + n_q), 4) if (n + n_q) else 0.0
    fee_null_ratio = round((stats["fee_null"] or 0) / n, 4) if n else 0.0
    max_batch = stats["max_batch"]

    checks = [
        _check("silver_not_empty", "error", n, "> 0", n > 0, "Silver phải có dữ liệu"),
        _check("app_id_unique", "error", n - stats["distinct_ids"], "= 0",
               n == stats["distinct_ids"], "Mỗi hồ sơ chỉ còn 1 dòng sau khi khử trùng lặp"),
        _check("quarantine_ratio", "error", q_ratio, f"<= {MAX_QUARANTINE_RATIO}",
               q_ratio <= MAX_QUARANTINE_RATIO, "Tỷ lệ bản ghi lỗi bất thường => nghi nguồn đổi định dạng"),
        _check("finished_has_completed_at", "error", stats["done_no_ts"], "= 0",
               stats["done_no_ts"] == 0, "Hồ sơ đã xử lý xong phải có thời điểm hoàn thành"),
        _check("referential_service", "error", orphan_services, "= 0",
               orphan_services == 0, "Mọi service_code phải tồn tại trong danh mục"),
        _check("fee_null_ratio", "warn", fee_null_ratio, "<= 0.4",
               fee_null_ratio <= 0.4, "Phí bỏ trống (thường là thủ tục miễn phí)"),
        _check("freshness", "warn", str(max_batch), f">= {as_of - timedelta(days=1)}",
               max_batch is not None and max_batch >= as_of - timedelta(days=1),
               "Batch mới nhất không được trễ quá 1 ngày so với ngày chạy"),
    ]
    reasons = (quarantine.select(F.explode("dq_errors").alias("reason"))
               .groupBy("reason").count().orderBy(F.desc("count")).collect())

    failed_errors = [c["check"] for c in checks if c["severity"] == "error" and not c["passed"]]
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "as_of": str(as_of),
        "rows_valid": n, "rows_quarantined": n_q,
        "checks": checks,
        "quarantine_reasons": {r["reason"]: r["count"] for r in reasons},
        "gate_passed": not failed_errors,
        "failed_error_checks": failed_errors,
    }


def run(spark: SparkSession, run_id: str, as_of: date) -> dict:
    report = run_checks(
        spark.read.parquet(str(SILVER / "applications")),
        spark.read.parquet(str(SILVER / "quarantine" / "applications")),
        spark.read.parquet(str(SILVER / "services")),
        as_of,
    )
    report["run_id"] = run_id
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "dq_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "inputs": [str(SILVER)], "outputs": [str(REPORTS / "dq_report.json")],
        "rows": {"silver.applications": report["rows_valid"], "silver.quarantine": report["rows_quarantined"]},
        "gate_passed": report["gate_passed"], "failed_error_checks": report["failed_error_checks"],
    }
