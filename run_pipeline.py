"""CLI điều phối pipeline. Airflow gọi từng stage riêng; chạy local dùng --stage all.

  python -m lakehouse.run_pipeline --stage all --as-of 2026-09-30
  python -m lakehouse.run_pipeline --stage bronze --batch-date 2026-09-30
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from datetime import date, datetime

from lakehouse import bronze, dashboard, gold, quality, silver
from lakehouse.config import REPORTS
from lakehouse.spark import get_spark

STAGES = ["bronze", "silver", "quality", "gold", "dashboard"]


def log_lineage(run_id: str, stage: str, result: dict, seconds: float) -> None:
    """Ghi 1 dòng lineage/audit (JSON Lines): stage nào đọc gì, ghi gì, bao nhiêu dòng."""
    REPORTS.mkdir(parents=True, exist_ok=True)
    entry = {"run_id": run_id, "stage": stage, "finished_at": datetime.now().isoformat(timespec="seconds"),
             "duration_s": round(seconds, 1), **result}
    with open(REPORTS / "lineage.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def main(argv=None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--stage", choices=STAGES + ["all"], default="all")
    p.add_argument("--as-of", type=date.fromisoformat, default=date.today(),
                   help="Ngày chốt số liệu (Airflow truyền {{ ds }})")
    p.add_argument("--batch-date", type=date.fromisoformat, default=None,
                   help="Chỉ nạp file landing của ngày này vào Bronze (mặc định: tất cả = backfill)")
    p.add_argument("--run-id", default=None)
    a = p.parse_args(argv)

    run_id = a.run_id or f"{datetime.now():%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    spark = get_spark()
    stages = STAGES if a.stage == "all" else [a.stage]
    for stage in stages:
        t0 = time.time()
        if stage == "bronze":
            result = bronze.run(spark, run_id, a.batch_date)
        elif stage == "silver":
            result = silver.run(spark, run_id)
        elif stage == "quality":
            result = quality.run(spark, run_id, a.as_of)
        elif stage == "gold":
            result = gold.run(spark, run_id, a.as_of)
        else:
            result = dashboard.run(spark, run_id)
        dt = time.time() - t0
        log_lineage(run_id, stage, result, dt)
        print(f"[{run_id}] {stage:<9} {dt:6.1f}s  {json.dumps(result.get('rows', {}), ensure_ascii=False)}")

        if stage == "quality" and not result["gate_passed"]:
            print(f"QUALITY GATE FAILED: {result['failed_error_checks']} -> dừng, không build Gold.", file=sys.stderr)
            return 1
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
