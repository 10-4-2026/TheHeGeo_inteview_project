"""Đường dẫn và cấu hình chung. Ghi đè bằng biến môi trường khi chạy trên Airflow/K8s."""
from __future__ import annotations

import os
import warnings
from pathlib import Path

ROOT = Path(os.environ.get("LAKEHOUSE_ROOT", Path(__file__).resolve().parents[2]))
DATA = ROOT / "data"

LANDING = DATA / "landing"   # file thô do hệ thống nguồn đẩy sang
BRONZE = DATA / "bronze"     # bản sao thô, append-only, có metadata ingest
SILVER = DATA / "silver"     # đã làm sạch, chuẩn hoá, khử trùng lặp, che PII
GOLD = DATA / "gold"         # mô hình sao + data mart + feature dataset
REPORTS = ROOT / "reports"   # DQ report, lineage, dashboard

TIMEZONE = "Asia/Ho_Chi_Minh"

# Ngưỡng của quality gate
MAX_QUARANTINE_RATIO = float(os.environ.get("LAKEHOUSE_MAX_QUARANTINE_RATIO", "0.05"))


def pii_salt() -> str:
    """Salt để băm CCCD. Trên môi trường thật lấy từ secret manager / Airflow Variable."""
    salt = os.environ.get("LAKEHOUSE_PII_SALT")
    if not salt:
        warnings.warn("LAKEHOUSE_PII_SALT chưa được đặt — đang dùng salt mặc định chỉ cho môi trường dev.")
        salt = "dev-only-salt"
    return salt
