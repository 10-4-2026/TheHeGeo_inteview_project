from __future__ import annotations

from datetime import date, datetime, timedelta

from lakehouse.gold import build_fact, build_late_risk_features


def services(spark):
    return spark.createDataFrame([("TP001", 1, "Tư pháp", 4)], ["service_code", "sla_days", "sector", "service_level"])


def app(app_id, unit, submitted, completed, status="COMPLETED"):
    return (app_id, "k", "TP001", unit, "ONLINE", status, 0, submitted, completed)


APP_COLS = ["app_id", "citizen_key", "service_code", "unit_code", "channel", "status", "fee_vnd",
            "submitted_at", "completed_at"]


def test_fact_late_and_overdue_flags(spark):
    t = datetime(2026, 9, 1, 9, 0)
    apps = spark.createDataFrame([
        app("ON_TIME", "P01", t, t + timedelta(hours=20)),
        app("LATE", "P01", t, t + timedelta(days=2)),
        app("OPEN_OVERDUE", "P01", t, None, "RECEIVED"),
        app("OPEN_OK", "P01", datetime(2026, 9, 30, 9, 0), None, "RECEIVED"),
    ], APP_COLS)
    f = {r.app_id: r for r in build_fact(apps, services(spark), date(2026, 9, 30)).collect()}
    assert f["ON_TIME"].is_late is False and f["LATE"].is_late is True
    assert f["OPEN_OVERDUE"].is_overdue_open is True and f["OPEN_OK"].is_overdue_open is False
    assert f["LATE"].processing_days == 2.0


def test_features_are_point_in_time(spark):
    """Hồ sơ trễ hoàn thành CÙNG NGÀY hoặc SAU ngày nộp không được lọt vào feature (chống leakage)."""
    d0 = datetime(2026, 9, 10, 8, 0)
    apps = spark.createDataFrame([
        app("PAST_LATE", "P01", d0 - timedelta(days=5), d0 - timedelta(days=2)),    # xong trước ngày nộp
        app("SAME_DAY_LATE", "P01", d0 - timedelta(days=3), d0 + timedelta(hours=5)),  # xong đúng ngày nộp
        app("TARGET", "P01", d0, d0 + timedelta(hours=3)),
    ], APP_COLS)
    fact = build_fact(apps, services(spark), date(2026, 9, 30))
    feats = {r.app_id: r for r in build_late_risk_features(spark, fact, services(spark)).collect()}
    t = feats["TARGET"]
    assert t.unit_finished_prev30d == 1          # chỉ PAST_LATE
    assert t.unit_late_rate_prev30d == 1.0
    assert t.unit_received_prev7d == 2           # PAST_LATE, SAME_DAY_LATE nộp trong 7 ngày trước
