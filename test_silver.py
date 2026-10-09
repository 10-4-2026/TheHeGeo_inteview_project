from __future__ import annotations

from datetime import date, datetime

from pyspark.sql import functions as F

from lakehouse.silver import (PII_RAW_COLUMNS, parse_fee, parse_ts, standardize_channel,
                              transform_applications)

COLS = ["app_id", "citizen_id", "full_name", "phone", "service_code", "unit_code", "channel",
        "status", "fee", "submitted_at", "completed_at", "updated_at", "batch_date", "_source_file", "_ingested_at"]


def raw(spark, rows):
    now = datetime(2026, 9, 30, 1, 0, 0)
    return spark.createDataFrame([r + (date(2026, 9, 1), "f.csv", now) for r in rows], COLS)


def refs(spark):
    services = spark.createDataFrame([("TP001",)], ["service_code"])
    units = spark.createDataFrame([("P01",)], ["unit_code"])
    return services, units


def test_parse_timestamp_multiple_formats(spark):
    df = spark.createDataFrame([("2026-03-04 09:12:33",), ("04/03/2026 09:12",),
                                ("2026-03-04T09:12:33+07:00",), ("rác",), ("",)], ["s"])
    out = [r.t for r in df.select(parse_ts(F.col("s")).alias("t")).collect()]
    assert out[0] == datetime(2026, 3, 4, 9, 12, 33)
    assert out[1] == datetime(2026, 3, 4, 9, 12)
    assert out[2] == datetime(2026, 3, 4, 9, 12, 33)   # session timezone = Asia/Ho_Chi_Minh
    assert out[3] is None and out[4] is None


def test_standardize_channel_and_fee(spark):
    df = spark.createDataFrame([("Trực tuyến", "50.000đ"), (" OFFLINE ", "2000 VND"),
                                ("truc_tiep", ""), ("fax", None)], ["c", "f"])
    out = df.select(standardize_channel(F.col("c")).alias("c"), parse_fee(F.col("f")).alias("f")).collect()
    assert [r.c for r in out] == ["ONLINE", "OFFLINE", "OFFLINE", None]
    assert [r.f for r in out] == [50000, 2000, None, None]


def test_keeps_latest_version_and_drops_resent_duplicates(spark):
    base = ("HS1", "001234567890", "Nguyễn Văn A", "0912345678", "TP001", "P01", "online", "RECEIVED", "0",
            "2026-09-01 08:00:00", "", "2026-09-01 08:00:00")
    done = base[:7] + ("COMPLETED", "0", "2026-09-01 08:00:00", "2026-09-01 15:00:00", "2026-09-01 15:00:00")
    services, units = refs(spark)
    valid, quarantine = transform_applications(raw(spark, [base, base, done]), services, units, "s")
    rows = valid.collect()
    assert len(rows) == 1 and rows[0].status == "COMPLETED"
    assert quarantine.count() == 0


def test_invalid_records_go_to_quarantine_with_reasons(spark):
    ok = ("HS1", "001234567890", "A", "0912345678", "TP001", "P01", "online", "RECEIVED", "0",
          "2026-09-01 08:00:00", "", "2026-09-01 08:00:00")
    bad_svc = ("HS2",) + ok[1:4] + ("XX999",) + ok[5:]
    bad_id = ("HS3", "") + ok[2:]
    bad_time = ("HS4",) + ok[1:7] + ("COMPLETED", "0", "2026-09-02 08:00:00", "2026-09-01 08:00:00",
                                     "2026-09-02 09:00:00")
    services, units = refs(spark)
    valid, quarantine = transform_applications(raw(spark, [ok, bad_svc, bad_id, bad_time]), services, units, "s")
    reasons = {r.app_id: r.dq_errors for r in quarantine.collect()}
    assert valid.count() == 1
    assert reasons == {"HS2": ["unknown_service"], "HS3": ["invalid_citizen_id"],
                       "HS4": ["completed_before_submitted"]}


def test_pii_is_never_written_to_silver(spark):
    row = ("HS1", "001234567890", "Nguyễn Văn A", "0912345678", "TP001", "P01", "online", "RECEIVED", "0",
           "2026-09-01 08:00:00", "", "2026-09-01 08:00:00")
    services, units = refs(spark)
    valid, quarantine = transform_applications(raw(spark, [row]), services, units, "salt-1")
    for df in (valid, quarantine):
        assert not set(PII_RAW_COLUMNS) & set(df.columns)
    r = valid.first()
    assert r.phone_masked == "*******678"
    assert len(r.citizen_key) == 64 and "001234567890" not in r.citizen_key
    # cùng salt -> cùng khoá (join được); khác salt -> khác khoá
    v2, _ = transform_applications(raw(spark, [row]), services, units, "salt-2")
    assert v2.first().citizen_key != r.citizen_key
