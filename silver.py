"""SILVER: làm sạch, chuẩn hoá, khử trùng lặp, kiểm tra chất lượng từng bản ghi, che PII.

Luồng: bronze -> chuẩn hoá kiểu dữ liệu -> giữ phiên bản mới nhất của mỗi app_id
       -> gắn danh sách lỗi DQ -> băm/che PII -> tách 2 bảng: applications (hợp lệ)
       và quarantine (lỗi, kèm lý do) để nghiệp vụ xử lý lại thay vì âm thầm bỏ đi.
Các hàm transform là hàm thuần (DataFrame -> DataFrame) để unit test được.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.config import BRONZE, SILVER, pii_salt

TS_FORMATS = ["yyyy-MM-dd HH:mm:ss", "dd/MM/yyyy HH:mm", "yyyy-MM-dd'T'HH:mm:ssXXX"]
VALID_STATUS = ["RECEIVED", "COMPLETED", "REJECTED"]
PII_RAW_COLUMNS = ["citizen_id", "full_name", "phone"]


# ---------- hàm chuẩn hoá cột ----------
def parse_ts(col: Column) -> Column:
    trimmed = F.trim(col)
    return F.coalesce(*[F.to_timestamp(trimmed, fmt) for fmt in TS_FORMATS])


def standardize_channel(col: Column) -> Column:
    norm = F.lower(F.trim(col))
    return (F.when(norm.isin("online", "trực tuyến", "truc_tuyen"), "ONLINE")
             .when(norm.isin("offline", "trực tiếp", "truc_tiep"), "OFFLINE"))


def parse_fee(col: Column) -> Column:
    digits = F.regexp_replace(col, r"[^0-9\-]", "")
    return F.when(digits == "", None).otherwise(digits).cast("long")


def clean(df: DataFrame) -> DataFrame:
    return df.select(
        F.trim("app_id").alias("app_id"),
        F.trim("citizen_id").alias("citizen_id"),
        F.trim("full_name").alias("full_name"),
        F.trim("phone").alias("phone"),
        F.upper(F.trim("service_code")).alias("service_code"),
        F.upper(F.trim("unit_code")).alias("unit_code"),
        standardize_channel(F.col("channel")).alias("channel"),
        F.upper(F.trim("status")).alias("status"),
        parse_fee(F.col("fee")).alias("fee_vnd"),
        parse_ts(F.col("submitted_at")).alias("submitted_at"),
        parse_ts(F.col("completed_at")).alias("completed_at"),
        parse_ts(F.col("updated_at")).alias("updated_at"),
        (F.coalesce(F.trim("completed_at"), F.lit("")) != "").alias("_has_completed_raw"),
        "batch_date", "_source_file", "_ingested_at",
    )


def latest_version(df: DataFrame) -> DataFrame:
    """Mỗi app_id giữ bản ghi mới nhất (CDC: RECEIVED -> COMPLETED), loại luôn bản gửi trùng."""
    w = Window.partitionBy("app_id").orderBy(F.col("updated_at").desc_nulls_last(),
                                             F.col("_ingested_at").desc(), F.col("batch_date").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def attach_dq_errors(df: DataFrame, services: DataFrame, units: DataFrame) -> DataFrame:
    svc = F.broadcast(services.select("service_code", F.lit(True).alias("_svc_ok")))
    unit = F.broadcast(units.select("unit_code", F.lit(True).alias("_unit_ok")))
    df = df.join(svc, "service_code", "left").join(unit, "unit_code", "left")

    rules = {
        "missing_app_id": F.col("app_id").isNull() | (F.col("app_id") == ""),
        "invalid_citizen_id": ~F.coalesce(F.col("citizen_id").rlike(r"^\d{12}$"), F.lit(False)),
        "unknown_service": F.col("_svc_ok").isNull(),
        "unknown_unit": F.col("_unit_ok").isNull(),
        "unknown_channel": F.col("channel").isNull(),
        "unknown_status": ~F.coalesce(F.col("status").isin(VALID_STATUS), F.lit(False)),
        "unparseable_submitted_at": F.col("submitted_at").isNull(),
        "unparseable_completed_at": F.col("_has_completed_raw") & F.col("completed_at").isNull(),
        "completed_before_submitted": F.coalesce(F.col("completed_at") < F.col("submitted_at"), F.lit(False)),
        "negative_fee": F.coalesce(F.col("fee_vnd") < 0, F.lit(False)),
    }
    errors = F.array(*[F.when(cond, F.lit(name)) for name, cond in rules.items()])
    return (df.withColumn("dq_errors", F.filter(errors, lambda x: x.isNotNull()))
              .drop("_svc_ok", "_unit_ok", "_has_completed_raw"))


def mask_pii(df: DataFrame, salt: str) -> DataFrame:
    """CCCD -> khoá băm (sha256 + salt) vẫn join/đếm công dân được; SĐT chỉ giữ 3 số cuối; bỏ họ tên."""
    valid_id = F.coalesce(F.col("citizen_id").rlike(r"^\d{12}$"), F.lit(False))
    return (
        df.withColumn("citizen_key",
                      F.when(valid_id, F.sha2(F.concat(F.lit(salt), F.col("citizen_id")), 256)))
          .withColumn("phone_masked",
                      F.when(F.length("phone") >= 3, F.concat(F.lit("*******"), F.substring("phone", -3, 3))))
          .drop(*PII_RAW_COLUMNS)
    )


SILVER_COLUMNS = ["app_id", "citizen_key", "phone_masked", "service_code", "unit_code", "channel",
                  "status", "fee_vnd", "submitted_at", "completed_at", "updated_at",
                  "batch_date", "_source_file", "_ingested_at"]


def transform_applications(bronze_apps: DataFrame, services: DataFrame, units: DataFrame,
                           salt: str) -> tuple[DataFrame, DataFrame]:
    checked = attach_dq_errors(latest_version(clean(bronze_apps)), services, units)
    masked = mask_pii(checked, salt)
    valid = masked.filter(F.size("dq_errors") == 0).select(*SILVER_COLUMNS)
    quarantine = masked.filter(F.size("dq_errors") > 0).select(*SILVER_COLUMNS, "dq_errors")
    return valid, quarantine


def transform_reference(services_raw: DataFrame, units_raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    services = services_raw.select(
        F.upper(F.trim("service_code")).alias("service_code"),
        F.trim("service_name").alias("service_name"),
        F.trim("sector").alias("sector"),
        F.col("level").cast("int").alias("service_level"),
        F.col("sla_days").cast("int").alias("sla_days"),
        F.col("fee_vnd").cast("long").alias("standard_fee_vnd"),
    ).dropDuplicates(["service_code"])
    units = units_raw.select(
        F.upper(F.trim("unit_code")).alias("unit_code"),
        F.trim("unit_name").alias("unit_name"),
        F.trim("province").alias("province"),
    ).dropDuplicates(["unit_code"])
    return services, units


def run(spark: SparkSession, run_id: str) -> dict:
    services, units = transform_reference(spark.read.parquet(str(BRONZE / "reference" / "services")),
                                          spark.read.parquet(str(BRONZE / "reference" / "units")))
    services.write.mode("overwrite").parquet(str(SILVER / "services"))
    units.write.mode("overwrite").parquet(str(SILVER / "units"))

    # Silver được build lại toàn bộ từ Bronze: tất định & idempotent.
    # (Production: Delta/Iceberg MERGE INTO theo app_id để xử lý incremental.)
    bronze_apps = spark.read.parquet(str(BRONZE / "applications"))
    valid, quarantine = transform_applications(bronze_apps, services, units, pii_salt())
    valid = valid.withColumn("_run_id", F.lit(run_id))
    quarantine = quarantine.withColumn("_run_id", F.lit(run_id))
    valid.write.mode("overwrite").parquet(str(SILVER / "applications"))
    quarantine.write.mode("overwrite").parquet(str(SILVER / "quarantine" / "applications"))

    n_valid = spark.read.parquet(str(SILVER / "applications")).count()
    n_quar = spark.read.parquet(str(SILVER / "quarantine" / "applications")).count()
    return {
        "inputs": [str(BRONZE / "applications"), str(BRONZE / "reference")],
        "outputs": [str(SILVER / "applications"), str(SILVER / "quarantine" / "applications"),
                    str(SILVER / "services"), str(SILVER / "units")],
        "rows": {"bronze.applications(all)": bronze_apps.count(),
                 "silver.applications": n_valid, "silver.quarantine": n_quar},
    }
