"""BRONZE: nạp file thô vào lakehouse, KHÔNG sửa nội dung.

- Mọi cột giữ kiểu string (schema-on-read) để không mất dữ liệu khi nguồn đổi định dạng.
- Thêm metadata: batch_date, _source_file, _ingested_at, _run_id -> truy vết (lineage/audit).
- Ghi theo partition batch_date với dynamic overwrite -> chạy lại 1 ngày là idempotent.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.config import BRONZE, LANDING
from lakehouse.generate_data import FIELDS

RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in FIELDS])


def read_landing_applications(spark: SparkSession, batch_date: date | None) -> DataFrame:
    pattern = f"applications_{batch_date:%Y%m%d}.csv" if batch_date else "applications_*.csv"
    return (
        spark.read.option("header", True)
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(str(LANDING / "applications" / pattern))
    )


def add_ingest_metadata(df: DataFrame, run_id: str) -> DataFrame:
    return (
        df.withColumn("_source_file", F.input_file_name())
        .withColumn("batch_date",
                    F.to_date(F.regexp_extract("_source_file", r"applications_(\d{8})\.csv", 1), "yyyyMMdd"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )


def run(spark: SparkSession, run_id: str, batch_date: date | None = None) -> dict:
    apps = add_ingest_metadata(read_landing_applications(spark, batch_date), run_id)
    apps.write.mode("overwrite").partitionBy("batch_date").parquet(str(BRONZE / "applications"))

    # Bảng tham chiếu nhỏ: snapshot đầy đủ mỗi lần chạy
    ref = LANDING / "reference"
    services = spark.read.option("multiLine", True).json(str(ref / "services.json"))
    units = spark.read.option("header", True).csv(str(ref / "units.csv"))
    for name, df in [("services", services), ("units", units)]:
        (df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_run_id", F.lit(run_id))
           .write.mode("overwrite").parquet(str(BRONZE / "reference" / name)))

    written = spark.read.parquet(str(BRONZE / "applications"))
    if batch_date:
        written = written.filter(F.col("batch_date") == F.lit(batch_date))
    return {
        "inputs": [str(LANDING / "applications"), str(ref)],
        "outputs": [str(BRONZE / "applications"), str(BRONZE / "reference")],
        "rows": {"bronze.applications(batch)": written.count(),
                 "bronze.services": services.count(), "bronze.units": units.count()},
    }
