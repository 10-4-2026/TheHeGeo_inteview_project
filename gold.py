"""GOLD: mô hình dữ liệu phục vụ phân tích & ML.

  dim_date, dim_service, dim_unit   ┐
  fact_application (grain: 1 hồ sơ) ┘  -> mô hình sao (Kimball)
  mart_unit_monthly_kpi              -> dashboard điều hành: tỷ lệ đúng hạn theo phường/tháng
  mart_service_kpi                   -> thủ tục nào hay trễ hạn, p50/p90 thời gian xử lý
  feature_late_risk                  -> feature dataset cho mô hình dự báo hồ sơ trễ hạn,
                                        tính point-in-time (chỉ dùng thông tin có TRƯỚC ngày nộp)
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from lakehouse.config import GOLD, SILVER

FINISHED = ["COMPLETED", "REJECTED"]


def date_key(col) -> F.Column:
    return F.date_format(col, "yyyyMMdd").cast("int")


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    return (
        spark.sql(f"SELECT explode(sequence(DATE'{start}', DATE'{end}', INTERVAL 1 DAY)) AS date")
        .select(
            date_key(F.col("date")).alias("date_key"), "date",
            F.year("date").alias("year"), F.quarter("date").alias("quarter"),
            F.month("date").alias("month"), F.dayofmonth("date").alias("day"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.dayofweek("date").alias("day_of_week"),            # 1=CN ... 7=T7
            F.dayofweek("date").isin(1, 7).alias("is_weekend"),
        )
    )


def build_fact(apps: DataFrame, services: DataFrame, as_of: date) -> DataFrame:
    as_of_end = F.to_timestamp(F.lit(f"{as_of} 23:59:59"))
    f = (
        apps.join(F.broadcast(services.select("service_code", "sla_days")), "service_code")
        .withColumn("due_at", F.expr("submitted_at + make_interval(0, 0, 0, sla_days, 0, 0, 0)"))
        .withColumn("is_finished", F.col("status").isin(FINISHED))
        .withColumn("processing_days",
                    F.when(F.col("is_finished"),
                           F.round((F.unix_timestamp("completed_at") - F.unix_timestamp("submitted_at")) / 86400, 2)))
        .withColumn("is_late", F.when(F.col("is_finished"), F.col("completed_at") > F.col("due_at")))
        .withColumn("is_overdue_open", ~F.col("is_finished") & (as_of_end > F.col("due_at")))
    )
    return f.select(
        "app_id", "citizen_key", "service_code", "unit_code", "channel", "status",
        F.coalesce("fee_vnd", F.lit(0)).alias("fee_vnd"),
        date_key(F.col("submitted_at")).alias("submitted_date_key"),
        date_key(F.col("completed_at")).alias("completed_date_key"),
        "submitted_at", "completed_at", "due_at", "sla_days",
        "processing_days", "is_finished", "is_late", "is_overdue_open",
        F.date_format("submitted_at", "yyyy-MM").alias("submitted_month"),
    )


def build_mart_unit_monthly(fact: DataFrame, units: DataFrame) -> DataFrame:
    return (
        fact.groupBy("submitted_month", "unit_code")
        .agg(
            F.count("*").alias("received"),
            F.sum(F.col("is_finished").cast("int")).alias("finished"),
            F.sum(F.when(F.col("is_finished") & ~F.col("is_late"), 1).otherwise(0)).alias("finished_on_time"),
            F.sum(F.col("is_overdue_open").cast("int")).alias("overdue_open"),
            F.round(F.avg("processing_days"), 2).alias("avg_processing_days"),
            F.round(F.avg((F.col("channel") == "ONLINE").cast("int")), 4).alias("online_rate"),
            F.sum("fee_vnd").alias("fee_collected_vnd"),
        )
        .withColumn("on_time_rate", F.round(F.col("finished_on_time") / F.col("finished"), 4))
        .join(units.select("unit_code", "unit_name"), "unit_code")
        .orderBy("submitted_month", "unit_code")
    )


def build_mart_service(fact: DataFrame, services: DataFrame) -> DataFrame:
    done = fact.filter("is_finished")
    return (
        done.groupBy("service_code")
        .agg(
            F.count("*").alias("finished"),
            F.round(F.avg(F.col("is_late").cast("int")), 4).alias("late_rate"),
            F.percentile_approx("processing_days", 0.5).alias("p50_processing_days"),
            F.percentile_approx("processing_days", 0.9).alias("p90_processing_days"),
        )
        .join(services.select("service_code", "service_name", "sector", "sla_days"), "service_code")
        .orderBy(F.desc("late_rate"))
    )


def build_late_risk_features(spark: SparkSession, fact: DataFrame, services: DataFrame,
                             window_days: int = 30) -> DataFrame:
    """Feature theo phường được tính trên cửa sổ [ngày nộp - window_days, ngày nộp - 1].

    Không dùng thông tin của chính ngày nộp hoặc tương lai -> tránh data leakage khi train,
    và cùng logic đó dùng được cho online scoring lúc hồ sơ vừa được tiếp nhận.
    """
    bounds = fact.agg(F.min(F.to_date("submitted_at")).alias("lo"), F.max(F.to_date("submitted_at")).alias("hi")).first()
    calendar = spark.sql(
        f"SELECT explode(sequence(DATE'{bounds['lo']}' - INTERVAL {window_days} DAYS, DATE'{bounds['hi']}', "
        f"INTERVAL 1 DAY)) AS d")
    grid = calendar.crossJoin(fact.select("unit_code").distinct())

    finished_daily = (fact.filter("is_finished")
                      .groupBy("unit_code", F.to_date("completed_at").alias("d"))
                      .agg(F.count("*").alias("n_finished"), F.sum(F.col("is_late").cast("int")).alias("n_late")))
    received_daily = (fact.groupBy("unit_code", F.to_date("submitted_at").alias("d"))
                      .agg(F.count("*").alias("n_received")))

    w = (Window.partitionBy("unit_code").orderBy("day_idx").rangeBetween(-window_days, -1))
    w7 = (Window.partitionBy("unit_code").orderBy("day_idx").rangeBetween(-7, -1))
    unit_day = (
        grid.join(finished_daily, ["unit_code", "d"], "left").join(received_daily, ["unit_code", "d"], "left")
        .fillna(0, ["n_finished", "n_late", "n_received"])
        .withColumn("day_idx", F.datediff("d", F.to_date(F.lit("1970-01-01"))))
        .withColumn("unit_finished_prev30d", F.sum("n_finished").over(w))
        .withColumn("unit_late_prev30d", F.sum("n_late").over(w))
        .withColumn("unit_received_prev7d", F.sum("n_received").over(w7))
        .withColumn("unit_late_rate_prev30d",
                    F.when(F.col("unit_finished_prev30d") > 0,
                           F.round(F.col("unit_late_prev30d") / F.col("unit_finished_prev30d"), 4)))
        .select("unit_code", "d", "unit_finished_prev30d", "unit_late_rate_prev30d", "unit_received_prev7d")
    )

    labeled = fact.filter("is_finished")   # chỉ hồ sơ đã có kết quả mới có nhãn (hồ sơ đang mở = censored)
    return (
        labeled.withColumn("d", F.to_date("submitted_at"))
        .join(unit_day, ["unit_code", "d"], "left")
        .join(F.broadcast(services.select("service_code", "sector", "service_level")), "service_code")
        .select(
            "app_id", "unit_code", "service_code", "sector", "service_level", "sla_days",
            (F.col("channel") == "ONLINE").cast("int").alias("is_online"),
            F.hour("submitted_at").alias("submit_hour"),
            F.dayofweek("submitted_at").alias("submit_dow"),
            "fee_vnd", "unit_finished_prev30d", "unit_late_rate_prev30d", "unit_received_prev7d",
            F.col("d").alias("submitted_date"),
            F.col("is_late").cast("int").alias("label_is_late"),
        )
    )


def run(spark: SparkSession, run_id: str, as_of: date) -> dict:
    apps = spark.read.parquet(str(SILVER / "applications"))
    services = spark.read.parquet(str(SILVER / "services"))
    units = spark.read.parquet(str(SILVER / "units"))

    fact = build_fact(apps, services, as_of).cache()
    lo_hi = fact.agg(F.min(F.to_date("submitted_at")), F.max(F.to_date("completed_at"))).first()
    end = max(d for d in [lo_hi[1], as_of] if d is not None)

    outputs = {
        "dim_date": build_dim_date(spark, lo_hi[0], end),
        "dim_service": services,
        "dim_unit": units,
        "mart_unit_monthly_kpi": build_mart_unit_monthly(fact, units),
        "mart_service_kpi": build_mart_service(fact, services),
        "feature_late_risk": build_late_risk_features(spark, fact, services),
    }
    fact.withColumn("_run_id", F.lit(run_id)).write.mode("overwrite") \
        .partitionBy("submitted_month").parquet(str(GOLD / "fact_application"))
    rows = {"gold.fact_application": fact.count()}
    for name, df in outputs.items():
        df.write.mode("overwrite").parquet(str(GOLD / name))
        rows[f"gold.{name}"] = spark.read.parquet(str(GOLD / name)).count()
    fact.unpersist()
    return {"inputs": [str(SILVER)], "outputs": [str(GOLD / n) for n in ["fact_application", *outputs]],
            "rows": rows}
