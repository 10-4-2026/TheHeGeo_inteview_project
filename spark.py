from __future__ import annotations

import os

from pyspark.sql import SparkSession

from lakehouse.config import TIMEZONE


def get_spark(app_name: str = "gov-lakehouse") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", TIMEZONE)
        # chỉ ghi đè partition của batch đang chạy -> chạy lại (re-run) an toàn
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
