"""Chạy các câu truy vấn trong sql/analytics.sql trên lớp Gold."""
from __future__ import annotations

import sys

from lakehouse.config import GOLD, ROOT
from lakehouse.spark import get_spark

VIEWS = {"fact": "fact_application", "dim_date": "dim_date", "dim_unit": "dim_unit", "dim_service": "dim_service"}


def main(path: str = str(ROOT / "sql" / "analytics.sql")) -> None:
    spark = get_spark("gov-lakehouse-sql")
    for view, table in VIEWS.items():
        spark.read.parquet(str(GOLD / table)).createOrReplaceTempView(view)
    text = open(path, encoding="utf-8").read()
    for stmt in [s.strip() for s in text.split(";") if s.strip()]:
        comments = [l for l in stmt.splitlines() if l.startswith("--")]
        title = comments[-1] if comments else ""
        sql = "\n".join(l for l in stmt.splitlines() if not l.startswith("--")).strip()
        if sql:
            print(title)
            spark.sql(sql).show(truncate=False)


if __name__ == "__main__":
    main(*sys.argv[1:])
