"""Baseline ML trên gold.feature_late_risk — chứng minh feature dataset dùng được cho Data Science.

Chia train/test THEO THỜI GIAN (train: trước tháng 8, test: tháng 8-9) thay vì random split,
để mô phỏng đúng cách mô hình được dùng trong thực tế (học quá khứ, dự báo tương lai).
"""
from __future__ import annotations

import json

import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from lakehouse.config import GOLD, REPORTS

CATEGORICAL = ["unit_code", "service_code", "sector"]
NUMERIC = ["service_level", "sla_days", "is_online", "submit_hour", "submit_dow", "fee_vnd",
           "unit_finished_prev30d", "unit_late_rate_prev30d", "unit_received_prev7d"]
SPLIT_DATE = "2026-08-01"


def main() -> None:
    df = pd.read_parquet(GOLD / "feature_late_risk")
    df["submitted_date"] = pd.to_datetime(df["submitted_date"])
    train, test = df[df.submitted_date < SPLIT_DATE], df[df.submitted_date >= SPLIT_DATE]

    model = Pipeline([
        ("prep", ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL)],
                                   remainder="passthrough")),
        ("clf", HistGradientBoostingClassifier(max_iter=200, learning_rate=0.08, random_state=0)),
    ])
    X = CATEGORICAL + NUMERIC
    model.fit(train[X], train["label_is_late"])
    proba = model.predict_proba(test[X])[:, 1]

    result = {
        "train_rows": len(train), "test_rows": len(test), "split_date": SPLIT_DATE,
        "test_late_rate": round(float(test["label_is_late"].mean()), 4),
        "roc_auc": round(float(roc_auc_score(test["label_is_late"], proba)), 4),
        "pr_auc": round(float(average_precision_score(test["label_is_late"], proba)), 4),
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "baseline_model.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
