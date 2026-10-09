PY ?= python
export PYTHONPATH := src
export LAKEHOUSE_PII_SALT ?= dev-only-salt
AS_OF ?= 2026-09-30

.PHONY: setup data pipeline model sql test all clean

setup:          ## cài thư viện (cần Java 11/17/21)
	$(PY) -m pip install -r requirements.txt

data:           ## sinh dữ liệu giả lập vào data/landing
	$(PY) -m lakehouse.generate_data

pipeline:       ## Bronze -> Silver -> DQ gate -> Gold -> dashboard
	$(PY) -m lakehouse.run_pipeline --stage all --as-of $(AS_OF)

model:          ## baseline ML trên feature dataset
	$(PY) -m lakehouse.train_baseline

sql:            ## chạy truy vấn mẫu trên Gold
	$(PY) -m lakehouse.run_sql

test:
	$(PY) -m pytest -q

all: data pipeline model

clean:
	rm -rf data reports/*.json reports/*.jsonl reports/*.html
