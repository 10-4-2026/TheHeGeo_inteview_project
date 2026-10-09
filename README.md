# gov-lakehouse — Mini Lakehouse cho dữ liệu dịch vụ công

Project demo cho vị trí **Data Engineer / Analytics Engineer**: xây dựng pipeline Lakehouse
**Bronze → Silver → Gold** bằng **PySpark**, điều phối bằng **Airflow**, có **Data Quality gate**,
**Data Governance** (che PII, data catalog, lineage), **data mart + dashboard** và **feature dataset cho ML**.

Bài toán (GovTech): theo dõi việc giải quyết hồ sơ thủ tục hành chính tại 12 phường ở Hà Nội —
phường nào trễ hạn nhiều, thủ tục nào hay quá SLA, tỷ lệ nộp trực tuyến tăng thế nào, và dự báo
sớm hồ sơ có nguy cơ trễ hạn.

> Toàn bộ dữ liệu là **giả lập** (tên, CCCD, SĐT, SLA, phí đều bịa), sinh bởi `generate_data.py`.

---

## Kiến trúc

```
 Hệ thống nguồn (một cửa điện tử)           Lakehouse (Parquet, partitioned)                   Tiêu thụ
 ───────────────────────────────           ─────────────────────────────────                  ──────────
 applications_YYYYMMDD.csv  ─┐   BRONZE                SILVER                      GOLD
 (CDC: RECEIVED → COMPLETED) │   raw, string,          làm sạch, chuẩn hoá,        dim_date / dim_service / dim_unit
 services.json  ─────────────┼─► + batch_date,   ──►   dedupe (bản mới nhất),  ──► fact_application ─────────► Dashboard
 units.csv ──────────────────┘   _source_file,         DQ từng bản ghi,            mart_unit_monthly_kpi ────► SQL / BI
                                 _ingested_at          băm/che PII                 mart_service_kpi
                                                         │                         feature_late_risk ───────► Baseline ML
                                                         ├─► quarantine (+dq_errors)
                                                         └─► DQ gate (dataset-level) ── FAIL ⇒ dừng, không build Gold
                Airflow: wait_for_file → bronze → silver → quality_gate → gold → dashboard   (+ lineage.jsonl mỗi stage)
```

| Lớp | Nội dung | Kỹ thuật thể hiện |
|---|---|---|
| **Bronze** | Bản sao thô, mọi cột string, thêm metadata ingest | schema-on-read, partition `batch_date`, dynamic partition overwrite ⇒ **re-run idempotent** |
| **Silver** | Parse 3 định dạng ngày, chuẩn hoá kênh nộp/phí/trạng thái, giữ phiên bản mới nhất mỗi `app_id`, 10 rule DQ, **băm CCCD + che SĐT** | window function, broadcast join, quarantine thay vì xoá âm thầm |
| **Gold** | Star schema + 2 data mart + feature dataset | Kimball modelling, KPI SLA, **point-in-time features** (chống leakage) |

## Chạy thử

Yêu cầu: Python 3.9+ và Java 11/17/21 (đã chạy thử trên Python 3.9 và 3.13).

```bash
python -m venv .venv && source .venv/bin/activate
make setup          # cài pyspark, pandas, sklearn, pytest
make all            # sinh dữ liệu -> pipeline -> baseline model  (~1 phút trên laptop)
make test           # 7 unit test
make sql            # truy vấn mẫu trên Gold
open reports/dashboard.html
```

Chạy từng stage (giống Airflow gọi):

```bash
PYTHONPATH=src python -m lakehouse.run_pipeline --stage bronze --batch-date 2026-09-30
PYTHONPATH=src python -m lakehouse.run_pipeline --stage quality --as-of 2026-09-30
```

## Kết quả một lần chạy (as-of 2026-09-30)

| Bước | Số dòng |
|---|---|
| Landing / Bronze | 91.806 dòng từ 273 file ngày (≈45,9 nghìn hồ sơ, mỗi hồ sơ xuất hiện 1–2 lần + bản gửi trùng) |
| Silver hợp lệ | 45.410 hồ sơ (mỗi `app_id` đúng 1 dòng) |
| Quarantine | 525 bản ghi (1,1%) — khớp **đúng** số lỗi đã chèn: 174 mã thủ tục sai, 158 CCCD trống, 102 phí âm, 91 thời điểm hoàn thành < tiếp nhận |
| Gold fact / feature | 45.410 / 44.879 dòng |
| DQ gate | 7/7 check đạt |
| Baseline (HistGradientBoosting, chia theo thời gian) | ROC-AUC 0,80 · PR-AUC 0,35 (tỷ lệ trễ thực tế ở tập test 14%) |

Các file sinh ra: `reports/dq_report.json`, `reports/lineage.jsonl`, `reports/dashboard.html`, `reports/baseline_model.json`.

## Cấu trúc thư mục

```
src/lakehouse/
  generate_data.py   sinh dữ liệu giả lập + chèn lỗi có chủ đích
  bronze.py          nạp thô + metadata
  silver.py          làm sạch, dedupe, rule DQ bản ghi, che PII
  quality.py         check mức dataset + quality gate
  gold.py            star schema, data mart, feature dataset point-in-time
  dashboard.py       dashboard HTML từ Gold
  train_baseline.py  mô hình baseline trên feature dataset
  run_pipeline.py    CLI điều phối + ghi lineage
dags/gov_lakehouse_dag.py   Airflow DAG (tương thích 2.x & 3.x, đã load thử trên 3.3)
governance/data_catalog.yaml owner, phân loại dữ liệu, PII, retention, lineage
sql/analytics.sql           truy vấn phân tích mẫu (join star schema, window function)
tests/                      unit test transform Silver/Gold
```

## Map với JD

| Yêu cầu trong JD | Ở đâu trong project |
|---|---|
| Data Pipeline & Lakehouse | `bronze.py` → `silver.py` → `gold.py`, `run_pipeline.py` |
| Thiết kế Bronze/Silver/Gold | Bảng ở trên; mỗi lớp có hợp đồng rõ ràng (raw / clean / business-ready) |
| Data Mart, Feature Dataset | `mart_unit_monthly_kpi`, `mart_service_kpi`, `feature_late_risk` |
| Data Quality | Rule bản ghi + quarantine (`silver.py`), check dataset + gate (`quality.py`) |
| Phân tích & dashboard | `dashboard.py`, `sql/analytics.sql` |
| SQL, Python, Spark | PySpark DataFrame API + Spark SQL |
| Airflow | `dags/gov_lakehouse_dag.py` |
| Data Modeling | Star schema: 3 dim + 1 fact, grain ghi rõ trong catalog |
| Data Governance | Băm CCCD có salt, che SĐT, bỏ họ tên; `data_catalog.yaml`; `lineage.jsonl`; test đảm bảo PII không lọt xuống Silver |
| Điểm cộng AI/LLM, GovTech | Feature dataset + baseline ML; bài toán dịch vụ công |

## Các quyết định thiết kế (để trình bày khi phỏng vấn)

1. **Bronze giữ string thô.** Nếu nguồn đổi định dạng ngày, Bronze vẫn nạp được, lỗi chỉ hiện ở Silver
   (vào quarantine) — không mất dữ liệu, có thể replay.
2. **Idempotency.** Bronze ghi theo partition `batch_date` với dynamic overwrite: chạy lại / backfill
   một ngày không tạo trùng (đã kiểm tra: chạy 2 lần cùng ngày, tổng số dòng không đổi).
3. **CDC đơn giản.** Một hồ sơ xuất hiện ở batch ngày tiếp nhận (RECEIVED) và batch ngày trả kết quả
   (COMPLETED). Silver giữ bản mới nhất theo `updated_at` bằng `row_number()`.
4. **Quarantine thay vì drop.** Bản ghi lỗi giữ lại kèm `dq_errors` để gửi đơn vị nguồn sửa —
   quan trọng với dữ liệu hành chính, không được "mất" hồ sơ của dân.
5. **Quality gate.** Rule mức `error` vi phạm ⇒ exit code 1 ⇒ Airflow dừng, dashboard không hiển thị số sai.
   Thử: `LAKEHOUSE_MAX_QUARANTINE_RATIO=0.001 make pipeline`.
6. **PII.** CCCD → `sha256(salt + CCCD)` (pseudonymization: vẫn đếm/join theo công dân được, salt nằm
   ở secret manager); SĐT chỉ giữ 3 số cuối; họ tên bị loại từ Silver trở đi.
7. **Point-in-time features.** Tỷ lệ trễ của phường được tính trên cửa sổ `[ngày nộp − 30, ngày nộp − 1]`
   (`rangeBetween(-30, -1)` trên lịch đầy đủ) ⇒ không dùng thông tin tương lai khi train, và cùng logic
   dùng được cho scoring lúc vừa tiếp nhận hồ sơ. Có unit test riêng cho điều này.
8. **Chia train/test theo thời gian**, không random — đúng với cách mô hình được dùng thực tế.

## Giới hạn & hướng mở rộng

- **Parquet thuần → Delta Lake / Apache Iceberg**: ACID, `MERGE INTO` cho Silver incremental
  (hiện Silver build lại toàn bộ từ Bronze — đơn giản, tất định, đủ cho vài trăm nghìn dòng),
  time travel, schema evolution, `OPTIMIZE` gom file nhỏ (Bronze hiện có 1 file/ngày).
- **SLA tính theo ngày lịch**; thực tế là ngày làm việc ⇒ cần bảng ngày nghỉ lễ trong `dim_date`.
- **DQ**: chuyển rule sang Great Expectations / Soda / dbt tests; gửi cảnh báo Slack/Email khi gate fail.
- **Governance**: đẩy catalog vào DataHub/OpenMetadata, lineage chuẩn OpenLineage (Airflow có sẵn
  provider), phân quyền theo cột với Unity Catalog / Ranger.
- **Hạ tầng**: Spark on Kubernetes (`SparkKubernetesOperator`), MinIO/S3 làm object storage,
  Trino cho SQL, Superset cho dashboard.
- **Dimension SCD Type 2** cho danh mục thủ tục (SLA có thể thay đổi theo văn bản mới).
