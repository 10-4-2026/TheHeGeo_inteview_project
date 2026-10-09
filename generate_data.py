"""Sinh dữ liệu GIẢ LẬP hồ sơ thủ tục hành chính cấp phường (Hà Nội).

Mô phỏng một hệ thống nguồn đẩy file CSV theo ngày (CDC kiểu đơn giản):
  - Ngày tiếp nhận: hồ sơ xuất hiện với status=RECEIVED
  - Ngày trả kết quả: cùng app_id xuất hiện lại với status=COMPLETED/REJECTED
Có cố ý chèn "dữ liệu bẩn" giống thực tế: nhiều định dạng ngày, kênh nộp viết
lung tung, phí dạng "50.000đ", bản ghi gửi trùng, mã dịch vụ sai, CCCD trống...

Toàn bộ tên, CCCD, SĐT, SLA, phí đều là dữ liệu bịa, không phải số liệu thật.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from collections import defaultdict
from datetime import date, datetime, time, timedelta

from lakehouse.config import LANDING

UNITS = [
    ("P01", "UBND phường Giảng Võ"),
    ("P02", "UBND phường Ba Đình"),
    ("P03", "UBND phường Hoàn Kiếm"),
    ("P04", "UBND phường Cửa Nam"),
    ("P05", "UBND phường Đống Đa"),
    ("P06", "UBND phường Văn Miếu - Quốc Tử Giám"),
    ("P07", "UBND phường Hai Bà Trưng"),
    ("P08", "UBND phường Bạch Mai"),
    ("P09", "UBND phường Cầu Giấy"),
    ("P10", "UBND phường Nghĩa Đô"),
    ("P11", "UBND phường Tây Hồ"),
    ("P12", "UBND phường Thanh Xuân"),
]

SERVICES = [
    {"service_code": "TP001", "service_name": "Đăng ký khai sinh", "sector": "Tư pháp - Hộ tịch", "level": 4, "sla_days": 1, "fee_vnd": 0, "weight": 14},
    {"service_code": "TP002", "service_name": "Đăng ký kết hôn", "sector": "Tư pháp - Hộ tịch", "level": 4, "sla_days": 3, "fee_vnd": 0, "weight": 5},
    {"service_code": "TP003", "service_name": "Chứng thực bản sao từ bản chính", "sector": "Tư pháp - Chứng thực", "level": 3, "sla_days": 1, "fee_vnd": 2000, "weight": 30},
    {"service_code": "TP004", "service_name": "Xác nhận tình trạng hôn nhân", "sector": "Tư pháp - Hộ tịch", "level": 4, "sla_days": 3, "fee_vnd": 0, "weight": 8},
    {"service_code": "KT001", "service_name": "Đăng ký hộ kinh doanh", "sector": "Kinh tế", "level": 4, "sla_days": 3, "fee_vnd": 100000, "weight": 9},
    {"service_code": "XD001", "service_name": "Cấp giấy phép xây dựng nhà ở riêng lẻ", "sector": "Xây dựng", "level": 3, "sla_days": 15, "fee_vnd": 75000, "weight": 4},
    {"service_code": "DD001", "service_name": "Đăng ký biến động đất đai", "sector": "Đất đai", "level": 3, "sla_days": 10, "fee_vnd": 50000, "weight": 6},
    {"service_code": "DD002", "service_name": "Cấp GCN quyền sử dụng đất lần đầu", "sector": "Đất đai", "level": 3, "sla_days": 20, "fee_vnd": 100000, "weight": 3},
    {"service_code": "XH001", "service_name": "Trợ cấp xã hội hằng tháng", "sector": "Lao động - Xã hội", "level": 3, "sla_days": 10, "fee_vnd": 0, "weight": 4},
    {"service_code": "XH002", "service_name": "Cấp thẻ BHYT cho trẻ dưới 6 tuổi", "sector": "Lao động - Xã hội", "level": 4, "sla_days": 5, "fee_vnd": 0, "weight": 7},
]

HO = ["Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Vũ", "Đặng", "Bùi", "Đỗ", "Ngô", "Dương", "Lý"]
DEM = ["Văn", "Thị", "Minh", "Thu", "Hoàng", "Ngọc", "Đức", "Thanh", "Quang", "Thảo"]
TEN = ["An", "Bình", "Châu", "Dũng", "Hà", "Hùng", "Lan", "Long", "Mai", "Nam", "Phương", "Quân", "Trang", "Tuấn", "Vy"]

CHANNEL_VARIANTS = {
    "ONLINE": ["online", "ONLINE", "Online", "Trực tuyến", "truc_tuyen"],
    "OFFLINE": ["offline", "OFFLINE", "Trực tiếp", "truc_tiep"],
}

FIELDS = ["app_id", "citizen_id", "full_name", "phone", "service_code", "unit_code", "channel",
          "status", "fee", "submitted_at", "completed_at", "updated_at"]


def fmt_ts(ts: datetime, rng: random.Random) -> str:
    r = rng.random()
    if r < 0.85:
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    if r < 0.95:
        return ts.strftime("%d/%m/%Y %H:%M")
    return ts.strftime("%Y-%m-%dT%H:%M:%S+07:00")


def fmt_fee(fee: int, rng: random.Random) -> str:
    if fee == 0:
        return rng.choice(["0", "0", ""])
    return rng.choice([str(fee), f"{fee:,}".replace(",", ".") + "đ", f"{fee} VND"])


def daily_volume(d: date, base: int, rng: random.Random) -> int:
    if d.weekday() == 6:
        return int(base * 0.03)
    if d.weekday() == 5:
        return int(base * 0.3 * rng.uniform(0.8, 1.2))
    return int(base * rng.uniform(0.8, 1.2))


def generate(start: date, end: date, base_daily: int = 220, seed: int = 42) -> dict:
    rng = random.Random(seed)
    unit_codes = [u for u, _ in UNITS]
    unit_weight = [rng.uniform(0.6, 1.6) for _ in UNITS]
    unit_speed = {u: rng.uniform(0.6, 1.45) for u in unit_codes}   # >1 = xử lý chậm hơn
    unit_online = {u: rng.uniform(0.3, 0.7) for u in unit_codes}
    svc_weight = [s["weight"] for s in SERVICES]

    batches: dict[date, list[dict]] = defaultdict(list)
    stats = defaultdict(int)
    seq = 0
    d = start
    while d <= end:
        for _ in range(daily_volume(d, base_daily, rng)):
            seq += 1
            unit = rng.choices(unit_codes, unit_weight)[0]
            svc = rng.choices(SERVICES, svc_weight)[0]
            submitted = datetime.combine(d, time(7, 30)) + timedelta(minutes=rng.randint(0, 600),
                                                                     seconds=rng.randint(0, 59))
            online_p = min(0.95, unit_online[unit] + 0.025 * (d.month - 1))  # chuyển đổi số tăng dần
            channel = "ONLINE" if rng.random() < online_p else "OFFLINE"

            factor = rng.lognormvariate(0, 0.35) * unit_speed[unit] * (0.85 if channel == "ONLINE" else 1.0)
            completed = submitted + timedelta(days=max(0.04, svc["sla_days"] * 0.75 * factor))

            row = {
                "app_id": f"HS{d:%Y%m%d}{seq:07d}",
                "citizen_id": f"0{rng.randint(1, 96):02d}{rng.randint(0, 999999999):09d}",
                "full_name": f"{rng.choice(HO)} {rng.choice(DEM)} {rng.choice(TEN)}",
                "phone": f"09{rng.randint(0, 99999999):08d}",
                "service_code": svc["service_code"],
                "unit_code": unit,
                "channel": rng.choice(CHANNEL_VARIANTS[channel]),
                "status": rng.choice(["RECEIVED", "RECEIVED", "received "]),
                "fee": fmt_fee(svc["fee_vnd"], rng),
                "submitted_at": fmt_ts(submitted, rng),
                "completed_at": "",
                "updated_at": fmt_ts(submitted, rng),
            }

            # --- chèn lỗi dữ liệu có chủ đích ---
            r = rng.random()
            if r < 0.004:
                row["service_code"] = "XX999"; stats["bad_service"] += 1
            elif r < 0.007:
                row["citizen_id"] = ""; stats["missing_citizen_id"] += 1
            elif r < 0.009:
                row["fee"] = "-50000"; stats["negative_fee"] += 1

            batches[d].append(row)
            if rng.random() < 0.01:                       # hệ thống nguồn gửi lại y hệt
                batches[d].append(dict(row)); stats["resent_duplicate"] += 1

            if completed.date() <= end:
                final = dict(row)
                final["status"] = "REJECTED" if rng.random() < 0.04 else "COMPLETED"
                done_at = completed
                if rng.random() < 0.002:                  # lỗi đồng hồ hệ thống nguồn
                    done_at = submitted - timedelta(days=1); stats["completed_before_submitted"] += 1
                final["completed_at"] = fmt_ts(done_at, rng)
                final["updated_at"] = fmt_ts(completed, rng)
                batches[completed.date()].append(final)
        d += timedelta(days=1)

    stats["applications"] = seq
    return {"batches": batches, "stats": dict(stats)}


def write_landing(result: dict) -> None:
    app_dir = LANDING / "applications"
    ref_dir = LANDING / "reference"
    app_dir.mkdir(parents=True, exist_ok=True)
    ref_dir.mkdir(parents=True, exist_ok=True)

    for d, rows in sorted(result["batches"].items()):
        with open(app_dir / f"applications_{d:%Y%m%d}.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)

    services = [{k: v for k, v in s.items() if k != "weight"} for s in SERVICES]
    (ref_dir / "services.json").write_text(json.dumps(services, ensure_ascii=False, indent=2), encoding="utf-8")
    with open(ref_dir / "units.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["unit_code", "unit_name", "province"])
        for code, name in UNITS:
            w.writerow([code, name, "Hà Nội"])


def main() -> None:
    p = argparse.ArgumentParser(description="Sinh dữ liệu giả lập vào data/landing")
    p.add_argument("--start", default="2026-01-01")
    p.add_argument("--end", default="2026-09-30")
    p.add_argument("--base-daily", type=int, default=220)
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    result = generate(date.fromisoformat(a.start), date.fromisoformat(a.end), a.base_daily, a.seed)
    write_landing(result)
    n_rows = sum(len(v) for v in result["batches"].values())
    print(f"Đã ghi {len(result['batches'])} file batch, {n_rows:,} dòng vào {LANDING}")
    print("Lỗi đã chèn:", json.dumps(result["stats"], ensure_ascii=False))


if __name__ == "__main__":
    main()
