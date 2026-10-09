"""Sinh dashboard HTML tĩnh (1 file, mở bằng trình duyệt) từ các bảng Gold + DQ report.

Trong production sẽ trỏ Superset / Metabase / Power BI vào Gold; file này để demo nhanh.
"""
from __future__ import annotations

import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse.config import GOLD, REPORTS


def collect(spark: SparkSession) -> dict:
    fact = spark.read.parquet(str(GOLD / "fact_application"))
    unit_m = spark.read.parquet(str(GOLD / "mart_unit_monthly_kpi"))
    svc = spark.read.parquet(str(GOLD / "mart_service_kpi"))

    kpi = fact.agg(
        F.count("*").alias("received"),
        F.sum(F.col("is_finished").cast("int")).alias("finished"),
        F.avg(F.when(F.col("is_finished"), (~F.col("is_late")).cast("int"))).alias("on_time_rate"),
        F.avg("processing_days").alias("avg_days"),
        F.avg((F.col("channel") == "ONLINE").cast("int")).alias("online_rate"),
        F.sum(F.col("is_overdue_open").cast("int")).alias("overdue_open"),
    ).first().asDict()

    monthly = (unit_m.groupBy("submitted_month")
               .agg(F.sum("received").alias("received"), F.sum("finished").alias("finished"),
                    F.sum("finished_on_time").alias("on_time"),
                    (F.sum(F.col("online_rate") * F.col("received")) / F.sum("received")).alias("online_rate"))
               .withColumn("on_time_rate", F.col("on_time") / F.col("finished"))
               .orderBy("submitted_month").toPandas())
    by_unit = (unit_m.groupBy("unit_code", "unit_name")
               .agg(F.sum("received").alias("received"), F.sum("finished_on_time").alias("on_time"),
                    F.sum("finished").alias("finished"), F.sum("overdue_open").alias("overdue_open"))
               .withColumn("on_time_rate", F.col("on_time") / F.col("finished"))
               .orderBy(F.desc("on_time_rate")).toPandas())
    services = svc.toPandas()

    dq = json.loads((REPORTS / "dq_report.json").read_text(encoding="utf-8"))
    return {
        "kpi": {k: (float(v) if v is not None else None) for k, v in kpi.items()},
        "monthly": monthly.to_dict(orient="list"),
        "units": by_unit.to_dict(orient="records"),
        "services": services.to_dict(orient="records"),
        "dq": dq,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
    }


TEMPLATE = r"""<!doctype html>
<html lang="vi"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dịch vụ công KPI</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root{color-scheme:light;--bg:#f4f4f2;--surface:#fcfcfb;--border:#e4e3df;--text:#0b0b0b;--text2:#52514e;--muted:#8a8984;
--s1:#2a78d6;--good:#1b7f3b;--bad:#c4302b;--warn:#a86b00;--grid:#ecebe7}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){color-scheme:dark;--bg:#111110;--surface:#1a1a19;--border:#2c2c2a;
--text:#fff;--text2:#c3c2b7;--muted:#8f8e86;--s1:#3987e5;--good:#4cbb6c;--bad:#ef6b66;--warn:#e0a63a;--grid:#262624}}
:root[data-theme="dark"]{color-scheme:dark;--bg:#111110;--surface:#1a1a19;--border:#2c2c2a;--text:#fff;--text2:#c3c2b7;--muted:#8f8e86;
--s1:#3987e5;--good:#4cbb6c;--bad:#ef6b66;--warn:#e0a63a;--grid:#262624}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
main{max-width:1180px;margin:0 auto;padding:24px 16px 48px}
header h1{font-size:22px;margin:0 0 4px}header p{margin:0;color:var(--text2)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:20px 0}
.tile,.card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px 16px}
.tile .label{color:var(--text2);font-size:12px}.tile .value{font-size:26px;font-weight:650;font-variant-numeric:tabular-nums}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(340px,1fr));gap:12px;margin-bottom:12px}
.card h2{font-size:15px;margin:0 0 2px}.card .sub{color:var(--text2);font-size:12px;margin:0 0 10px}
.chart{position:relative;height:260px}.chart.tall{height:380px}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums;font-size:13px}
th,td{padding:7px 8px;border-bottom:1px solid var(--border);text-align:right}th:first-child,td:first-child{text-align:left}
th{color:var(--text2);font-weight:600}.scroll{overflow-x:auto}
.pill{display:inline-block;padding:1px 8px;border-radius:999px;font-size:12px;font-weight:600;border:1px solid currentColor}
.ok{color:var(--good)}.fail{color:var(--bad)}.warnc{color:var(--warn)}
footer{color:var(--muted);font-size:12px;margin-top:20px}
</style></head><body><main>
<header><h1>Giám sát xử lý hồ sơ thủ tục hành chính</h1>
<p>Dữ liệu giả lập · 12 phường · lớp Gold của mini-lakehouse · cập nhật __GEN__</p></header>
<section class="tiles" id="tiles"></section>
<section class="grid2">
 <div class="card"><h2>Hồ sơ tiếp nhận theo tháng</h2><p class="sub">Số hồ sơ</p><div class="chart"><canvas id="cVol"></canvas></div></div>
 <div class="card"><h2>Tỷ lệ giải quyết đúng hạn theo tháng</h2><p class="sub">% hồ sơ đã xử lý xong trong SLA</p><div class="chart"><canvas id="cOnTime"></canvas></div></div>
</section>
<section class="grid2">
 <div class="card"><h2>Tỷ lệ đúng hạn theo phường</h2><p class="sub">Toàn kỳ, sắp xếp giảm dần</p><div class="chart tall"><canvas id="cUnit"></canvas></div></div>
 <div class="card"><h2>Tỷ lệ nộp trực tuyến theo tháng</h2><p class="sub">% hồ sơ nộp qua cổng dịch vụ công</p><div class="chart"><canvas id="cOnline"></canvas></div></div>
</section>
<section class="card" style="margin-bottom:12px"><h2>Thời gian xử lý theo thủ tục</h2><p class="sub">p50/p90 (ngày) so với SLA — thủ tục trễ nhiều nhất ở trên</p>
<div class="scroll"><table id="tSvc"></table></div></section>
<section class="grid2">
 <div class="card"><h2>Data Quality gate</h2><p class="sub" id="dqSub"></p><div class="scroll"><table id="tDq"></table></div></div>
 <div class="card"><h2>Bản ghi bị cách ly (quarantine)</h2><p class="sub">Theo lý do — chuyển lại đơn vị nguồn xử lý</p><div class="scroll"><table id="tQ"></table></div></div>
</section>
<footer>Nguồn: gold.fact_application, gold.mart_unit_monthly_kpi, gold.mart_service_kpi, reports/dq_report.json</footer>
</main>
<script>
const D = __DATA__;
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const pct = v => v==null ? '—' : (v*100).toFixed(1)+'%';
const num = v => v==null ? '—' : Math.round(v).toLocaleString('vi-VN');
const k = D.kpi;
const tiles = [["Hồ sơ tiếp nhận", num(k.received)], ["Đã giải quyết", num(k.finished)],
  ["Đúng hạn", pct(k.on_time_rate)], ["TB ngày xử lý", k.avg_days.toFixed(2)],
  ["Nộp trực tuyến", pct(k.online_rate)], ["Đang quá hạn", num(k.overdue_open)]];
document.getElementById('tiles').innerHTML = tiles.map(([l,v])=>`<div class="tile"><div class="label">${l}</div><div class="value">${v}</div></div>`).join('');

const charts = [];
function base(yFmt, yMin, yMax){
  return {responsive:true, maintainAspectRatio:false, animation:false,
    interaction:{mode:'index', intersect:false},
    plugins:{legend:{display:false}, tooltip:{callbacks:{label:c=>' '+yFmt(c.parsed.y ?? c.parsed.x)}}},
    scales:{x:{grid:{display:false}, ticks:{color:css('--text2')}, border:{color:css('--border')}},
            y:{min:yMin, max:yMax, grid:{color:css('--grid')}, border:{display:false},
               ticks:{color:css('--text2'), callback:v=>yFmt(v)}}}};
}
function draw(){
  charts.forEach(c=>c.destroy()); charts.length=0;
  const s1 = css('--s1'), surf = css('--surface');
  const m = D.monthly;
  charts.push(new Chart(cVol, {type:'bar', data:{labels:m.submitted_month, datasets:[{data:m.received,
    backgroundColor:s1, borderRadius:{topLeft:4,topRight:4}, borderSkipped:'bottom', maxBarThickness:28}]},
    options:base(num, 0)}));
  charts.push(new Chart(cOnTime, {type:'line', data:{labels:m.submitted_month, datasets:[{data:m.on_time_rate,
    borderColor:s1, borderWidth:2, pointRadius:4, pointBackgroundColor:s1, pointBorderColor:surf, pointBorderWidth:2, tension:0}]},
    options:base(pct)}));
  charts.push(new Chart(cOnline, {type:'line', data:{labels:m.submitted_month, datasets:[{data:m.online_rate,
    borderColor:s1, borderWidth:2, pointRadius:4, pointBackgroundColor:s1, pointBorderColor:surf, pointBorderWidth:2, tension:0}]},
    options:base(pct, 0, 1)}));
  const u = D.units, ou = base(pct);
  ou.indexAxis='y'; ou.interaction={mode:'nearest', axis:'y', intersect:false};
  ou.scales = {x:{grid:{color:css('--grid')}, border:{display:false}, ticks:{color:css('--text2'), callback:v=>pct(v)}},
               y:{grid:{display:false}, ticks:{color:css('--text2')}, border:{color:css('--border')}}};
  ou.plugins.tooltip.callbacks.label = c => ` ${pct(c.parsed.x)} · ${num(u[c.dataIndex].received)} hồ sơ`;
  charts.push(new Chart(cUnit, {type:'bar', data:{labels:u.map(r=>r.unit_name.replace('UBND phường ','')),
    datasets:[{data:u.map(r=>r.on_time_rate), backgroundColor:s1, borderRadius:{topRight:4,bottomRight:4}, borderSkipped:'left', barPercentage:.7}]},
    options:ou}));
}
draw();
matchMedia('(prefers-color-scheme: dark)').addEventListener('change', draw);

document.getElementById('tSvc').innerHTML = '<tr><th>Thủ tục</th><th>Lĩnh vực</th><th>SLA</th><th>p50</th><th>p90</th><th>Trễ hạn</th><th>Đã xử lý</th></tr>' +
  D.services.map(s=>`<tr><td>${s.service_name}</td><td>${s.sector}</td><td>${s.sla_days}</td><td>${s.p50_processing_days.toFixed(2)}</td>
  <td class="${s.p90_processing_days>s.sla_days?'fail':''}">${s.p90_processing_days.toFixed(2)}</td><td>${pct(s.late_rate)}</td><td>${num(s.finished)}</td></tr>`).join('');

const dq = D.dq;
document.getElementById('dqSub').innerHTML = `Gate: <span class="pill ${dq.gate_passed?'ok':'fail'}">${dq.gate_passed?'✓ PASSED':'✗ FAILED'}</span> · ${num(dq.rows_valid)} hợp lệ · ${num(dq.rows_quarantined)} cách ly`;
document.getElementById('tDq').innerHTML = '<tr><th>Check</th><th>Mức</th><th>Giá trị</th><th>Ngưỡng</th><th>Kết quả</th></tr>' +
  dq.checks.map(c=>`<tr><td>${c.check}</td><td>${c.severity}</td><td>${c.value}</td><td>${c.threshold}</td>
  <td><span class="${c.passed?'ok':(c.severity==='error'?'fail':'warnc')}">${c.passed?'✓ đạt':'✗ không đạt'}</span></td></tr>`).join('');
document.getElementById('tQ').innerHTML = '<tr><th>Lý do</th><th>Số bản ghi</th></tr>' +
  Object.entries(dq.quarantine_reasons).map(([r,n])=>`<tr><td>${r}</td><td>${num(n)}</td></tr>`).join('');
</script></body></html>
"""


def run(spark: SparkSession, run_id: str) -> dict:
    data = collect(spark)
    html = (TEMPLATE.replace("__DATA__", json.dumps(data, ensure_ascii=False, default=str))
                    .replace("__GEN__", data["generated_at"]))
    REPORTS.mkdir(parents=True, exist_ok=True)
    out = REPORTS / "dashboard.html"
    out.write_text(html, encoding="utf-8")
    return {"inputs": [str(GOLD)], "outputs": [str(out)], "rows": {}}
