-- Truy vấn mẫu trên lớp Gold (Spark SQL). Chạy: make sql
-- (run_sql.py đăng ký sẵn các view: fact, dim_date, dim_unit, dim_service)

-- 1) Top 3 phường có tỷ lệ đúng hạn thấp nhất quý 3/2026
SELECT u.unit_name,
       COUNT(*)                                             AS finished,
       ROUND(AVG(CASE WHEN f.is_late THEN 0 ELSE 1 END), 4) AS on_time_rate
FROM fact f
JOIN dim_unit u ON f.unit_code = u.unit_code
JOIN dim_date d ON f.submitted_date_key = d.date_key
WHERE f.is_finished AND d.year = 2026 AND d.quarter = 3
GROUP BY u.unit_name
ORDER BY on_time_rate
LIMIT 3;

-- 2) Xu hướng tỷ lệ nộp trực tuyến theo tháng + thay đổi so với tháng trước (window function)
SELECT submitted_month,
       ROUND(AVG(CASE WHEN channel = 'ONLINE' THEN 1 ELSE 0 END), 4) AS online_rate,
       ROUND(AVG(CASE WHEN channel = 'ONLINE' THEN 1 ELSE 0 END)
             - LAG(AVG(CASE WHEN channel = 'ONLINE' THEN 1 ELSE 0 END)) OVER (ORDER BY submitted_month), 4) AS mom_change
FROM fact
GROUP BY submitted_month
ORDER BY submitted_month;

-- 3) Hồ sơ đang quá hạn (backlog) theo phường x thủ tục, kèm số ngày quá hạn lâu nhất
SELECT f.unit_code, s.service_name,
       COUNT(*)                                                         AS overdue_open,
       MAX(DATEDIFF(DATE'2026-09-30', CAST(f.due_at AS DATE)))          AS max_days_overdue
FROM fact f
JOIN dim_service s ON f.service_code = s.service_code
WHERE f.is_overdue_open
GROUP BY f.unit_code, s.service_name
ORDER BY overdue_open DESC
LIMIT 5;
