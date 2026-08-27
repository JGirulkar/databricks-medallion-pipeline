-- Daily grain with a week_start column: weekly views GROUP BY week_start,
-- so both grains come from one set of numbers. Days with no qualifying
-- orders are absent (grain = observed business days).
CREATE OR REPLACE TABLE {gold}.daily_weekly_trends AS
SELECT
  q.order_date,
  CAST(DATE_TRUNC('WEEK', q.order_date) AS DATE) AS week_start,
  COUNT(q.order_id) AS total_orders,
  CAST(SUM(q.total_amount) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(SUM(q.total_amount) / COUNT(q.order_id) AS DECIMAL(18, 2)) AS avg_order_value
FROM qualifying_orders q
GROUP BY q.order_date
