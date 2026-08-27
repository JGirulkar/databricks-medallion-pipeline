-- Revenue by customer. customer_segment is the source-declared column
-- (Premium/Standard/Basic), carried as delivered; the BEHAVIOURAL segment
-- lives in customer_segmentation. lifetime_value_actual is computed from
-- orders — it sits alongside the declared lifetime_value upstream as a
-- declared-vs-actual comparison. last_order_date feeds the segment ladder's
-- recency test and is NULL for customers with no qualifying orders.
CREATE OR REPLACE TABLE {gold}.revenue_by_customer AS
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_segment,
  COUNT(q.order_id) AS total_orders,
  CAST(COALESCE(SUM(q.total_amount), 0) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(SUM(q.total_amount) / NULLIF(COUNT(q.order_id), 0) AS DECIMAL(18, 2)) AS avg_order_value,
  CAST(COALESCE(SUM(q.total_amount), 0) AS DECIMAL(18, 2)) AS lifetime_value_actual,
  MAX(q.order_date) AS last_order_date
FROM {silver}.customers c
LEFT JOIN qualifying_orders q
  ON q.customer_id = c.customer_id
WHERE NOT c._is_deleted
GROUP BY c.customer_id, c.customer_name, c.customer_segment
