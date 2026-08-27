-- Sales by product. Reads qualifying_orders (order_status = 'Completed',
-- NOT _is_orphan, NOT _is_deleted) — the rule is defined once, in the
-- runner, never here. Zero-sales products are kept: a product missing from
-- a sales report is indistinguishable from a pipeline bug. avg_order_value
-- is NULL (not 0) when there are no orders — an average over nothing is
-- unknown, not zero.
CREATE OR REPLACE TABLE {gold}.sales_by_product AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  COUNT(q.order_id) AS total_orders,
  CAST(COALESCE(SUM(q.total_amount), 0) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(SUM(q.total_amount) / NULLIF(COUNT(q.order_id), 0) AS DECIMAL(18, 2)) AS avg_order_value
FROM {silver}.products p
LEFT JOIN qualifying_orders q
  ON q.product_id = p.product_id
WHERE NOT p._is_deleted
GROUP BY p.product_id, p.product_name, p.category
