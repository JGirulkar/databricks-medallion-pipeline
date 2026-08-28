-- Behavioural segmentation. Derives from {gold}.revenue_by_customer — NOT
-- from silver — so the pie cross-foots with the customer table by
-- construction. ORDERING CONSTRAINT: 02_revenue_by_customer.sql must run
-- first (the runner executes files in name order).
--
-- Ladder, evaluated top-down, mutually exclusive and exhaustive:
--   Inactive    no qualifying order in the {inactive_days} days before
--               as_of (as_of = MAX(last_order_date), data-anchored — safe
--               because silver's order_date window check quarantines
--               future-dated rows). Includes customers with no qualifying
--               orders at all.
--   High-Value  active AND lifetime qualifying revenue >= {high_value_revenue}
--   Repeat      active AND >= 2 lifetime qualifying orders
--   One-Time    active AND exactly 1
-- Recency outranks value: a lapsed big spender is the win-back signal.
CREATE OR REPLACE TABLE {gold}.customer_segmentation AS
WITH as_of AS (
  SELECT MAX(last_order_date) AS as_of_date
  FROM {gold}.revenue_by_customer
),
labeled AS (
  SELECT
    CASE
      WHEN r.last_order_date IS NULL
        OR r.last_order_date < DATE_SUB(a.as_of_date, {inactive_days}) THEN 'Inactive'
      WHEN r.lifetime_value_actual >= {high_value_revenue} THEN 'High-Value'
      WHEN r.total_orders >= 2 THEN 'Repeat'
      ELSE 'One-Time'
    END AS segment_type,
    r.total_revenue
  FROM {gold}.revenue_by_customer r
  CROSS JOIN as_of a
)
SELECT
  segment_type,
  COUNT(*) AS customer_count,
  CAST(AVG(total_revenue) AS DECIMAL(18, 2)) AS avg_revenue,
  CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue
FROM labeled
GROUP BY segment_type
