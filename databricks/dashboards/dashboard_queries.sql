-- Dashboard dataset queries — GENERATED from sales_overview.lvdash.json
-- by scripts/gen_dashboard_queries.py. Do not edit by hand: the JSON is the
-- executed source of truth, and a test fails if this file drifts from it.
-- Table names are bare on purpose: the catalog and schema are supplied at
-- deploy time (--dataset-catalog de_assessment --dataset-schema gold).

-- ds_customers: Revenue by customer
SELECT customer_id, customer_name, customer_segment, total_orders,
       total_revenue, lifetime_value_actual, last_order_date
FROM revenue_by_customer;

-- ds_top_products: Top products by revenue
SELECT product_id, product_name, category, total_orders,
       total_revenue, avg_order_value
FROM sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;

-- ds_segments: Customer segmentation
SELECT segment_type, customer_count, avg_revenue, total_revenue
FROM customer_segmentation;

-- ds_trends: Daily revenue and orders
SELECT order_date, week_start, total_orders, total_revenue, avg_order_value
FROM daily_weekly_trends
ORDER BY order_date;
