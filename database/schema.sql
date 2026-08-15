-- Database schema for medallion pipeline (reference)
-- Tables created via Delta in jobs; this documents logical schema.

CREATE TABLE IF NOT EXISTS bronze.customers (
  customer_id INT,
  customer_name STRING,
  email STRING,
  country STRING,
  signup_date DATE,
  customer_segment STRING,
  lifetime_value DECIMAL(18,2)
);

CREATE TABLE IF NOT EXISTS bronze.orders (
  order_id INT,
  customer_id INT,
  order_date DATE,
  product_id INT,
  quantity INT,
  unit_price DECIMAL(18,2),
  total_amount DECIMAL(18,2),
  order_status STRING,
  payment_date DATE
);

CREATE TABLE IF NOT EXISTS bronze.products (
  product_id INT,
  product_name STRING,
  category STRING,
  price DECIMAL(18,2),
  cost DECIMAL(18,2),
  stock_quantity INT,
  reorder_level INT
);
