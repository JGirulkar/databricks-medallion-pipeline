# Data Model

## customers

- customer_id (PK), customer_name, email, country, signup_date, customer_segment, lifetime_value

## orders

- order_id (PK), customer_id (FK), order_date, product_id (FK), quantity, unit_price, total_amount, order_status, payment_date

## products

- product_id (PK), product_name, category, price, cost, stock_quantity, reorder_level
