CREATE TABLE IF NOT EXISTS dim_customers (customer_id INTEGER PRIMARY KEY, customer_name TEXT, email TEXT, created_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS fact_sales (sale_id INTEGER PRIMARY KEY, sale_date DATE, product TEXT, qty INTEGER, unit_price NUMERIC, total_amount NUMERIC);
CREATE TABLE IF NOT EXISTS fact_orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, status TEXT, order_total NUMERIC, created_at TIMESTAMP);
