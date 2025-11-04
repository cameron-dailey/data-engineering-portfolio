SELECT strftime('%Y-%m', sale_date) AS month, SUM(total_amount) AS revenue FROM fact_sales GROUP BY 1 ORDER BY 1;
SELECT strftime('%Y-%m', created_at) AS month, COUNT(*) AS new_customers FROM dim_customers GROUP BY 1 ORDER BY 1;
SELECT strftime('%Y-%m', created_at) AS month, status, COUNT(*) AS orders, SUM(order_total) AS total_order_value FROM fact_orders GROUP BY 1, 2 ORDER BY 1, 2;
