-- Q2 Find how many completed orders were placed on 2024-01-03

SELECT
	DATE(order_timestamp) AS order_date,
	COUNT(*) as completed_order_count,
	COUNT(DISTINCT user_id) as unique_customers
FROM orders
WHERE status = 'COMPLETE'
AND date(order_timestamp) = '2024-01-03'
GROUP BY date(order_timestamp);