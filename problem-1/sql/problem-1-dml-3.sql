-- Q3 Find the average daily order count for the 'Beverage' product category in January 2024

WITH daily_counts AS (
    SELECT 
        DATE(o.order_timestamp) as order_date,
        COUNT(*) as order_count
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    WHERE p.category = 'Beverage'
      AND o.status = 'COMPLETE'
      AND o.order_timestamp >= '2024-01-01'
      AND o.order_timestamp < '2024-02-01'
    GROUP BY DATE(o.order_timestamp)
)
SELECT 
    ROUND(AVG(order_count), 2) as avg_daily_orders
FROM daily_counts;