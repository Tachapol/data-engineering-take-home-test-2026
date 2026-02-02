-- Q1 Find the total revenue for user USER-001 across all completed orders

SELECT 
    o.user_id,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.quantity) as total_quantity,
    SUM(o.quantity * p.price) as total_revenue,
    AVG(o.quantity * p.price) as avg_order_value
FROM orders o
JOIN products p ON o.product_id = p.product_id
WHERE o.user_id = 'USER-001'
  AND o.status = 'COMPLETE'
GROUP BY o.user_id;