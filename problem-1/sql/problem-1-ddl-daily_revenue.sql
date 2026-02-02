-- ====================================================================
-- Table: daily_revenue
-- Description: Daily aggregated revenue from completed orders
-- Purpose: Track daily revenue performance for business analytics
-- ====================================================================

CREATE TABLE IF NOT EXISTS daily_revenue (
    date DATE PRIMARY KEY,
    total_revenue DECIMAL(15,2) NOT NULL,
    total_orders INTEGER NOT NULL,
    total_quantity INTEGER NOT NULL,
    avg_order_value DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for date range queries
CREATE INDEX IF NOT EXISTS idx_daily_revenue_date ON daily_revenue(date DESC);