-- ====================================================================
-- Table: daily_category_orders
-- Description: Daily order count by product category
-- Purpose: Track product category performance
-- ====================================================================

CREATE TABLE IF NOT EXISTS daily_category_orders (
    date DATE NOT NULL,
    category VARCHAR(50) NOT NULL,
    order_count INTEGER NOT NULL,
    total_quantity INTEGER NOT NULL,
    total_revenue DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, category)
);

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_daily_category_orders_date ON daily_category_orders(date DESC);
CREATE INDEX IF NOT EXISTS idx_daily_category_orders_category ON daily_category_orders(category);