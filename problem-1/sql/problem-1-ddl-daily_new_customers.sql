-- ====================================================================
-- Table: daily_new_customers
-- Description: Daily count of first-time customers
-- Purpose: Track customer acquisition metrics
-- ====================================================================

CREATE TABLE IF NOT EXISTS daily_new_customers (
    date DATE PRIMARY KEY,
    new_customer_count INTEGER NOT NULL,
    new_customer_revenue DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index for date range queries
CREATE INDEX IF NOT EXISTS idx_daily_new_customers_date ON daily_new_customers(date DESC);

