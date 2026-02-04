#!/bin/bash
# Initialize destination tables for Problem 1

set -e

echo "Creating destination tables..."

PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}

# Create daily_revenue table
echo "Creating daily_revenue table..."
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f sql/problem-1-ddl-daily_revenue.sql

# Create daily_new_customers table
echo "Creating daily_new_customers table..."
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f sql/problem-1-ddl-daily_new_customers.sql

# Create daily_category_orders table
echo "Creating daily_category_orders table..."
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f sql/problem-1-ddl-daily_category_orders.sql

echo "All destination tables created successfully!"

# Verify tables
echo ""
echo "Verifying tables..."
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -c "\dt daily_*"

echo ""
echo "Setup complete!"