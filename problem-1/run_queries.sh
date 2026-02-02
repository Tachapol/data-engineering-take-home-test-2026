#!/bin/bash
# Test script to run all SQL queries and display results

set -e

PGHOST=${PGHOST:-localhost}
PGPORT=${PGPORT:-5432}
PGUSER=${PGUSER:-postgres}
PGDATABASE=${PGDATABASE:-postgres}

echo "========================================================================"
echo "Problem 1 - SQL Query Test Runner"
echo "========================================================================"
echo ""

# Question 1
echo "========================================================================"
echo "Question 1: Total revenue for user USER-001 across all completed orders"
echo "========================================================================"
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f sql/problem-1-dml-1.sql
echo ""

# Question 2
echo "========================================================================"
echo "Question 2: How many completed orders were placed on 2024-01-03"
echo "========================================================================"
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f sql/problem-1-dml-2.sql
echo ""

# Question 3
echo "========================================================================"
echo "Question 3: Average daily order count for Beverage category in Jan 2024"
echo "========================================================================"
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f sql/problem-1-dml-3.sql
echo ""

# Question 4
echo "========================================================================"
echo "Question 4: Date with the highest number of new customers acquired"
echo "========================================================================"
psql -h $PGHOST -p $PGPORT -U $PGUSER -d $PGDATABASE -f sql/problem-1-dml-4.sql
echo ""

echo "========================================================================"
echo "All queries executed successfully!"
echo "========================================================================"