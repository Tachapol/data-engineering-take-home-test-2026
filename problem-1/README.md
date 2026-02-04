# Problem 1: Daily Analytics Pipeline

## File Structure

```
problem-1/
├── dags/
│   └── daily_batch_dag.py          # Schedule (Airflow DAG)
├── sql/
│   ├── problem-1-ddl-*.sql         # Create tables
│   └── problem-1-dml-*.sql         # Insert data
├── src/
│   └── batch_pipeline.py           # Main program that processes data
├── docker-compose.yml             
├── Dockerfile                      # Dependencies
└── README.MD                       
```

## How to Use
1. **init_tables.sh** - Create destination tables (run once during setup)
2. **daily_order_analytics DAG** - Process data automatically every day
3. **run_queries.sh** - Query the results to get business insights

#### Step 1: Start the Program
```bash
cd problem-1
docker-compose up -d
```

#### Step 2: Initialize Tables (One-Time Setup)
```bash
chmod +x init_tables.sh
./init_tables.sh
```
This creates the 3 destination tables: `daily_revenue`, `daily_new_customers`,  `daily_category_orders`. You only need to run this once.

#### Step 3: Open Airflow (The Task Scheduler)

Open your browser: http://localhost:8081
- **Username:** admin
- **Password:** admin

#### Step 4: Enable and Trigger the DAG

1. Find the DAG named `daily_order_analytics` in the list
2. Click the toggle switch at the top to enable it
3. Click "Trigger" to start processing
4. Wait for the DAG to complete (watch the status in Airflow UI)

#### Note on Execution Date
If no data exists for the execution date, the pipeline inserts a zero-summary row instead of failing. This allows safe re-runs and keeps downstream checks working.

**Historical data can be reprocessed using Airflow backfill**
```bash
docker exec -it <airflow-container-id> airflow dags backfill \
  --start-date 2024-01-01 \
  --end-date 2024-01-5 \
  daily_order_analytics
```

#### Step 5: View Results
After the DAG completes, run the queries to see the results
```bash
chmod +x run_queries.sh
./run_queries.sh
```
You will see answers to 4 business questions based on the processed data.

## What Data We Got?
The program creates 3 new tables

### 1. daily_revenue (Total Sales)
This table shows
- How much did we sell today?
- How many items sold?
- What is the average order value?

### 2. daily_new_customers (New Customers)
This table shows
- How many new customers today?
- How much did new customers spend?

### 3. daily_category_orders (Sales by Product Category)
This table shows
- How much did each product category sell?
- How many items per category?

## How Does It Work?

```
Order Data (PostgreSQL)
        |
    Read Data
        |
   Process (PySpark)
        |
   Create Daily Summaries
        |
   Save Results (PostgreSQL)
```

**Every day at 8:00 AM the program runs automatically**

## Required Data
- `orders.csv` - Order information
- `products.csv` - Product information
- PostgreSQL database - Stores the data