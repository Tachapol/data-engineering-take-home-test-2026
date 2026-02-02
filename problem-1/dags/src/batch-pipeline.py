import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, current_timestamp, round, lit, row_number
from pyspark.sql.window import Window

def run_etl(execution_date):
    # Initialize Spark with Postgres Driver
    spark = SparkSession.builder \
        .appName(f"Daily_ETL_{execution_date}") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .getOrCreate()

    print(f"DEBUG: Processing data for {execution_date}")

    # Database Connection Info
    jdbc_url = "jdbc:postgresql://postgresql:5432/postgres"
    db_props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

    # --- 1. EXTRACT ---
    # Load products (full table) and orders (only for execution_date)
    products_df = spark.read.jdbc(jdbc_url, "products", properties=db_props)
    
    order_query = f"(SELECT * FROM orders WHERE DATE(order_timestamp) = '{execution_date}') AS daily_orders"
    orders_df = spark.read.jdbc(jdbc_url, order_query, properties=db_props)

    # --- 2. TRANSFORM: Daily Revenue & Categories ---
    # Filter COMPLETE orders and Join with products
    complete_orders = orders_df.filter(col("status") == "COMPLETE")
    joined_df = complete_orders.join(products_df, "product_id") \
                               .withColumn("revenue", col("quantity") * col("price"))

    # A. Aggregate Daily Revenue
    daily_rev = joined_df.groupBy(to_date("order_timestamp").alias("date")) \
        .agg(
            sum("revenue").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            sum("quantity").alias("total_quantity")
        ) \
        .withColumn("avg_order_value", round(col("total_revenue") / col("total_orders"), 2)) \
        .withColumn("created_at", current_timestamp())

    # B. Aggregate Daily Category Orders
    daily_cat = joined_df.groupBy(to_date("order_timestamp").alias("date"), "category") \
        .agg(
            count("order_id").alias("order_count"),
            sum("quantity").alias("total_quantity"),
            sum("revenue").alias("total_revenue")
        ) \
        .withColumn("created_at", current_timestamp())

    # --- 3. TRANSFORM: New Customers (Needs Historical Check) ---
    # To find "First Time Buyers", we must look at all historical COMPLETE orders
    all_history_query = "(SELECT user_id, order_timestamp, product_id, quantity FROM orders WHERE status = 'COMPLETE') AS history"
    history_df = spark.read.jdbc(jdbc_url, all_history_query, properties=db_props)

    # Use Window Function to find the very first order for each user
    window_spec = Window.partitionBy("user_id").orderBy("order_timestamp")
    first_orders = history_df.withColumn("rn", row_number().over(window_spec)) \
                             .filter(col("rn") == 1) \
                             .withColumn("order_date", to_date("order_timestamp"))

    # Filter only customers who made their first purchase TODAY
    new_customers_today = first_orders.filter(col("order_date") == execution_date) \
                                      .join(products_df, "product_id") \
                                      .withColumn("revenue", col("quantity") * col("price"))

    daily_new_cust = new_customers_today.groupBy(col("order_date").alias("date")) \
        .agg(
            count("user_id").alias("new_customer_count"),
            sum("revenue").alias("new_customer_revenue")
        ) \
        .withColumn("created_at", current_timestamp())

    # --- 4. LOAD ---
    print("Loading data to PostgreSQL...")

    # If daily revenue is empty, write a zero row so downstream DQ/selects have a row for the date
    if daily_rev.rdd.isEmpty():
        print(f"No revenue rows for {execution_date}: inserting zero-summary row")
        zero_rev = spark.createDataFrame([
            (execution_date, 0.0, 0, 0, 0.0)
        ], schema=['date', 'total_revenue', 'total_orders', 'total_quantity', 'avg_order_value'])
        zero_rev.write.jdbc(jdbc_url, "daily_revenue", mode="append", properties=db_props)
    else:
        daily_rev.write.jdbc(jdbc_url, "daily_revenue", mode="append", properties=db_props)

    # For category and new customers, skip writing if empty
    if not daily_cat.rdd.isEmpty():
        daily_cat.write.jdbc(jdbc_url, "daily_category_orders", mode="append", properties=db_props)
    else:
        print(f"No category rows for {execution_date}, skipping daily_category_orders write")

    if not daily_new_cust.rdd.isEmpty():
        daily_new_cust.write.jdbc(jdbc_url, "daily_new_customers", mode="append", properties=db_props)
    else:
        print(f"No new customer rows for {execution_date}, skipping daily_new_customers write")

    print(f"✓ Pipeline finished for {execution_date}")
    spark.stop()

if __name__ == "__main__":
    # Get date from Airflow or command line
    dt = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    run_etl(dt)