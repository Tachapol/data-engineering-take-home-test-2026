"""
Airflow DAG for Daily Order Analytics Batch Pipeline Scheduled to run daily at 8:00 AM
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pendulum

# Define default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz='UTC'),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'daily_order_analytics',
    default_args=default_args,
    description='Daily batch pipeline for order analytics',
    schedule_interval='0 8 * * *',  # Run daily at 8:00 AM
    catchup=False,  # Don't backfill historical runs
    max_active_runs=1,
    tags=['batch', 'analytics', 'orders'],
)

# Task 1: Create destination tables (if not exists)
create_tables = PostgresOperator(
    task_id='create_destination_tables',
    postgres_conn_id='postgres_default',
    sql=[
        """
        CREATE TABLE IF NOT EXISTS daily_revenue (
            date DATE PRIMARY KEY,
            total_revenue DECIMAL(15,2) NOT NULL,
            total_orders INTEGER NOT NULL,
            total_quantity INTEGER NOT NULL,
            avg_order_value DECIMAL(10,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_new_customers (
            date DATE PRIMARY KEY,
            new_customer_count INTEGER NOT NULL,
            new_customer_revenue DECIMAL(15,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_category_orders (
            date DATE NOT NULL,
            category VARCHAR(50) NOT NULL,
            order_count INTEGER NOT NULL,
            total_quantity INTEGER NOT NULL,
            total_revenue DECIMAL(15,2) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            PRIMARY KEY (date, category)
        );
        """
    ],
    dag=dag,
)

# Task 2: Delete existing data for the execution date (for idempotency)
delete_existing_data = PostgresOperator(
    task_id='delete_existing_data',
    postgres_conn_id='postgres_default',
    sql="""
        DELETE FROM daily_revenue WHERE date = '{{ ds }}';
        DELETE FROM daily_new_customers WHERE date = '{{ ds }}';
        DELETE FROM daily_category_orders WHERE date = '{{ ds }}';
    """,
    dag=dag,
)

# Task 3: Run Spark batch job
run_batch_pipeline = SparkSubmitOperator(
    task_id='run_batch_pipeline',
    application='/opt/airflow/dags/src/batch_pipeline.py',
    conn_id='spark_default',
    conf={
        'spark.jars.packages': 'org.postgresql:postgresql:42.7.1',
        'spark.driver.memory': '2g',
        'spark.executor.memory': '2g',
    },
    application_args=['{{ ds }}'],  # Pass execution date
    dag=dag,
)

# Task 4: Data quality checks
def check_data_quality(**context):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    revenue_count = pg_hook.get_first("SELECT COUNT(*) FROM daily_revenue")[0]
    
    if revenue_count == 0:
        raise ValueError("No data found in daily_revenue table")
        
    print(f"Data Quality Passed: Found {revenue_count} records in total.")

data_quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=check_data_quality,
    provide_context=True,
    dag=dag,
)

# Task 5: Generate summary report
def generate_summary_report(**context):
    """Generate and log summary statistics"""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    execution_date = context['ds']
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Get summary stats
    revenue_stats = pg_hook.get_first(f"""
        SELECT total_revenue, total_orders, avg_order_value
        FROM daily_revenue
        WHERE date = '{execution_date}'
    """)
    
    new_customers = pg_hook.get_first(f"""
        SELECT new_customer_count, new_customer_revenue
        FROM daily_new_customers
        WHERE date = '{execution_date}'
    """)
    
    category_stats = pg_hook.get_records(f"""
        SELECT category, order_count, total_revenue
        FROM daily_category_orders
        WHERE date = '{execution_date}'
        ORDER BY total_revenue DESC
    """)
    
    # Log summary
    print("="*60)
    print(f"DAILY SUMMARY REPORT - {execution_date}")
    print("="*60)
    
    if revenue_stats:
        print(f"\nRevenue Metrics:")
        print(f"  Total Revenue: ${revenue_stats[0]:,.2f}")
        print(f"  Total Orders: {revenue_stats[1]:,}")
        print(f"  Avg Order Value: ${revenue_stats[2]:,.2f}")
    
    if new_customers:
        print(f"\nNew Customer Metrics:")
        print(f"  New Customers: {new_customers[0]:,}")
        print(f"  Revenue from New Customers: ${new_customers[1]:,.2f}")
    
    if category_stats:
        print(f"\nCategory Performance:")
        for cat, orders, revenue in category_stats:
            print(f"  {cat}: {orders:,} orders, ${revenue:,.2f}")
    
    print("="*60)

summary_report = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    provide_context=True,
    dag=dag,
)

# Task dependencies
create_tables >> delete_existing_data >> run_batch_pipeline >> data_quality_check >> summary_report
