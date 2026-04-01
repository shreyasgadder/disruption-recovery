"""
ETG Disruption Recovery Monitoring 

This DAG monitors the automated streaming pipeline (Phase B).
- Gold: Business-level reporting and VIP alerting.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# --- Configuration ---
DB_CONN_ID = "postgres_default"

def report_gold_metrics(**ctx):
    """Fetch high-level airline metrics from the Gold analytics layer."""
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    
    print("RECOVERY MISSION STATUS (Gold Layer)")
    print("-" * 40)
    
    try:
        # Fetching aggregated airline performance
        df = hook.get_pandas_df("SELECT airline, total_events, total_revenue, anomaly_count FROM airline_metrics ORDER BY total_revenue DESC")
        
        if df.empty:
            print("Status: Waiting for consumer to populate the Gold layer.")
        else:
            print(df.to_string(index=False))
    except Exception as e:
        print(f"Error fetching Gold metrics: {e}")
    print("-" * 40)

def surface_vip_anomalies(**ctx):
    """Business Intelligence: Identifying high-value customers needing immediate support."""
    hook = PostgresHook(postgres_conn_id=DB_CONN_ID)
    
    # Priority score threshold for platinum/gold tier escalation
    query = """
        SELECT customer_name, priority_score, conflict_type 
        FROM flagged_bookings 
        WHERE priority_score >= 70 
        ORDER BY priority_score DESC 
        LIMIT 5
    """
    try:
        df = hook.get_pandas_df(query)
        
        if not df.empty:
            print("SCENARIO ALERT: High-value customers affected by disruption:")
            print(df.to_string(index=False))
            print("-" * 40)
    except Exception as e:
        print(f"Error surfacing anomalies: {e}")

# --- DAG Definition ---
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "etg_recovery_mission",
    default_args=default_args,
    description="Pipeline monitoring for ETG disruption recovery",
    schedule_interval=timedelta(minutes=5),
    catchup=False,
    tags=["egt", "monitoring"],
) as dag:

    gold_report = PythonOperator(
        task_id="gold_layer_report",
        python_callable=report_gold_metrics,
    )

    anomaly_alerts = PythonOperator(
        task_id="vip_anomaly_alerts",
        python_callable=surface_vip_anomalies,
    )

    gold_report >> anomaly_alerts
