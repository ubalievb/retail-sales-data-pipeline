
from datetime import datetime

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="std18_266_main_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "pipeline"],
) as dag:

    load_stores = TriggerDagRunOperator(
        task_id="load_stores",
        trigger_dag_id="std18_266_load_stores",
        wait_for_completion=True,
    )

    load_coupons = TriggerDagRunOperator(
        task_id="load_coupons",
        trigger_dag_id="std18_266_load_coupons",
        wait_for_completion=True,
    )

    load_promos = TriggerDagRunOperator(
        task_id="load_promos",
        trigger_dag_id="std18_266_load_promos",
        wait_for_completion=True,
    )

    load_promo_types = TriggerDagRunOperator(
        task_id="load_promo_types",
        trigger_dag_id="std18_266_load_promo_types",
        wait_for_completion=True,
    )

    load_traffic = TriggerDagRunOperator(
        task_id="load_traffic",
        trigger_dag_id="std18_266_load_traffic",
        wait_for_completion=True,
    )
    
    load_bills = TriggerDagRunOperator(
    task_id="check_bills_source",
    trigger_dag_id="std18_266_load_bills",
    wait_for_completion=True,
    )

    load_sales_mart = TriggerDagRunOperator(
        task_id="load_sales_mart",
        trigger_dag_id="std18_266_load_sales_mart",
        wait_for_completion=True,
    )

    load_clickhouse = TriggerDagRunOperator(
        task_id="load_clickhouse",
        trigger_dag_id="std18_266_load_clickhouse",
        wait_for_completion=True,
    )

    (
        load_stores
        >> load_coupons
        >> load_promos
        >> load_promo_types
        >> load_traffic
        >> load_bills
        >> load_sales_mart
        >> load_clickhouse
    )
