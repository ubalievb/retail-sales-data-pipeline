from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

default_args = {
    "owner": "std18_266"
}

with DAG(
    dag_id="std18_266_load_stores",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "greenplum"],
) as dag:

    check_stores_source = PostgresOperator(
        task_id="check_stores_source",
        postgres_conn_id="gp_std18_266",
        sql="""
            SELECT COUNT(*)
            FROM std18_266_final.ext_stores;
        """,
    )

    load_stores = PostgresOperator(
        task_id="load_stores",
        postgres_conn_id="gp_std18_266",
        sql="""
            BEGIN;

            TRUNCATE TABLE std18_266_final.stores;

            INSERT INTO std18_266_final.stores (
                stores_id,
                stores_name
            )
            SELECT
                stores_id,
                stores_name
            FROM std18_266_final.ext_stores;

            COMMIT;
        """,
    )

    check_stores_source >> load_stores
