
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="std18_266_load_promos",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "greenplum"],
) as dag:

    check_promos_source = PostgresOperator(
        task_id="check_promos_source",
        postgres_conn_id="gp_std18_266",
        sql="""
            SELECT COUNT(*)
            FROM std18_266_final.promos_ext;
        """,
    )

    load_promos = PostgresOperator(
        task_id="load_promos",
        postgres_conn_id="gp_std18_266",
        sql="""
            BEGIN;

            TRUNCATE TABLE std18_266_final.promos;

            INSERT INTO std18_266_final.promos (
                promotion_id,
                promotion_name,
                promotion_type_id,
                product_id,
                discount_amount
            )
            SELECT
                promotion_id,
                promotion_name,
                promotion_type_id,
                product_id,
                discount_amount
            FROM std18_266_final.promos_ext;

            COMMIT;
        """,
    )

    check_promos_source >> load_promos
