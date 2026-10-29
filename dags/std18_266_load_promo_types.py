
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="std18_266_load_promo_types",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "greenplum"],
) as dag:

    check_promo_types_source = PostgresOperator(
        task_id="check_promo_types_source",
        postgres_conn_id="gp_std18_266",
        sql="""
            SELECT COUNT(*)
            FROM std18_266_final.promo_types_ext;
        """,
    )

    load_promo_types = PostgresOperator(
        task_id="load_promo_types",
        postgres_conn_id="gp_std18_266",
        sql="""
            BEGIN;

            TRUNCATE TABLE std18_266_final.promo_types;

            INSERT INTO std18_266_final.promo_types (
                promotion_type_id,
                text
            )
            SELECT
                promotion_type_id,
                text
            FROM std18_266_final.promo_types_ext;

            COMMIT;
        """,
    )

    check_promo_types_source >> load_promo_types
