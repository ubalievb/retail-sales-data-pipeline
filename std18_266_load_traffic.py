
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="std18_266_load_traffic",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "greenplum"],
) as dag:

    check_traffic_source = PostgresOperator(
        task_id="check_traffic_source",
        postgres_conn_id="gp_std18_266",
        sql="""
            SELECT COUNT(*)
            FROM std18_266_final.traffic_ext;
        """,
    )

    load_traffic = PostgresOperator(
        task_id="load_traffic",
        postgres_conn_id="gp_std18_266",
        sql="""
            BEGIN;

            TRUNCATE TABLE std18_266_final.traffic;

            INSERT INTO std18_266_final.traffic (
                stores_id,
                sale_date,
                hour,
                frame_id,
                quantity
            )
            SELECT
                plant,
                TO_DATE(date, 'DD.MM.YYYY'),
                time::TIME,
                frame_id,
                quantity
            FROM std18_266_final.traffic_ext;

            COMMIT;
        """,
    )

    check_traffic_source >> load_traffic
