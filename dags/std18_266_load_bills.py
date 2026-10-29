
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="std18_266_load_bills",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "greenplum"],
) as dag:

    check_bills_source = PostgresOperator(
        task_id="check_bills_source",
        postgres_conn_id="gp_std18_266",
        sql="""
            SELECT COUNT(*)
            FROM std18_266_final.bills_head_ext;
        """,
    )
    load_bills = PostgresOperator(
        task_id="load_bills",
        postgres_conn_id="gp_std18_266",
        sql="""
            INSERT INTO std18_266_final.bills_partitioned (
                billnum,
                billitem,
                product_id,
                stores_id,
                day,
                month,
                turnover_with_vat,
                units,
                netval,
                tax,
                bill_count
            )
            SELECT
                h.billnum,
                i.billitem,
                i.material::VARCHAR,
                h.plant::VARCHAR,
                h.calday,
                DATE_TRUNC('month', h.calday)::DATE,
                (i.netval + i.tax)::INTEGER,
                i.qty::INTEGER,
                i.netval,
                i.tax,
                ROW_NUMBER() OVER (
                    PARTITION BY h.billnum
                    ORDER BY i.billitem
                )::INTEGER
            FROM std18_266_final.bills_head_ext h
            JOIN std18_266_final.bills_item_ext i
                ON h.billnum = i.billnum
            WHERE h.calday > COALESCE(
                (
                    SELECT MAX(day)
                    FROM std18_266_final.bills_partitioned
                ),
                DATE '2020-10-31'
            );
        """,
    )


    check_bills_source >> load_bills
