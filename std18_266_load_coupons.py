
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="std18_266_load_coupons",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "greenplum"],
) as dag:

    check_coupons_source = PostgresOperator(
        task_id="check_coupons_source",
        postgres_conn_id="gp_std18_266",
        sql="""
            SELECT COUNT(*)
            FROM std18_266_final.coupons_ext;
        """,
    )

    load_coupons = PostgresOperator(
        task_id="load_coupons",
        postgres_conn_id="gp_std18_266",
        sql="""
            BEGIN;
                INSERT INTO std18_266_final.coupons (
                    stores_id,
                    sale_date,
                    coupon_id,
                    promotion_id,
                    product_id,
                    billnum
                )
                SELECT
                    e.store_id,
                    TO_DATE(e.sale_date, 'YYYYMMDD'),
                    e.coupon_id,
                    e.coupon_promo_id,
                    e.product_id,
                    e.receipt_id
                FROM std18_266_final.coupons_ext e
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM std18_266_final.coupons c
                    WHERE c.coupon_id = e.coupon_id
                );

            COMMIT;
        """,
    )

    check_coupons_source >> load_coupons
