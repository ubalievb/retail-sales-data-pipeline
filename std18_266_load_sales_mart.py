
from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="std18_266_load_sales_mart",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "greenplum", "data_mart"],
) as dag:

    load_sales_mart = PostgresOperator(
        task_id="load_sales_mart",
        postgres_conn_id="gp_std18_266",
        sql="""
            BEGIN;

            TRUNCATE TABLE std18_266_final.sales_mart_test;

            INSERT INTO std18_266_final.sales_mart_test (
                day,
                stores_id,
                stores_name,
                turnover,
                coupon_discount,
                turnover_with_dis,
                total_units_sold,
                total_checks,
                total_traffic,
                promo_products,
                promo_products_pct,
                avg_units_per_check,
                store_conversion,
                avg_turnover_per_check,
                avg_revenue_per_visitor
            )
            WITH turnover AS (
                SELECT
                    stores_id,
                    day,
                    SUM(turnover_with_vat) AS turnover
                FROM std18_266_final.bills_partitioned
                WHERE day >= DATE '2021-01-01'
                  AND day < DATE '2021-03-01'
                GROUP BY stores_id, day
            ),

            coupon_discount_raw AS (
                SELECT
                    c.stores_id,
                    c.sale_date AS day,
                    c.billnum,
                    TRIM(c.product_id) AS product_id,
                    c.coupon_id,
                    CASE
                        WHEN p.promotion_type_id = '001'
                            THEN p.discount_amount::NUMERIC
                        WHEN p.promotion_type_id = '002'
                            THEN b.turnover_with_vat::NUMERIC
                                 * p.discount_amount::NUMERIC / 100.0
                        ELSE 0
                    END AS discount_amount
                FROM std18_266_final.coupons c
                JOIN std18_266_final.bills_partitioned b
                  ON TRIM(c.product_id) = TRIM(b.product_id)
                 AND c.billnum = b.billnum
                 AND c.sale_date = b.day
                 AND c.stores_id = b.stores_id
                JOIN std18_266_final.promos p
                  ON TRIM(c.product_id) = TRIM(p.product_id)
                 AND c.promotion_id = p.promotion_id
                WHERE c.sale_date >= DATE '2021-01-01'
                  AND c.sale_date < DATE '2021-03-01'
            ),

            coupon_discount AS (
                SELECT
                    stores_id,
                    day,
                    SUM(discount_amount) AS coupon_discount
                FROM coupon_discount_raw
                GROUP BY stores_id, day
            ),

            total_units_sold AS (
                SELECT
                    stores_id,
                    day,
                    SUM(units) AS total_units_sold
                FROM std18_266_final.bills_partitioned
                WHERE day >= DATE '2021-01-01'
                  AND day < DATE '2021-03-01'
                GROUP BY stores_id, day
            ),

            total_checks AS (
                SELECT
                    stores_id,
                    day,
                    COUNT(DISTINCT billnum) AS total_checks
                FROM std18_266_final.bills_partitioned
                WHERE day >= DATE '2021-01-01'
                  AND day < DATE '2021-03-01'
                GROUP BY stores_id, day
            ),

            traffic_agg AS (
                SELECT
                    stores_id,
                    sale_date,
                    SUM(quantity) AS total_traffic
                FROM std18_266_final.traffic
                WHERE sale_date >= DATE '2021-01-01'
                  AND sale_date < DATE '2021-03-01'
                GROUP BY stores_id, sale_date
            ),

            coupons_agg AS (
                SELECT
                    stores_id,
                    sale_date AS day,
                    COUNT(*) AS promo_products
                FROM std18_266_final.coupons
                WHERE sale_date >= DATE '2021-01-01'
                  AND sale_date < DATE '2021-03-01'
                GROUP BY stores_id, sale_date
            )

            SELECT
                tur.day,
                s.stores_id,
                s.stores_name,
                tur.turnover::INTEGER,
                COALESCE(dis.coupon_discount, 0),
                tur.turnover - COALESCE(dis.coupon_discount, 0),
                sold.total_units_sold::INTEGER,
                tot.total_checks::INTEGER,
                COALESCE(t.total_traffic, 0),
                COALESCE(c.promo_products, 0)::INTEGER,

                ROUND(
                    COALESCE(c.promo_products, 0)::NUMERIC
                    / NULLIF(sold.total_units_sold, 0) * 100,
                    1
                ),

                ROUND(
                    sold.total_units_sold::NUMERIC
                    / NULLIF(tot.total_checks, 0),
                    2
                ),

                ROUND(
                    tot.total_checks::NUMERIC
                    / NULLIF(t.total_traffic, 0) * 100,
                    2
                ),

                ROUND(
                    tur.turnover::NUMERIC
                    / NULLIF(tot.total_checks, 0),
                    1
                ),

                CASE
                    WHEN COALESCE(t.total_traffic, 0) = 0 THEN 0
                    ELSE ROUND(
                        tur.turnover::NUMERIC / t.total_traffic,
                        1
                    )
                END

            FROM turnover tur

            JOIN std18_266_final.stores s
              ON s.stores_id = tur.stores_id

            LEFT JOIN traffic_agg t
              ON t.stores_id = tur.stores_id
             AND t.sale_date = tur.day

            LEFT JOIN coupons_agg c
              ON c.stores_id = tur.stores_id
             AND c.day = tur.day

            LEFT JOIN total_checks tot
              ON tot.stores_id = tur.stores_id
             AND tot.day = tur.day

            LEFT JOIN total_units_sold sold
              ON sold.stores_id = tur.stores_id
             AND sold.day = tur.day

            LEFT JOIN coupon_discount dis
              ON dis.stores_id = tur.stores_id
             AND dis.day = tur.day;

            COMMIT;
        """,
    )
