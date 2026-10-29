
from datetime import datetime
from decimal import Decimal

import requests

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import requests
import json




def load_sales_mart_to_clickhouse():
    # 1. Читаем витрину из Greenplum
    hook = PostgresHook(postgres_conn_id="gp_std18_266")

    sql = """
        SELECT
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
        FROM std18_266_final.sales_mart_test
        ORDER BY day, stores_id;
    """

    rows = hook.get_records(sql)
    print(f"Rows read from Greenplum: {len(rows)}")

    if not rows:
        print("No data to load.")
        return

    # 2. Подключаемся к ClickHouse
    ch_hook = BaseHook.get_connection("ch_std18_266")
    host = ch_hook.host
    port = ch_hook.port or 8123
    database = ch_hook.schema or "std18_266_final"
    user = ch_hook.login
    password = ch_hook.password

    base_url = f"http://{host}:{port}/"
    
    
    # 3. Пересоздаём таблицу ClickHouse
    recreate_sql = """
        DROP TABLE IF EXISTS std18_266_final.sales_mart;

        CREATE TABLE std18_266_final.sales_mart
        (
            day Date,
            stores_id String,
            stores_name String,
            turnover Int32,
            coupon_discount Decimal(10, 2),
            turnover_with_dis Decimal(10, 2),
            total_units_sold Int32,
            total_checks Int32,
            total_traffic Decimal(10, 2),
            promo_products Int32,
            promo_products_pct Decimal(10, 2),
            avg_units_per_check Decimal(10, 2),
            store_conversion Decimal(10, 2),
            avg_turnover_per_check Decimal(10, 2),
            avg_revenue_per_visitor Decimal(10, 2)
        )
        ENGINE = MergeTree()
        ORDER BY (day, stores_id)
    """

    for statement in recreate_sql.strip().split(";"):
        if statement.strip():
            response = requests.post(
                base_url,
                params={
                    "database": database,
                    "query": statement.strip()
                },
                auth=(user, password),
                timeout=120
            )
            response.raise_for_status()
    
    
    
    # 4. Загружаем строки напрямую в основную таблицу
   

    insert_sql = f"""
        INSERT INTO {database}.sales_mart
        FORMAT JSONEachRow
    """

    payload = "\n".join(
        json.dumps({
            "day": row[0].isoformat() if hasattr(row[0], "isoformat") else str(row[0]),
            "stores_id": str(row[1]),
            "stores_name": str(row[2]),
            "turnover": int(row[3] or 0),
            "coupon_discount": float(row[4] or 0),
            "turnover_with_dis": float(row[5] or 0),
            "total_units_sold": int(row[6] or 0),
            "total_checks": int(row[7] or 0),
            "total_traffic": float(row[8] or 0),
            "promo_products": int(row[9] or 0),
            "promo_products_pct": float(row[10] or 0),
            "avg_units_per_check": float(row[11] or 0),
            "store_conversion": float(row[12] or 0),
            "avg_turnover_per_check": float(row[13] or 0),
            "avg_revenue_per_visitor": float(row[14] or 0),
        }, ensure_ascii=False)
        for row in rows
    )

    response = requests.post(
        base_url,
        params={"database": database, "query": insert_sql},
        data=payload.encode("utf-8"),
        auth=(user, password),
        timeout=120,
    )    
    if not response.ok:
        raise RuntimeError(
            f"ClickHouse error {response.status_code}: {response.text}"
        )

    print(f"Loaded {len(rows)} rows into {database}.sales_mart")
    
with DAG(
    dag_id="std18_266_load_clickhouse",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["retail", "clickhouse", "data_mart"],
) as dag:

    load_clickhouse = PythonOperator(
        task_id="load_sales_mart_to_clickhouse",
        python_callable=load_sales_mart_to_clickhouse,
    )
