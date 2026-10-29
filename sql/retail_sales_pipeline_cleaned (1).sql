-- Retail Sales Data Pipeline
-- Greenplum SQL development script
--
-- This file preserves the original SQL logic and section order.
-- It includes table setup, source loading, checks, and exploratory calculations.
-- Review statements before re-running: some sections are historical/manual steps.
--
------------------------
-- Загрузка магазинов --
------------------------
drop table std18_266_final.stores

create table std18_266_final.stores(
        stores_id text,
        stores_name text
)
distributed replicated


DROP EXTERNAL TABLE IF EXISTS std18_266_final.ext_stores;

create readable external table std18_266_final.ext_stores(
        stores_id text,
        stores_name text
)
LOCATION (
    'gpfdist://172.16.128.86:8080/stores.csv'
)
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
);


insert into std18_266_final.stores
select *
from std18_266_final.ext_stores

select *
from std18_266_final.ext_stores

SELECT
    COUNT(*) AS row_count,
    COUNT(DISTINCT stores_id) AS unique_stores
FROM std18_266_final.stores;


-----------------------
-- Загрузка купонов ---
-----------------------
drop table std18_266_final.coupons

create table std18_266_final.coupons(
    stores_id      TEXT,
    sale_date      date,
    coupon_id      TEXT,
    promotion_id   TEXT,
    product_id     TEXT,
    billnum        bigint
)
distributed replicated

ALTER TABLE std18_266_final.coupons
RENAME COLUMN store_id TO stores_id;

ALTER TABLE std18_266_final.coupons
ALTER COLUMN billnum TYPE BIGINT
USING billnum::BIGINT;

select *
from std18_266_final.coupons


drop external table std18_266_final.coupons_ext

create readable external table std18_266_final.coupons_ext(
    store_id       TEXT,
    sale_date      TEXT,
    coupon_id      TEXT,
    coupon_promo_id TEXT,
    product_id     TEXT,
    receipt_id     bigint
)
LOCATION (
    'gpfdist://172.16.128.86:8080/coupons.csv'
)
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
);

insert into std18_266_final.coupons
select  store_id,
        TO_DATE(sale_date, 'YYYYMMDD'),
        coupon_id,
        coupon_promo_id,
        product_id,
        receipt_id
from std18_266_final.coupons_ext


select *
from std18_266_final.coupons_ext


-----------------------
-- Загрузка Акции -----
-----------------------

drop table std18_266_final.promos

create table std18_266_final.promos(
    promotion_id      text,
    promotion_name    text,
    promotion_type_id text,
    product_id        text,
    discount_amount   INTEGER
)
distributed replicated


drop external table std18_266_final.promos_ext

create readable external table std18_266_final.promos_ext(
    promotion_id      text,
    promotion_name    text,
    promotion_type_id text,
    product_id        text,
    discount_amount   INTEGER
)
LOCATION (
    'gpfdist://172.16.128.86:8080/promos.csv'
)
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
);

select *
from std18_266_final.promos_ext

insert into std18_266_final.promos
select *
from std18_266_final.promos_ext


SELECT COUNT(*) AS total_rows
FROM std18_266_final.promos;

SELECT *
FROM std18_266_final.promos_ext
LIMIT 10;


-----------------------
-- Загрузка Тип Акции -
-----------------------

drop table std18_266_final.promo_types

create table std18_266_final.promo_types(
    promotion_type_id text,
    text              text
)
distributed replicated

drop external table std18_266_final.promo_types_ext

create readable external table std18_266_final.promo_types_ext(
    promotion_type_id text,
    text              text
)
LOCATION (
    'gpfdist://172.16.128.86:8080/promo_types.csv'
)
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
);

insert into std18_266_final.promo_types
select *
from std18_266_final.promo_types_ext

select *
from std18_266_final.promo_types_ext


-----------------------
-- Загрузка traffic ---
-----------------------
drop table std18_266_final.traffic

create table std18_266_final.traffic(
    stores_id text,
    sale_date date,
    hour      time,
    frame_id  text,
    quantity  numeric
)
WITH (
    appendonly = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 1
)
distributed by (stores_id)
PARTITION BY RANGE (sale_date)
(
    START (DATE '2021-01-01')
    INCLUSIVE
    END (DATE '2023-03-01')
    EXCLUSIVE
    EVERY (INTERVAL '1 month')
);


drop external table std18_266_final.traffic_ext

create READABLE external table std18_266_final.traffic_ext(
    plant  TEXT,
    date   TEXT,
    time  TEXT,
    frame_id  TEXT,
    quantity numeric
)
LOCATION (
    'pxf://gp.traffic?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
)
FORMAT 'CUSTOM' (
    FORMATTER='pxfwritable_import'
);

select *
from std18_266_final.traffic_ext

select count(*), count(distinct stores_id)
from std18_266_final.traffic


insert into std18_266_final.traffic(stores_id, sale_date, hour,  frame_id, quantity)
select  plant,
        TO_DATE(date, 'DD.MM.YYYY'),
        time::TIME,
        frame_id,
        quantity
from std18_266_final.traffic_ext

SELECT
    COUNT(*) AS source_rows,
    COUNT(DISTINCT (plant, date, time, frame_id)) AS unique_rows
FROM std18_266_final.traffic_ext;


-----------------------
-- Загрузка Чеки ------
-----------------------
drop external table std18_266_final.bills_item_ext

create readable external table std18_266_final.bills_item_ext(
        billnum  bigint,
        billitem bigint,
        material bigint,
        qty      bigint,
        netval   numeric,
        tax      numeric,
        rpa_sat  numeric,
        calday   date
)
LOCATION (
    'pxf://gp.bills_item?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
)
FORMAT 'CUSTOM' (
    FORMATTER='pxfwritable_import'
);


select *
from std18_266_final.bills_item_ext


drop external table bills_head_ext

create readable external table std18_266_final.bills_head_ext(
        billnum bigint,
        plant   VARCHAR,
        calday  date
)
LOCATION (
    'pxf://gp.bills_head?PROFILE=JDBC&JDBC_DRIVER=org.postgresql.Driver&DB_URL=jdbc:postgresql://192.168.214.212:5432/postgres&USER=intern&PASS=intern'
)
FORMAT 'CUSTOM' (
    FORMATTER='pxfwritable_import'
);

select *
FROM std18_266_final.bills_head_ext


DROP TABLE IF EXISTS std18_266_final.bills;

CREATE TABLE std18_266_final.bills AS
SELECT
    h.billnum,
    i.billitem,
    i.material,
    h.plant,
    h.calday,
    TO_CHAR(h.calday, 'YYYYMMDD')::INTEGER AS day,
    TO_CHAR(h.calday, 'YYYYMM')::INTEGER AS month,
    i.netval + i.tax AS turnover_with_vat,
    i.qty AS units,
    i.netval,
    i.tax,
    ROW_NUMBER() OVER (
        PARTITION BY h.billnum
        ORDER BY i.billitem
    ) AS bill_count
FROM std18_266_final.bills_head_ext h
JOIN std18_266_final.bills_item_ext i
    ON h.billnum = i.billnum
DISTRIBUTED BY (billnum);


select *
from std18_266_final.bills_item_ext


select *
FROM std18_266_final.bills_head_ext


select *
from std18_266_final.bills


drop table std18_266_final.bills_partitioned

CREATE TABLE std18_266_final.bills_partitioned (
    billnum BIGINT,
    billitem INTEGER,
    material BIGINT,
    plant VARCHAR,
    day  date,
    month DATE,
    turnover_with_vat integer,
    units integer,
    netval NUMERIC,
    tax NUMERIC,
    bill_count integer
)
WITH (
    appendonly = true,
    orientation = column,
    compresstype = zstd,
    compresslevel = 1
)
DISTRIBUTED BY (billnum)
PARTITION BY RANGE (day)
(
    START (DATE '2020-11-01')
    INCLUSIVE
    END (DATE '2023-03-01')
    EXCLUSIVE
    EVERY (INTERVAL '1 month')
);


insert into std18_266_final.bills_partitioned
select
    billnum,
    billitem,
    material,
    plant,
    calday AS day,
    DATE_TRUNC('month', calday)::DATE AS month,
    turnover_with_vat,
    units,
    netval,
    tax,
    bill_count
from std18_266_final.bills


select count(*)
FROM std18_266_final.bills_partitioned;

select *
from std18_266_final.bills_item_ext;


select *
FROM std18_266_final.bills_head_ext;


select count(*), count(distinct billnum )
FROM std18_266_final.bills_item_ext;


SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'std18_266_final'
  AND table_name = 'bills_partitioned'
ORDER BY ordinal_position;


select max(day)
from std18_266_final.bills_partitioned bp


------------------------------
-- Расчеты -------------------
------------------------------

drop external table std18_266_final.netval_ext

create readable external table std18_266_final.netval_ext(
    billnum       BIGINT,
    billitem      BIGINT,
    material      VARCHAR,
    plant         VARCHAR(10),
    day        DATE,
    month      INTEGER,
    turnover_with_vat  NUMERIC(12,2),
    units           INTEGER,
    netval    NUMERIC(12,2),
    tax    NUMERIC(12,2),
    bill_count    INTEGER
)
LOCATION (
    'gpfdist://172.16.128.86:8080/netval.csv'
)
FORMAT 'CSV' (
    HEADER
    DELIMITER ','
);


SELECT *
FROM std18_266_final.netval_ext


-- Оборот - turnover
select stores_id, sum(turnover_with_vat) as turnover
from std18_266_final.bills_partitioned bp
where day >= date '2021-01-01' and day < date '2021-03-01'
group by stores_id
order by stores_id


select *
from std18_266_final.coupons c

select *
from std18_266_final.promos p


--Скидки по купонам
select
    c.stores_id,
    SUM(p.discount_amount) AS coupon_discount
FROM std18_266_final.coupons c
JOIN std18_266_final.promos p
    ON c.promotion_id = p.promotion_id
WHERE c.sale_date >= DATE '2021-01-01'
  AND c.sale_date <  DATE '2021-03-01'
GROUP BY c.stores_id


with cub as (
select c.stores_id,
       c.billnum ,
       c.product_id,
       b.turnover_with_vat,
       p.promotion_type_id,
    CASE
        WHEN p.promotion_type_id = '001'
            THEN p.discount_amount::numeric
        WHEN p.promotion_type_id = '002'
            THEN b.turnover_with_vat::numeric
                 * p.discount_amount / 100.0
        ELSE 0
    END AS discount_amount
from std18_266_final.coupons c
join std18_266_final.bills_partitioned b
    ON TRIM(c.product_id) = TRIM(b.product_id)
    and c.billnum = b.billnum
    and c.sale_date = b.day
    and c.stores_id = b.stores_id
join std18_266_final.promos p
    on c.product_id = p.product_id
    and c.promotion_id = p.promotion_id
WHERE c.sale_date >= DATE '2021-01-01'
  AND c.sale_date <  DATE '2021-03-01'
  )
select stores_id, sum(discount_amount)
from cub
group by stores_id
order by stores_id


select *
from std18_266_final.bills_partitioned b;


select *
from std18_266_final.coupons b;

select *
from std18_266_final.promos;


--кол-во проданных товаров
select stores_id, sum(units)
FROM std18_266_final.bills_partitioned b
WHERE b.day >= DATE '2021-01-01'
  AND b.day <  DATE '2021-03-01'
group by stores_id


--Количество чеков

SELECT stores_id, COUNT(DISTINCT billnum) AS total_checks
FROM std18_266_final.bills_partitioned b
WHERE b.day >= DATE '2021-01-01'
  AND b.day <  DATE '2021-03-01'
group by stores_id


-- Трафик

SELECT
    stores_id,
    SUM(quantity) AS total_traffic
FROM std18_266_final.traffic
GROUP BY stores_id
ORDER BY stores_id;


-- Кол-во товаров по акции

select stores_id, count(*)
from std18_266_final.coupons
group by stores_id
order by stores_id


CREATE TABLE std18_266_final.sales_mart_by_month_begin as
WITH
turnover as (
    select stores_id, sum(turnover_with_vat) as turnover
    from std18_266_final.bills_partitioned bp
    where day >= date '2021-01-01' and day < date '2021-03-01'
    group by stores_id
    order by stores_id
    ),
coupon_discount_raw AS (
    SELECT
        c.stores_id,
        c.billnum,
        TRIM(c.product_id) AS product_id,
        c.sale_date,
        c.coupon_id,
        CASE
            WHEN p.promotion_type_id = '001'
                THEN p.discount_amount::numeric
            WHEN p.promotion_type_id = '002'
                THEN b.turnover_with_vat::numeric
                     * p.discount_amount::numeric / 100.0
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
        SUM(discount_amount) AS coupon_discount
    FROM coupon_discount_raw
    GROUP BY stores_id
),
total_units_sold as (
    select stores_id, sum(units) as total_units_sold
    FROM std18_266_final.bills_partitioned b
    WHERE b.day >= DATE '2021-01-01'
      AND b.day <  DATE '2021-03-01'
    group by stores_id
    ),
total_checks AS
    (SELECT
        stores_id,
        COUNT(DISTINCT billnum) AS total_checks
    FROM std18_266_final.bills_partitioned b
    WHERE b.day >= DATE '2021-01-01'
      AND b.day <  DATE '2021-03-01'
    group by stores_id
    ),
traffic_agg AS (
    SELECT
        stores_id,
        SUM(quantity) AS total_traffic
    FROM std18_266_final.traffic
    GROUP BY stores_id
),
coupons_agg AS (
    SELECT
        stores_id,
        COUNT(*) AS promo_products
    FROM std18_266_final.coupons
    GROUP BY stores_id
)
SELECT
    s.stores_id,
    s.stores_name as Магазин,
    tur.turnover, -- оборот
    COALESCE(dis.coupon_discount, 0) AS coupon_discount,
    tur.turnover - COALESCE(dis.coupon_discount, 0) AS turnover_with_dis,
    sold.total_units_sold,
    tot.total_checks,
    t.total_traffic AS total_traffic,
    c.promo_products AS promo_products,
    round(COALESCE(c.promo_products, 0)::numeric
    / NULLIF(sold.total_units_sold, 0) * 100, 1)
    AS promo_products_pct,
    ROUND(sold.total_units_sold::numeric / NULLIF(tot.total_checks, 0), 2) AS Ср_кол_тов_в_чеке,
    round(tot.total_checks / t.total_traffic * 100, 2) as Коэф_маг,
    ROUND(tur.turnover::numeric / NULLIF(tot.total_checks, 0), 1) AS Ср_оборот_на_чек,
    CASE
    WHEN t.total_traffic = 0 OR t.total_traffic IS NULL THEN 0
    ELSE ROUND(tur.turnover::numeric / t.total_traffic, 1)
END AS Средняя_выручка
FROM std18_266_final.stores s
LEFT JOIN traffic_agg t
    ON s.stores_id = t.stores_id
LEFT JOIN coupons_agg c
    ON s.stores_id = c.stores_id
left join total_checks tot
    on s.stores_id = tot.stores_id
left join total_units_sold sold
    on s.stores_id = sold.stores_id
LEFT JOIN coupon_discount dis
    ON s.stores_id = dis.stores_id
left join turnover tur
    on s.stores_id = tur.stores_id
order by stores_id


select *
from std18_266_final.sales_mart_by_month


create table std18_266_final.sales_mart_by_month (
    stores_id   text,
    stores_name text,
    turnover    int,
    coupon_discount numeric,
    turnover_with_dis   numeric,
    total_units_sold    int,
    total_checks    int,
    total_traffic   numeric,
    promo_products  int,
    promo_products_pct  numeric,
    avg_units_per_check numeric,
    store_conversion    numeric,
    avg_turnover_per_check  numeric,
    avg_revenue_per_visitor numeric
)
distributed by (stores_id)


insert into std18_266_final.sales_mart_by_month
select *
from std18_266_final.sales_mart_by_month_begin


drop TABLE std18_266_final.sales_mart


CREATE TABLE std18_266_final.sales_mart as
WITH
turnover AS (
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
                THEN p.discount_amount::numeric
            WHEN p.promotion_type_id = '002'
                THEN b.turnover_with_vat::numeric
                     * p.discount_amount::numeric / 100.0
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
    s.stores_name AS stores_name,
    tur.turnover,
    COALESCE(dis.coupon_discount, 0) AS coupon_discount,
    tur.turnover - COALESCE(dis.coupon_discount, 0)
        AS turnover_with_dis,
    sold.total_units_sold,
    tot.total_checks,
    COALESCE(t.total_traffic, 0) AS total_traffic,
    COALESCE(c.promo_products, 0) AS promo_products,
    ROUND(
        COALESCE(c.promo_products, 0)::numeric
        / NULLIF(sold.total_units_sold, 0) * 100,
        1
    ) AS promo_products_pct,
    ROUND(
        sold.total_units_sold::numeric
        / NULLIF(tot.total_checks, 0),
        2
    ) AS avg_units_per_check,
    ROUND(
        tot.total_checks::numeric
        / NULLIF(t.total_traffic, 0) * 100,
        2
    ) AS store_conversion,
    ROUND(
        tur.turnover::numeric
        / NULLIF(tot.total_checks, 0),
        1
    ) AS avg_turnover_per_check,
    CASE
        WHEN COALESCE(t.total_traffic, 0) = 0 THEN 0
        ELSE ROUND(
            tur.turnover::numeric / t.total_traffic,
            1
        )
    END AS avg_revenue_per_visitor
FROM turnover tur
JOIN std18_266_final.stores s
    ON s.stores_id = tur.stores_id
LEFT JOIN traffic_agg t
    ON t.stores_id = tur.stores_id
   AND t.sale_date  = tur.day
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
   AND dis.day = tur.day
ORDER BY tur.day, s.stores_id;


SELECT *
FROM std18_266_final.sales_mart


drop table std18_266_final.sales_mart_test

create table std18_266_final.sales_mart_test (
    day date,
    stores_id   text,
    stores_name text,
    turnover    int,
    coupon_discount numeric,
    turnover_with_dis   numeric,
    total_units_sold    int,
    total_checks    int,
    total_traffic   numeric,
    promo_products  int,
    promo_products_pct  numeric,
    avg_units_per_check numeric,
    store_conversion    numeric,
    avg_turnover_per_check  numeric,
    avg_revenue_per_visitor numeric
)
distributed by (stores_id)


insert into std18_266_final.sales_mart_test
select *
from std18_266_final.sales_mart


select *
from std18_266_final.sales_mart_test


SELECT
    COUNT(*) AS row_count,
    MIN(day) AS min_day,
    MAX(day) AS max_day
FROM std18_266_final.sales_mart_test;
