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
