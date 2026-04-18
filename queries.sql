-- ============================================================
-- Query 1: Seller activity summary
-- First order date, last order date, and total orders per seller
-- ============================================================

SELECT 
    oi.seller_id
    , MIN(o.order_purchase_timestamp) AS first_purchase_date
    , MAX(o.order_purchase_timestamp) AS last_purchase_date
    , COUNT(DISTINCT o.order_id) AS total_orders    
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
GROUP BY oi.seller_id
ORDER BY first_purchase_date
LIMIT 5;

-- ============================================================
-- Query 2: Weekly Revenue per seller relative to launch
-- Calculates Revenue per week indexed to each seller's first order date
-- ============================================================

SELECT
    oi.seller_id
    , FLOOR(EXTRACT(DAY FROM (
        o.order_purchase_timestamp::timestamp - first_orders.first_purchase_date::timestamp
    )) / 7) + 1 AS week_number
    , ROUND((SUM(oi.price + oi.freight_value) :: numeric), 2) AS weekly_Revenue
FROM order_items oi
JOIN orders o ON oi.order_id = o.order_id
JOIN (
    SELECT
        oi2.seller_id
        , MIN(o2.order_purchase_timestamp) AS first_purchase_date
    FROM order_items oi2
    JOIN orders o2 ON oi2.order_id = o2.order_id   
    GROUP BY oi2.seller_id
) AS first_orders ON oi.seller_id = first_orders.seller_id
GROUP BY oi.seller_id, week_number
ORDER BY oi.seller_id, week_number
LIMIT 5;

-- ============================================================
-- Query 3: Steady state cohort segmentation
-- Identifies when each seller reaches steady state
-- using 2 consecutive weeks with WoW change < 10%
-- Tags sellers as fast_ramp, slow_ramp, or did_not_stabilize
-- ============================================================

WITH weekly AS (
    SELECT
        oi.seller_id
        , FLOOR(EXTRACT(DAY FROM (o.order_purchase_timestamp::timestamp - first_orders.first_purchase_date::timestamp)) / 7) + 1 AS week_number
        , ROUND((SUM(oi.price + oi.freight_value) :: numeric), 2) AS weekly_Revenue
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN (
        SELECT
            oi2.seller_id
            , MIN(o2.order_purchase_timestamp) AS first_purchase_date
            FROM order_items oi2
            JOIN orders o2 ON oi2.order_id = o2.order_id   
            GROUP BY oi2.seller_id
        ) AS first_orders ON oi.seller_id = first_orders.seller_id
        WHERE first_orders.first_purchase_date < '2017-07-01'
        GROUP BY oi.seller_id, week_number
),
wow AS (
    SELECT
        seller_id
        , week_number
        , weekly_revenue
        , LAG(weekly_revenue) OVER (PARTITION BY seller_id ORDER BY week_number) AS previous_week_revenue
        , ROUND(((weekly_revenue - LAG(weekly_revenue) OVER (PARTITION BY seller_id ORDER BY week_number)) / LAG(weekly_revenue) OVER (PARTITION BY seller_id ORDER BY week_number) * 100)::numeric, 2) AS wow_change
    FROM weekly
),
steady AS (
    SELECT
        seller_id
        , week_number AS steady_state_week
    FROM (
        SELECT
            seller_id
            , week_number
            , wow_change
            , LEAD(wow_change) OVER (PARTITION BY seller_id ORDER BY week_number) AS next_wow_change
            , LEAD(week_number) OVER (PARTITION BY seller_id ORDER BY week_number) AS next_week_number
        FROM wow
    ) AS lagged
    WHERE ABS(wow_change) < 10
    AND ABS(next_wow_change) < 10
    AND next_week_number = week_number + 1
),
cohorts AS (
    SELECT
        seller_id
        , steady_state_week
        , CASE
            WHEN steady_state_week <= 16 THEN 'fast_ramp'
            WHEN steady_state_week <= 52 THEN 'slow_ramp'
            ELSE 'did_not_stabilize'
        END AS cohort
    FROM (
        SELECT
            seller_id
            , MIN(steady_state_week) AS steady_state_week
        FROM steady
        GROUP BY seller_id
    ) AS steady_summary
)
SELECT
    cohort
    , COUNT(*) AS num_sellers
    , ROUND(AVG(steady_state_week), 1) AS avg_steady_state_week
FROM cohorts
GROUP BY cohort
ORDER BY avg_steady_state_week;