-- Query 1: first and last order date for each seller, and total number of orders
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

-- Query 2: get sellers weekly sales, week number relative to their first order
SELECT
    oi.seller_id
    , FLOOR(EXTRACT(DAY FROM AGE(
        o.order_purchase_timestamp :: timestamp
        , first_orders.first_purchase_date :: timestamp
    )) / 7) + 1 AS week_number
    , ROUND((SUM(oi.price + oi.freight_value) :: numeric), 2) AS weekly_GMS
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

