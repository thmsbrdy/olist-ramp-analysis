import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import os 

# ensure relatives paths work regardless of where the script is run from
os.chdir(os.path.dirname(os.path.abspath(__file__))) 

# connect to local Postgres database
engine = create_engine("postgresql://thomasbrady@localhost/olist") 

query = """
    WITH weekly AS (
    SELECT
        oi.seller_id
        , FLOOR(EXTRACT(DAY FROM (o.order_purchase_timestamp::timestamp - first_orders.first_purchase_date::timestamp)) / 7) + 1 AS week_number
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
        WHERE first_orders.first_purchase_date < '2017-07-01'
        GROUP BY oi.seller_id, week_number
),
wow AS (
    SELECT
        seller_id
        , week_number
        , weekly_gms
        , LAG(weekly_gms) OVER (PARTITION BY seller_id ORDER BY week_number) AS previous_week_gms
        , ROUND(((weekly_gms - LAG(weekly_gms) OVER (PARTITION BY seller_id ORDER BY week_number)) / LAG(weekly_gms) OVER (PARTITION BY seller_id ORDER BY week_number) * 100)::numeric, 2) AS wow_change
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
    w.seller_id,
    w.week_number,
    w.weekly_gms,
    c.cohort
FROM wow w
JOIN cohorts c ON w.seller_id = c.seller_id
WHERE w.week_number <= 52
ORDER BY w.seller_id, w.week_number;
"""

# execute query and load results into a DataFrame
df = pd.read_sql(query, engine) 

# calculate average weekly GMS per cohort per week
avg_by_cohort = df.groupby(["cohort", "week_number"])["weekly_gms"].mean().reset_index()

cohorts = ["fast_ramp", "slow_ramp", "did_not_stabilize"]
colors = ["green", "orange", "red"]

# plot smoothed ramp curves for each cohort
plt.figure(figsize=(12, 6))
for cohort, color in zip(cohorts, colors):
    cohort_data = avg_by_cohort[avg_by_cohort["cohort"] == cohort].copy()
    cohort_data["weekly_gms_smoothed"] = cohort_data["weekly_gms"].rolling(window=4, min_periods=1).mean()
    plt.plot(cohort_data["week_number"], cohort_data["weekly_gms_smoothed"], label=cohort, color=color)

plt.xlabel("Week Number")
plt.ylabel("Average Weekly Revenue")
plt.title("Average Revenue Ramp Curve by Cohort")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("ramp_curve.png")
plt.show()