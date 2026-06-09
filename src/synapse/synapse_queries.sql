-- =============================================================================
-- Azure Synapse Analytics: Gold Layer Analytical Queries
-- Author: Ashok | MS CS @ Auburn University at Montgomery
-- Description: Production-ready SQL queries for BI reporting on Gold Delta tables
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Top 10 Products by Total Revenue
-- -----------------------------------------------------------------------------
SELECT
    product_id,
    country,
    SUM(total_revenue)    AS total_revenue,
    SUM(order_count)      AS total_orders,
    AVG(avg_order_value)  AS avg_order_value
FROM gold.revenue_by_product
GROUP BY product_id, country
ORDER BY total_revenue DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 2. Daily Revenue Trend (rolling 30 days)
-- -----------------------------------------------------------------------------
SELECT
    CAST(last_purchase_ts AS DATE)   AS purchase_date,
    SUM(total_revenue)               AS daily_revenue,
    SUM(order_count)                 AS daily_orders
FROM gold.revenue_by_product
WHERE last_purchase_ts >= DATEADD(day, -30, GETDATE())
GROUP BY CAST(last_purchase_ts AS DATE)
ORDER BY purchase_date;


-- -----------------------------------------------------------------------------
-- 3. High-Value Customers (Lifetime Value > $100)
-- -----------------------------------------------------------------------------
SELECT
    user_id,
    lifetime_value,
    total_orders,
    DATEDIFF(day, first_purchase_ts, last_purchase_ts) AS customer_lifetime_days
FROM gold.user_purchase_summary
WHERE lifetime_value > 100
ORDER BY lifetime_value DESC;


-- -----------------------------------------------------------------------------
-- 4. Multi-Device Users (engaged on more than one device)
-- -----------------------------------------------------------------------------
SELECT
    user_id,
    lifetime_value,
    total_orders
FROM gold.user_purchase_summary
WHERE ARRAY_LENGTH(devices_used) > 1
ORDER BY lifetime_value DESC;


-- -----------------------------------------------------------------------------
-- 5. Revenue Share by Country
-- -----------------------------------------------------------------------------
SELECT
    country,
    SUM(total_revenue)                                    AS country_revenue,
    ROUND(
        100.0 * SUM(total_revenue) / SUM(SUM(total_revenue)) OVER (),
        2
    )                                                     AS revenue_pct
FROM gold.revenue_by_product
GROUP BY country
ORDER BY country_revenue DESC;
