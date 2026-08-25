-- Auto Generated (Do not modify) 714B48560F5F7E89A06C891BB67C992F3428684D709982046477397EB0D645A6
CREATE VIEW analytics.vw_sales_by_region AS
SELECT
    region,
    FORMAT(order_date, 'yyyy-MM') AS year_month,
    SUM(revenue) AS total_revenue,
    COUNT(order_id) AS total_orders,
    AVG(revenue) AS avg_order_value
FROM staging.silver_sales
GROUP BY
    region,
    FORMAT(order_date, 'yyyy-MM');