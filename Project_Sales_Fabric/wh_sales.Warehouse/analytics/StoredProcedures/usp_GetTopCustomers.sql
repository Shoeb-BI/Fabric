CREATE PROCEDURE analytics.usp_GetTopCustomers
    @TopN INT = 10
AS
BEGIN
    SELECT TOP (@TopN)
        customer_name,
        SUM(revenue) AS total_revenue,
        COUNT(order_id) AS total_orders
    FROM staging.silver_sales
    GROUP BY customer_name
    ORDER BY total_revenue DESC;
END;