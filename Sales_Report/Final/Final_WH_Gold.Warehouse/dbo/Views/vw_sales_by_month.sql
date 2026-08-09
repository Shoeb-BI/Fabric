-- Auto Generated (Do not modify) 1D5815A6CFF3F78015ED0FFF3C950E068E10AF5FDF8D6A2A668DC70A17B774AD
CREATE VIEW dbo.vw_sales_by_month as

SELECT
    YEAR(sale_date) AS Sales_Year,
    MONTH(sale_date) AS Sales_Month,
    SUM(amount) AS Total_Sales
FROM dbo.gold_sales
GROUP BY
    YEAR(sale_date),
    MONTH(sale_date);