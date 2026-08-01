-- Auto Generated (Do not modify) 498D6C220CA969E936FEF3FDF930367E00909166B844DE4F68E368EFCCB0CD09
CREATE VIEW dbo.vw_sales_vs_target AS

SELECT
    z.zone,
    z.Revenue,
    SUM(t.target_amount) AS target,
    z.Revenue - SUM(t.target_amount) AS variance
FROM dbo.view_Zonewise_Sales z
JOIN dbo.gold_stores st
    ON st.zone = z.zone
JOIN dbo.gold_target t
    ON t.store_id = st.sk_store
GROUP BY
    z.zone,
    z.Revenue;