-- Auto Generated (Do not modify) 4A246B98CC0476D4E80B6C8A189890C03D673FA99B5D65AE49A8DF685547099B
CREATE VIEW dbo.view_Zonewise_Sales AS

SELECT
    st.zone,
    SUM(sale.amount) AS Revenue,
    SUM(sale.quantity) AS Units
FROM dbo.gold_sales sale
JOIN dbo.gold_stores st
    ON sale.store_id = st.sk_store
GROUP BY st.zone;