-- Auto Generated (Do not modify) 496AA4DE2A62B01C195D643CA141B60AE7F07273FA2AA0DB785D1B7DD5ED0880
CREATE VIEW dbo.vw_top_products AS

SELECT TOP 50
    p.product_name,
    p.category,
    SUM(f.amount) AS revenue
FROM dbo.gold_sales f
JOIN dbo.gold_products p
    ON f.product_id = p.sk_product
GROUP BY
    p.product_name,
    p.category
ORDER BY
    revenue DESC;