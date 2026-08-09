CREATE TABLE [dbo].[gold_sales] (

	[sale_id] bigint NULL, 
	[customer_id] bigint NULL, 
	[product_id] bigint NULL, 
	[store_id] bigint NULL, 
	[sale_date] date NULL, 
	[quantity] int NULL, 
	[unit_price] decimal(18,2) NULL, 
	[amount] decimal(18,2) NULL
);