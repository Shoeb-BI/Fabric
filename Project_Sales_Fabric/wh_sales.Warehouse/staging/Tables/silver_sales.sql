CREATE TABLE [staging].[silver_sales] (

	[order_id] varchar(8000) NULL, 
	[order_date] date NULL, 
	[customer_name] varchar(8000) NULL, 
	[region] varchar(8000) NULL, 
	[product_category] varchar(8000) NULL, 
	[revenue] float NULL, 
	[quantity] int NULL, 
	[status] varchar(8000) NULL, 
	[revenue_usd] float NULL
);