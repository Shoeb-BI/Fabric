CREATE TABLE [dbo].[f_sales] (

	[Datekey] int NOT NULL, 
	[Storekey] int NOT NULL, 
	[Productkey] int NOT NULL, 
	[Customerkey] int NOT NULL, 
	[Quantity] int NOT NULL, 
	[Unitprice] decimal(10,2) NOT NULL, 
	[SalesAmount] decimal(10,2) NOT NULL, 
	[DiscountAmount] decimal(10,2) NOT NULL
);


GO
ALTER TABLE [dbo].[f_sales] ADD CONSTRAINT FK_Sales_Customer FOREIGN KEY ([Customerkey]) REFERENCES [dbo].[d_Customer]([CustomerKey]);
GO
ALTER TABLE [dbo].[f_sales] ADD CONSTRAINT FK_Sales_Date FOREIGN KEY ([Datekey]) REFERENCES [dbo].[d_Date]([Datekey]);
GO
ALTER TABLE [dbo].[f_sales] ADD CONSTRAINT FK_Sales_Product FOREIGN KEY ([Productkey]) REFERENCES [dbo].[d_Product]([ProductKey]);
GO
ALTER TABLE [dbo].[f_sales] ADD CONSTRAINT FK_Sales_Store FOREIGN KEY ([Storekey]) REFERENCES [dbo].[d_Store]([StoreKey]);