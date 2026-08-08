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
ALTER TABLE [dbo].[f_sales] ADD CONSTRAINT FK_Sales_Date FOREIGN KEY ([Datekey]) REFERENCES [dbo].[d_Date]([Datekey]);