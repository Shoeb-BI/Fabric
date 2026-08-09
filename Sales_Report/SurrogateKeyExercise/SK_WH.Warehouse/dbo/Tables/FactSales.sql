CREATE TABLE [dbo].[FactSales] (

	[OrderID] int NOT NULL, 
	[CustomerKey] int NOT NULL, 
	[SalesAmount] decimal(10,2) NOT NULL, 
	[Quantity] int NOT NULL
);