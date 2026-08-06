CREATE TABLE [dbo].[DimCustomer_new] (

	[CustomerKey] bigint IDENTITY NOT NULL, 
	[CustomerID] varchar(20) NOT NULL, 
	[CustomerName] varchar(100) NOT NULL, 
	[Email] varchar(255) NULL, 
	[Gender] varchar(20) NULL, 
	[DOB] date NULL, 
	[City] varchar(100) NULL, 
	[State] varchar(100) NULL, 
	[Country] varchar(100) NULL, 
	[JoinDate] date NULL, 
	[CustomerType] varchar(50) NULL
);