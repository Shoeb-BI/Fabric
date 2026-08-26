CREATE TABLE [analytics].[dim_date] (

	[order_date_key] int NOT NULL, 
	[full_date] date NOT NULL, 
	[year] int NOT NULL, 
	[month_number] int NOT NULL, 
	[month_name] varchar(20) NOT NULL, 
	[quarter] int NOT NULL, 
	[day_of_week] varchar(20) NOT NULL, 
	[is_weekend] bit NOT NULL
);