CREATE TABLE [dbo].[d_Date] (

	[Datekey] int NOT NULL, 
	[FullDate] int NOT NULL, 
	[Year] int NOT NULL, 
	[Quarter] int NOT NULL, 
	[Month] int NOT NULL, 
	[MonthName] varchar(10) NOT NULL, 
	[Day] int NOT NULL, 
	[DayOfWeek] int NOT NULL, 
	[FiscalYear] int NOT NULL, 
	[FiscalQuarter] int NOT NULL, 
	[IsHoliday] bit NOT NULL, 
	[IsWeekDay] bit NOT NULL
);


GO
ALTER TABLE [dbo].[d_Date] ADD CONSTRAINT PK_d_Date primary key NONCLUSTERED ([Datekey]);