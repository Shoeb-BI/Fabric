# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "48fdc633-8d87-4333-a755-2ba1bc3d5bec",
# META       "default_lakehouse_name": "LH_DP600",
# META       "default_lakehouse_workspace_id": "97365ca9-d891-4c18-a19c-c64a1e6c5a15",
# META       "known_lakehouses": [
# META         {
# META           "id": "48fdc633-8d87-4333-a755-2ba1bc3d5bec"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
excel_file_path = "https://github.com/amitchandakpbi/powerbi/raw/main/Sales%20Data%20for%20Fabric.xlsx"
 
# Use pandas to read the Excel file
df_sales = pd.read_excel(excel_file_path, sheet_name="Sales")
df_customer = pd.read_excel(excel_file_path, sheet_name="Customer")
df_geo = pd.read_excel(excel_file_path, sheet_name="Geography")
df_item = pd.read_excel(excel_file_path, sheet_name="Item")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df_sales.head(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_sales)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales = spark.createDataFrame(df_sales)
customer = spark.createDataFrame(df_customer)
geography = spark.createDataFrame(df_geo)
item = spark.createDataFrame(df_item)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Describe the DataFrames
sales.describe().show()
geography.describe().show()
customer.describe().show()
item.describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(geography.summary())
display(sales.summary())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_customer_1_to_60 = customer.select("CustomerId").filter(customer.CustomerId.between(1, 60))
df_customer_40_to_100  = customer.select("CustomerId").filter(customer.CustomerId.between(40, 100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_filter = sales.filter(sales.CustomerId.between(59, 62))
customer_filter = customer.filter(customer.CustomerId.between(50, 60))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#inner
inner_join_df = sales_filter.join(customer_filter, "CustomerId", "inner")
print("Inner Join DataFrame:")
display(inner_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#left
left_join_df = sales_filter.join(customer_filter, "CustomerId", "left")
print("Inner Join DataFrame:")
display(left_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Right
Right_join_df = sales_filter.join(customer_filter, "CustomerId", "Right")
print("Right Join DataFrame:")
display(Right_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fullouter
fullouter_join_df = sales_filter.join(customer_filter, "CustomerId", "fullouter")
print("fullouter Join DataFrame:")
display(fullouter_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# leftanti
leftanti_join_df = customer_filter.join(sales_filter, "CustomerId", "anti")

print("leftanti Join DataFrame:")
display(leftanti_join_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer = customer.drop('State', 'City')
display(customer)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_customer = sales.join(customer, "CustomerId")
sales_customer_geo = sales_customer.join(geography, "CityId")
# Perform the join operation
sales_all = sales_customer_geo.join(item, sales.ItemID == item.ItemId, "inner")
 
# Show the result of the join
print("Inner Join DataFrame:")
display(sales_all)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_all.distinct()
display(sales_all)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, expr
 
# Adding calculated columns
sales_all = sales_all.withColumn("Gross", col("Qty") * col("Price"))
sales_all = sales_all.withColumn("COGS", col("Qty") * col("Cost"))
sales_all = sales_all.withColumn("Discount", col("Qty") * col("Price") * col("DiscountPercent")/100)
 
# Display the DataFrame with new columns
display(sales_all)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Grouping by City and State
grouped_city_state = sales_all.groupBy("City", "State").agg(
    expr("sum(Gross) as TotalGross"),
    expr("sum(COGS) as TotalCOGS"),
    expr("sum(Discount) as TotalDiscount")
)
grouped_city_state.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Grouping by Brand and Category
grouped_brand_category = sales_all.groupBy("Brand", "Category").agg(
    expr("sum(Gross) as TotalGross"),
    expr("sum(COGS) as TotalCOGS"),
    expr("avg(Qty) as TotalQty"),
    expr("sum(Discount) as TotalDiscount")
)
grouped_brand_category.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales.write.format("delta").saveAsTable("dbo.sales_delta")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#another way to write
geography.write.format("delta").mode("overwrite").save("Tables/dbo/geography")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM LH_DP600.dbo.geography LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.table("LH_DP600.dbo.sales_delta "))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
 
delta_table = DeltaTable.forName(spark, "dbo.sales_delta")
 
delta_table.update(
    condition="Qty = 2",
    set={"Qty": "4"}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.table("LH_DP600.dbo.sales_delta "))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(delta_table.history())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table.delete("Qty=3")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(delta_table.history())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
 
DeltaTable.forName(spark, "sales_delta").history().show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.read.option("versionAsOf", 0).table("sales_delta"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delta_table.restoreToVersion(0)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(delta_table.history())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
