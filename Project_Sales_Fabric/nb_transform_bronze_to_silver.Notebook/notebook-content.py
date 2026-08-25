# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4b33216a-b082-44e7-906e-cd691c89b967",
# META       "default_lakehouse_name": "lh_sales",
# META       "default_lakehouse_workspace_id": "b83b0226-1908-4d19-9490-0702602d67b4",
# META       "known_lakehouses": [
# META         {
# META           "id": "4b33216a-b082-44e7-906e-cd691c89b967"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Cell 1: Read the bronze_sales Delta table
df = spark.read.format('delta').load('Tables/dbo/bronze_sales')

# Show the first 10 rows
display(df)
#show in another way
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#To show the datatypes
df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Transformation 1 - Filter rows
# Keep only active customers with positive revenue
df_filtered = df.filter(
    (df['Status'] == 'Active') & (df['Revenue'] > 0)
)

# Check how many rows remain
print(f"Original row count: {df.count()}")
print(f"Filtered row count: {df_filtered.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Transformation 2 - Rename columns to snake_case
df_renamed = df_filtered \
    .withColumnRenamed('OrderID', 'order_id') \
    .withColumnRenamed('OrderDate', 'order_date') \
    .withColumnRenamed('CustomerName', 'customer_name') \
    .withColumnRenamed('Region', 'region') \
    .withColumnRenamed('ProductCategory', 'product_category') \
    .withColumnRenamed('Revenue', 'revenue') \
    .withColumnRenamed('Quantity', 'quantity') \
    .withColumnRenamed('Status', 'status')

display(df_renamed.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Fix order_date from string to proper DateType
from pyspark.sql.functions import to_date, col

df_renamed = df_renamed.withColumn(
    'order_date',
    to_date(col('order_date'), 'dd-MM-yyyy')
)

# Verify if it worked
print(df_renamed.schema['order_date'].dataType)
display(df_renamed.select('order_id', 'order_date').limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Transformation 3 - Add calculated column
from pyspark.sql.functions import col, round

# Apply a fixed INR to USD conversion rate (example: 1 USD = 83 INR)
EXCHANGE_RATE = 83.0

df_transformed = df_renamed.withColumn(
    'revenue_usd',
    round(col('revenue') / EXCHANGE_RATE, 2)
)

display(df_transformed.select('order_id', 'revenue', 'revenue_usd').limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 6: Write the transformed DataFrame as silver_sales Delta table

df_transformed.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema','true')\
    .saveAsTable('silver_sales')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
