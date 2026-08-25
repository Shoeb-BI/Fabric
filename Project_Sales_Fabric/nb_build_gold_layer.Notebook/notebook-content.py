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

# Cell 1: Read the silver_sales Delta table

df_silver = spark.read.format('delta').load('Tables/dbo/silver_sales')

print(f"Row count: {df_silver.count()}")
print(f"Columns: {df_silver.columns}")
display(df_silver.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Extract year and month from order_date
from pyspark.sql.functions import col, to_date, date_format, sum, count, round, avg

# Convert order_date string to proper DateType first
df_dated = df_silver.withColumn(
    'order_date',
    to_date(col('order_date'), 'dd-MM-yyyy')
)

# Add year_month column for grouping
df_dated = df_dated.withColumn(
    'year_month',
    date_format(col('order_date'), 'yyyy-MM')
)

display(df_dated.select('order_id', 'order_date', 'year_month').limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Aggregate data by region and month

df_gold = df_dated.groupBy('region', 'year_month') \
    .agg(
        round(sum('revenue'), 2).alias('total_revenue'),
        count('order_id').alias('total_orders'),
        round(avg('revenue'), 2).alias('avg_order_value')
    ) \
    .orderBy('year_month', 'region')

print(f"Gold table row count: {df_gold.count()}")
display(df_gold)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Write the Gold layer to a Delta table

df_gold.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('gold_sales_summary')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
