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

# Cell 1 : Read silver sales
df = spark.read.format('delta').load('Tables/dbo/silver_sales')
print(f'silver_sales rows: {df.count()}')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 2: Build dim_customer
from pyspark.sql.functions import monotonically_increasing_id

dim_customer = df.select('customer_name', 'region') \
    .distinct() \
    .withColumn('customer_key', monotonically_increasing_id() + 1)

# Reorder columns: key first
dim_customer = dim_customer.select(
    'customer_key',
    'customer_name',
    'region'
)

dim_customer.write.format('delta').mode('overwrite').saveAsTable('dim_customer')

print(f'dim_customer rows: {dim_customer.count()}')
display(dim_customer.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 3: Build dim_product
# Create a product dimension from unique product categories

# Select product category and remove duplicate categories
dim_product = df.select('product_category') \
    .distinct() \
    .withColumn('product_key', monotonically_increasing_id() + 1)

# Reorder columns so the dimension key comes first
dim_product = dim_product.select(
    'product_key',
    'product_category'
)

# Write the dimension as a Delta table
# overwrite replaces existing table data if the table already exists
dim_product.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('dim_product')

# Validate row count and display sample data
print(f'dim_product rows: {dim_product.count()}')
display(dim_product)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 4: Build dim_region
from pyspark.sql.functions import monotonically_increasing_id

# Select unique regions and generate a unique region key
dim_region = df.select('region') \
    .distinct() \
    .withColumn('region_key', monotonically_increasing_id() + 1)

# Reorder columns so the dimension key comes first
dim_region = dim_region.select(
    'region_key',
    'region'
)

# Save the region dimension as a Delta table
# overwrite replaces existing data if the table already exists
dim_region.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('dim_region')

# Validate row count and display the result
print(f"dim_region rows: {dim_region.count()}")
display(dim_region)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cell 5: Build fact_sales by joining silver sales to each dimension

from pyspark.sql.functions import col, to_date, date_format

# Re-read silver sales and all dimension tables
df_silver = spark.read.format('delta').load('Tables/dbo/silver_sales')
df_customer = spark.read.format('delta').load('Tables/dbo/dim_customer')
df_product = spark.read.format('delta').load('Tables/dbo/dim_product')
df_region = spark.read.format('delta').load('Tables/dbo/dim_region')

# Join silver sales with dimensions to resolve surrogate keys
fact = df_silver \
    .join(
        df_customer,
        (df_silver.customer_name == df_customer.customer_name) &
        (df_silver.region == df_customer.region),
        'left'
    ) \
    .join(
        df_product,
        df_silver.product_category == df_product.product_category,
        'left'
    ) \
    .join(
        df_region,
        df_silver.region == df_region.region,
        'left'
    )

# Build the date surrogate key in YYYYMMDD integer format
fact = fact.withColumn(
    'order_date_key',
    date_format(
        to_date(col('order_date'), 'yyyy-MM-dd'),
        'yyyyMMdd'
    ).cast('int')
)

# Select only the required fact table columns
fact_sales = fact.select(
    col('order_id'),
    col('order_date_key'),
    col('customer_key'),
    col('product_key'),
    col('region_key'),
    col('revenue'),
    col('quantity'),
    col('revenue_usd')
)

# Save the fact table as a Delta table
# overwrite replaces existing data if the table already exists
fact_sales.write \
    .format('delta') \
    .mode('overwrite') \
    .saveAsTable('fact_sales')

# Validate row count and display sample records
print(f"fact_sales rows: {fact_sales.count()}")
display(fact_sales.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
