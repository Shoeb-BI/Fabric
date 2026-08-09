# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1042578f-a917-48cd-957a-35c6985b3258",
# META       "default_lakehouse_name": "BharatMart_Training_Bronze",
# META       "default_lakehouse_workspace_id": "d1847af3-63f2-4cdb-ac61-879503d263b2",
# META       "known_lakehouses": [
# META         {
# META           "id": "1042578f-a917-48cd-957a-35c6985b3258"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
Customer_br_tbl=spark.read.table("BharatMart_Training_Bronze.dbo.dim_customers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customer_br_file =spark.read

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#to see schema of table for datatype
Customer_br_tbl.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customer_br_tbl.show(5,truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customer_br_tbl.count()
Customer_br_tbl.show(Customer_br_tbl.count(),truncate=False)#dispaly complete table with full width

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#fill null where email is missing
df=Customer_br_tbl.fillna({"email":"Unknown@bharatmart.com"}) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.show(df.count(),truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType
 
# 1) Fill null emails
Customer_br_tbl = Customer_br_tbl.fillna({"email": "unknown@bharatmart.com"})
 
# 2) Trim the key so "1002" and "1002 " group together
Customer_br_tbl = Customer_br_tbl.withColumn("customer_id", F.trim(F.col("customer_id")))

 
# 3) Dedupe on customer_id, KEEP LATEST:
#    rank each customer's rows newest-first, keep rank 1
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())
Customer_br_tbl = (Customer_br_tbl
      .withColumn("_rn", F.row_number().over(w))
      .filter(F.col("_rn") == 1)
      .drop("_rn"))
 
# 4) Cast types (bad dates -> null, not an error)
Customer_br_tbl = (Customer_br_tbl
      .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
      .withColumn("birth_date",  F.to_date(F.col("birth_date"), "yyyy-MM-dd").cast(DateType())))

      


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Customer_br_tbl = (
    Customer_br_tbl
      .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
      .withColumn("birth_date", F.to_date(F.col("birth_date"), "yyyy-MM-dd"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Product_br_table=spark.read.table("BharatMart_Training_Bronze.dbo.dim_products")
store_br_table=spark.read.table("BharatMart_Training_Bronze.dbo.dim_stores")
target_br_table=spark.read.table("BharatMart_Training_Bronze.dbo.dim_targets")
sales_br_table=spark.read.table("BharatMart_Training_Bronze.dbo.fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

Product_br_table.show(Product_br_table.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType

# Clean Product table
Product_br_table = (
    Product_br_table
    .withColumn("product_name", F.trim(F.col("product_name")))
    .fillna({"category": "unknown"})
    .withColumn("price", F.col("price").cast(DoubleType()))
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))
)

# Display data
Product_br_table.orderBy("product_id").show(truncate=False)
Product_br_table.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType

store_br_table = (
    store_br_table
    # 1) Normalize: trim + uppercase
    .withColumn("zone", F.upper(F.trim(F.col("zone"))))

    # 2) Map zone values
    .withColumn(
        "zone",
        F.when(F.col("zone").isin("N", "NORTH"), "N")
         .when(F.col("zone").isin("S", "SOUTH"), "S")
         .when(F.col("zone").isin("E", "EAST"), "E")
         .when(F.col("zone").isin("W", "WEST"), "W")
         .otherwise("Unknown")
    )

    # 3) Convert data types
    .withColumn("open_date", F.to_date(F.col("open_date"), "dd-MM-yyyy").cast(DateType()))
    .withColumn("store_id", F.col("store_id").cast(IntegerType()))
)

store_br_table.orderBy("store_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

target_br_table = (
    target_br_table
    # Cast target_amount to DoubleType
    .withColumn("target_amount", F.col("target_amount").cast(DoubleType()))

    # Validate month (1–12)
    .withColumn(
        "month",
        F.when(F.col("month").between(1, 12), F.col("month"))
         .otherwise(None)     # Invalid months become NULL
    )
)

target_br_table.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DateType, DoubleType

sales_br_table = (
    sales_br_table
    .withColumn("amount", F.col("amount").cast(DoubleType()))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("sale_date", F.col("sale_date").cast(DateType()))
    .dropna(subset=["customer_id", "store_id"])   # Drop rows where customer_id or store_id is NULL
)

sales_br_table.show(truncate=False)
sales_br_table.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_br_table.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
