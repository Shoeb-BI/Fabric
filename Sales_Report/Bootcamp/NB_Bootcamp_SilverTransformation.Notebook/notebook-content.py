# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "dac96ccc-e112-4a3e-b4c3-fdde07f11ab5",
# META       "default_lakehouse_name": "LH_Bootcamp_Silver",
# META       "default_lakehouse_workspace_id": "d1847af3-63f2-4cdb-ac61-879503d263b2",
# META       "known_lakehouses": [
# META         {
# META           "id": "dac96ccc-e112-4a3e-b4c3-fdde07f11ab5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


# Read Bronze schema table
Targets_br_tbl = spark.read.table("LH_Bootcamp_Silver.bronze.dim_targets")

# To transform the read table of bronze

# Transformation to be applied dim target table dim_targets >>> cast: target_amount = double; validate month: 1-12

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType,DoubleType

Targets_Silver_tbl = (

    target_br_tbl
    .withColumn("month",F.col("month").cast(IntegerType())) #month as int
    .withColumn("target_amount",F.col("target_amount").cast(DoubleType())) #amount as double
                    )
# ---- Validation: never silently drop.
# First SEE the bad rows, then decide ----

bad_month = Targets_Silver_tbl.filter(~F.col("month").between(1, 12))

print("rows with invalid month:", bad_month.count())
bad_month.show(truncate=False)

# Keep only valid months (1..12)
Targets_Silver_tbl = Targets_Silver_tbl.filter(
    F.col("month").between(1, 12)
)

#organize the data in sorted order
Targets_Silver_tbl.orderBy("target_id").show(truncate=False)

# to view the schema for data types of the table
Targets_Silver_tbl.printSchema()



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Store table from bronze schema

Stores_br_tbl = spark.read.table("LH_Bootcamp_Silver.bronze.dim_stores")

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType

Stores_Silver_tbl = (
    Stores_br_tbl

    # 1) Normalize first: trim + uppercase so "north", " North " and "N" all match
    .withColumn("zone", F.upper(F.trim(F.col("zone"))))

    # 2) Map every variant to a single standard code
    .withColumn(
        "zone",
        F.when(F.col("zone").isin("N", "NORTH"), "N")
         .when(F.col("zone").isin("S", "SOUTH"), "S")
         .when(F.col("zone").isin("E", "EAST"), "E")
         .when(F.col("zone").isin("W", "WEST"), "W")
         .otherwise("Unknown")        # Anything unexpected is flagged
    )

    # 3) Cast open_date -> DateType (bad -> null)
    .withColumn(
        "open_date",
        F.to_date(F.col("open_date"), "dd-MM-yyyy").cast(DateType())
    )

    # Change data type to IntegerType
    .withColumn("store_id", F.col("store_id").cast(IntegerType()))
)

Stores_Silver_tbl.orderBy("store_id").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read Customer table from bronze schema

Customers_br_tbl = spark.read.table("LH_Bootcamp_Silver.bronze.dim_customers")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType

# Rank each customer's rows newest-first using the real recency column: updated_at
w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc_nulls_last())

Customers_Silver_tbl = (
    Customers_br_tbl
    .fillna({"email": "unknown@bharatmart.com"})                # 1) Fill null emails
    .withColumn("customer_id", F.trim(F.col("customer_id")))    # 2) Trim key
    .withColumn("_rn", F.row_number().over(w))                  # 3) Dedupe: keep latest
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))   # 4) Cast
    .withColumn(
        "birth_date",
        F.to_date(F.col("birth_date"), "dd-MM-yyyy").cast(DateType())
    )
)

Customers_Silver_tbl.show(Customers_Silver_tbl.count(), truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read product table from bronze schema
Products_br_tbl = spark.read.table("LH_Bootcamp_Silver.bronze.dim_products")

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType

Products_Silver_tbl = (
    Products_br_tbl
    .withColumn("product_name", F.trim(F.col("product_name")))      # Trim product_name
    .fillna({"category": "Unknown"})                                # Replace null with "Unknown"
    .withColumn("price", F.col("price").cast(DoubleType()))         # Change data type to DoubleType
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))  # Change data type to IntegerType
)

Products_Silver_tbl.orderBy("product_id").show(
    Products_Silver_tbl.count(),
    truncate=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_sales >>> cast: amount/qty/dates; drop null: customer_id or store_id

Sales_br_tbl = spark.read.table("LH_Bootcamp_Silver.bronze.fact_sales")

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, DateType

before = Sales_br_tbl.count()

Sales_Silver_tbl = (
    Sales_br_tbl
    .withColumn("amount", F.col("amount").cast(DoubleType()))                # money -> double
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))           # quantity -> int
    .withColumn(
        "sale_date",
        F.to_date(F.col("sale_date"), "dd-MM-yyyy").cast(DateType())
    )
    .withColumn("sale_id", F.col("sale_id").cast(IntegerType()))
    .na.drop(subset=["customer_id", "store_id"])                             # drop rows missing either key
)

print(
    f"rows [before] -> {Sales_Silver_tbl.count()} "
    f"(dropped {before - Sales_Silver_tbl.count()} null-key rows)"
)

Sales_Silver_tbl.orderBy("sale_id").show(truncate=False)

print("Schema before transformation")
Sales_br_tbl.printSchema()

print("Schema after transformation")
Sales_Silver_tbl.printSchema()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Drop the shortcut or table created using write (this only removes the pointer, NOT the underlying Bronze data)..one by one
spark.sql("DROP TABLE IF EXISTS fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Now write the transformed Silver version into table one by one

Sales_Silver_tbl.write.mode("overwrite").format("delta").saveAsTable("fact_sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Repeat this drop-then-write pattern for each of your 5 transformed tables. Loop version:
 
tables = {
    "dim_customers": Customers_Silver_tbl,
    "dim_products": Products_Silver_tbl,
    "dim_stores": Stores_Silver_tbl,
    "dim_targets": Targets_Silver_tbl,
    "fact_sales": Sales_Silver_tbl
}
 
for name, df in tables.items():
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    df.write.mode("overwrite").format("delta").saveAsTable(name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
