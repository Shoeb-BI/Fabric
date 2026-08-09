# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cbc7d6ad-919b-42fd-851c-ccd82426883b",
# META       "default_lakehouse_name": "LH_Bootcamp_Bronze",
# META       "default_lakehouse_workspace_id": "d1847af3-63f2-4cdb-ac61-879503d263b2",
# META       "known_lakehouses": [
# META         {
# META           "id": "cbc7d6ad-919b-42fd-851c-ccd82426883b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Reading all files at once from File and write it to Table


for t in ["fact_sales","dim_customers","dim_products","dim_stores","dim_targets"]:
    df = (spark.read.option("header","true").option("inferSchema","true")
               .csv(f"Files/RawData/{t}.csv"))
    df.write.mode("overwrite").format("delta").saveAsTable(t)
         

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
