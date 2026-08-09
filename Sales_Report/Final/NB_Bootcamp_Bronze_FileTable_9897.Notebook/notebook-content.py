# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ae705e2b-b8a2-44f2-936e-0c374fac57b7",
# META       "default_lakehouse_name": "Final_LH_Bronze",
# META       "default_lakehouse_workspace_id": "d1847af3-63f2-4cdb-ac61-879503d263b2",
# META       "known_lakehouses": [
# META         {
# META           "id": "ae705e2b-b8a2-44f2-936e-0c374fac57b7"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

for t in ["fact_sales","dim_customers","dim_products","dim_stores","dim_targets"]: 
    df = (spark.read
    .option("header","true")
    .option("inferSchema","true") 
    .csv(f"Files/{t}.csv")) 
    
    df.write.mode("overwrite").format("delta").saveAsTable(t)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
