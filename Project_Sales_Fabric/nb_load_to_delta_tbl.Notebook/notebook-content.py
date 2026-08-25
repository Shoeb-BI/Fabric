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

# Load the raw data from files folder to delta table in lh_sales

df= spark.read.format('csv')\
         .option('header','true')\
         .option('inferschema','true')\
         .load('Files/raw/sales_data')

df.write.format('delta')\
        .mode('overwrite')\
        .saveAsTable('bronze_sales ')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
