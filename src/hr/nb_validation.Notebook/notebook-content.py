# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b229f4c0-e27f-4fb4-92f4-5d4edc0a773c",
# META       "default_lakehouse_name": "lh_hr_landing",
# META       "default_lakehouse_workspace_id": "fc434332-df9b-42bf-9f6d-2e6f2ab3fca9",
# META       "known_lakehouses": [
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         },
# META         {
# META           "id": "b229f4c0-e27f-4fb4-92f4-5d4edc0a773c"
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

df = spark.sql("SELECT * FROM lh_hr_landing.workday_hr.academic_unit LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM `EAGLEI-PlatformMonitoring-DEV`.lh_platform_monitoring.audit.pipeline_monitoring order by end_time desc LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM `EAGLEI-PlatformMonitoring-DEV`.lh_platform_monitoring.audit.master_pipeline_monitoring order by end_time desc LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
