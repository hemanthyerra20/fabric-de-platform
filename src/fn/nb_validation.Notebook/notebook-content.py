# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5cb1dc45-6dbe-470f-8ef9-0291324328c2",
# META       "default_lakehouse_name": "lh_fn_landing",
# META       "default_lakehouse_workspace_id": "bedb4898-64aa-4f9d-b9de-e0c4b634ee27",
# META       "known_lakehouses": [
# META         {
# META           "id": "5cb1dc45-6dbe-470f-8ef9-0291324328c2"
# META         },
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": [
# META         {
# META           "id": "da7c9db2-6639-4346-b4e5-bd3256de4395",
# META           "type": "Lakewarehouse"
# META         },
# META         {
# META           "id": "59443c6e-4824-4a85-8b00-1b5f51c458bf",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

df = spark.sql("SELECT * FROM lh_fn_landing.workday_fn.cost_center LIMIT 10")
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
