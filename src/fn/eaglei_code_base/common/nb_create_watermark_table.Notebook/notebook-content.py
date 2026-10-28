# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d35b1ce6-d133-4623-9d58-8c88aba0654b",
# META       "default_lakehouse_name": "lh_platform_monitoring",
# META       "default_lakehouse_workspace_id": "b0bd8a9b-9bdf-4a90-99cf-fed9e6800f1a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         },
# META         {
# META           "id": "5bd544a5-340c-493c-aa88-61dc1992c614"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d6e9c4b7-fdc8-4966-afb3-443827650df9",
# META       "workspaceId": "088bba09-dd86-49a0-87f3-ad9331892945"
# META     }
# META   }
# META }

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
import json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DateType
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_cs_lib_name = "var_fn_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "watermark"
notebook_name = notebookutils.runtime.context["currentNotebookName"]


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# 2. Define schema
# ------------------------------------------------------------------
schema = StructType([
    StructField("id", StringType(), True),
    StructField("process_name", StringType(), True),
    StructField("source", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("lakehouse_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("watermark_date", DateType(), True),
    StructField("watermark_ts", TimestampType(), True),
    StructField("created_on", TimestampType(), True),
    StructField("created_by", StringType(), True),
    StructField("modified_on", TimestampType(), True),
    StructField("modified_by", StringType(), True),

])

# ------------------------------------------------------------------
# 3. Create a single-row DataFrame
# ------------------------------------------------------------------

df_config = spark.createDataFrame([], schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_config.write.partitionBy("process_name").format("delta")\
        .mode("append")\
        .option("mergeSchema", True)\
        .save(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def append_to_delta_partitioned(df, path: str):
    """Append DataFrame to a Delta table at `path`, partitioned by process_name.
    
    Parameters
    ----------
    df : DataFrame
        Spark DataFrame with a column `process_name`.
    path : str
        ABFSS or local path to the Delta table location.
    """
    (
        df.write
          .partitionBy("process_name")
          .format("delta")
          .mode("append")
          .option("mergeSchema", True)
          .save(path)
    )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
