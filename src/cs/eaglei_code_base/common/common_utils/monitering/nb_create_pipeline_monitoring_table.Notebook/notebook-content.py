# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run dq_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run get_variable_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType,
    IntegerType, TimestampType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib_name = get_variable_name("var_cs_name")
audit_lakehouse_name = get_variable_name("audit_lakehouse_name")


workspace_id = get_variable(var_lib_name, "platform_monitoring_workspace_id")
lakehouse_id = get_lakehouse_id(var_lib_name,audit_lakehouse_name)

ABFSS_BASE = (
    f"abfss://{workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com"
    f"/{lakehouse_id}"
)
MONITORING_TABLE_PATH =(f"{ABFSS_BASE}/Tables/audit/pipeline_monitoring")
MASTER_MONITORING_TABLE_PATH = (f"{ABFSS_BASE}/Tables/audit/master_pipeline_monitoring")

#abfss_path_builder(workspace_id: str, lakehouse_id: str, schema_name: str, table_name: str)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Monitering Workspace ID:",workspace_id)
print("Monitering Lakehouse ID:",lakehouse_id)
print("Monitering Table path:",MONITORING_TABLE_PATH)
print("MASTER_MONITORING_TABLE_PATH:",MASTER_MONITORING_TABLE_PATH)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

master_schema = StructType([
  StructField("id", StringType(), False),
  StructField("pipeline_id",  StringType(),  False), # Fabric pipeline run ID
  StructField("pipeline_name", StringType(),  False), # Master orchestrator name
  StructField("master_run_id", StringType(), False),
  StructField("source_name",     StringType(),  True),  # e.g. "workday", "ps", "canvas"
  StructField("domain",     StringType(),  True),  # e.g. "cs", "fn", "hr"
  StructField("environment",  StringType(),  True),  # dev / test / prod
  StructField("status",     StringType(),  False), # INPROGRESS / SUCCESS / FAILED
  StructField("rerun_count",  IntegerType(),  False),
  StructField("start_time",   TimestampType(), False),
  StructField("end_time",    TimestampType(), True),
  StructField("duration_seconds", IntegerType(), True),
  StructField("error_message", StringType(),  True),
  StructField("triggered_by",  StringType(),  True),
  StructField("workspace_id",  StringType(),  True),
])

audit_schema = StructType([
    StructField("id", StringType(), False),
    StructField("master_run_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("domain",     StringType(),  True),  # e.g. "cs", "fn", "hr"
    StructField("source_name", StringType(), False),  # e.g. "cs", "fn", "hr"
    StructField("layer", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("level", StringType(), False), # notebook/pipeline
    StructField("activity_name", StringType(), False),
    StructField("status", StringType(), False),
    StructField("rerun_count", IntegerType(), False),
    StructField("rows_read", LongType(), True),
    StructField("rows_written", LongType(), True),
    StructField("rows_rejected", LongType(), True),
    StructField("start_time", TimestampType(), False),
    StructField("end_time", TimestampType(), True),
    StructField("duration_seconds", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("error_type", StringType(), True),
    StructField("environment", StringType(), True),
    StructField("workspace_name", StringType(), True),
    StructField("workspace_id", StringType(), True),
    StructField("triggered_by", StringType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# CREATE TABLE IF NOT EXISTS
# ============================================================

try:
    df_master = spark.createDataFrame([], master_schema)
    df_master.write.format("delta").mode("ignore").save(MASTER_MONITORING_TABLE_PATH)
    print("SUCCESS: Master monitoring table created")

    df = spark.createDataFrame([], audit_schema)
    df.write.format("delta").mode("ignore").save(MONITORING_TABLE_PATH)
    print("SUCCESS: pipeline Monitoring table created")
except Exception as e:
    print(f"ERROR: {str(e)}")
    raise

# ============================================================
# VALIDATE
# ============================================================

display(spark.read.format("delta").load(MASTER_MONITORING_TABLE_PATH))
display(spark.read.format("delta").load(MONITORING_TABLE_PATH))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
