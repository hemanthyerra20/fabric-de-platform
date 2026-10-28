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

# PARAMETERS CELL ********************

# MASTER_RUN_ID = "M-123"
# RUN_ID = "123"
# PIPELINE_ID = "p-123"
# PIPELINE_NAME = "test"
# NOTEBOOK_NAME = "test_monitering"
# DOMAIN = "CS"
# SOURCE_NAME = "workday"
# ACTIVITY_NAME = "notebook activity"
# STATUS = "in-progress"
# LEVEL="notebook"
# LAYER="landing"
# SOURCE_TABLE="api"
# TARGET_TABLE="landing"
# ENVIRONMENT = "dev"
# WORKSPACE_NAME="test_process_workspace"
# WORKSPACE_ID = "12fwerwe"
# TRIGGERED_BY = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import time
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
MONITORING_TABLE_PATH =(
    f"{ABFSS_BASE}/Tables/audit/pipeline_monitoring"
)
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Helper to safely stringify complex values for Spark DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType


def _safe_str(value):
    """Convert any value (including exceptions) to a plain string for logging."""
    try:
        if value is None:
            return None
        return str(value)
    except Exception:
        # Fallback to repr if str() fails for any reason
        return repr(value)
        
master_schema = StructType([
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
    StructField("master_run_id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("notebook_name", StringType(), False),
    StructField("domain",     StringType(),  True),  # e.g. "cs", "fn", "hr"
    StructField("source_name", StringType(), False),      # e.g. "workday", "ps", "canvas"
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

MASTER_MONITORING_TABLE_PATH = (
  f"{ABFSS_BASE}/Tables/audit/master_pipeline_monitoring"
)

def upsert_master_record(record_df):
  delta_table = DeltaTable.forPath(spark, MASTER_MONITORING_TABLE_PATH)

  merge_condition = """
    target.id  = source.id
  """

  (
    delta_table.alias("target")
    .merge(record_df.alias("source"), merge_condition)
    .whenMatchedUpdate(set={
      "status":           "source.status",
      "end_time":         "source.end_time",
      "duration_seconds": "source.duration_seconds",
      "error_message":    "source.error_message",
      "rerun_count":      "target.rerun_count + 1",
    })
    .whenNotMatchedInsert(values={
      "pipeline_id":   "source.pipeline_id",
      "pipeline_name": "source.pipeline_name",
      "master_run_id": "source.master_run_id",
      "source_name":   "source.source_name",
      "domain":        "source.domain",
      "environment":   "source.environment",
      "status":        "source.status",
      "rerun_count":   "0",
      "start_time":     "source.start_time",
      "end_time":       "source.end_time",
      "duration_seconds":"source.duration_seconds",
      "error_message":   "source.error_message",
      "triggered_by":    "source.triggered_by",
      "workspace_id":    "source.workspace_id",
     })
     .execute()
   )

def log_master_status(
    pipeline_id, pipeline_name, master_run_id, source_name, domain, status,
    environment=None, start_time=None, end_time=None,
    error_message=None, triggered_by=None,
    workspace_id=None, rerun_count=0
):
    duration_seconds = None
    if start_time and end_time:
        duration_seconds = int((end_time - start_time).total_seconds())

    row = [(
        _safe_str(pipeline_id),
        _safe_str(pipeline_name),
        _safe_str(master_run_id),
        _safe_str(source_name),
        _safe_str(domain),
        _safe_str(environment),
        _safe_str(status),
        rerun_count,
        start_time,
        end_time,
        duration_seconds,
        _safe_str(error_message),
        _safe_str(triggered_by),
        _safe_str(workspace_id),
    )]
    df = spark.createDataFrame(row, schema=master_schema)

    id_added_df = df.select(
        "*",
        F.sha2(
                F.concat_ws(
                    "||",
                    coalesce(col("pipeline_id").cast("string"), lit("")),
                    coalesce(col("workspace_id").cast("string"), lit("")),
                    coalesce(col("master_run_id").cast("string"), lit(""))
                ),
                256
            ).alias("id"),
    )
    upsert_master_record(id_added_df)
    print(f"Master monitoring updated: {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_monitoring_record(record_df):
    delta_table = DeltaTable.forPath(spark, MONITORING_TABLE_PATH)

    merge_condition = """
        target.id = source.id
    """
    (
        delta_table.alias("target")
        .merge(
            record_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdate(set={
            "pipeline_name": "source.pipeline_name",
            "layer": "source.layer",
            "level": "source.level",
            "status": "source.status",
            "rows_read": "source.rows_read",
            "rows_written": "source.rows_written",
            "rows_rejected": "source.rows_rejected",
            "end_time": "source.end_time",
            "duration_seconds": "source.duration_seconds",
            "error_message": "source.error_message",
            "error_type": "source.error_type",
            "triggered_by": "source.triggered_by",
            # increment retry count
            "rerun_count": "target.rerun_count + 1"
        })
        .whenNotMatchedInsert(values={
            "id": "source.id",
            "master_run_id": "source.master_run_id",
            "run_id": "source.run_id",
            "pipeline_name": "source.pipeline_name",
            "notebook_name": "source.notebook_name",
            "domain": "source.domain",
            "source_name": "source.source_name",  
            "layer": "source.layer",
            "source_table": "source.source_table",
            "target_table": "source.target_table",
            "level": "source.level",
            "activity_name": "source.activity_name",
            "status": "source.status",
            "rerun_count": "0",
            "rows_read": "source.rows_read",
            "rows_written": "source.rows_written",
            "rows_rejected": "source.rows_rejected",
            "start_time": "source.start_time",
            "end_time": "source.end_time",
            "duration_seconds": "source.duration_seconds",
            "error_message": "source.error_message",
            "error_type": "source.error_type",
            "environment": "source.environment",
            "workspace_name": "source.workspace_name",
            "workspace_id": "source.workspace_id",
            "triggered_by": "source.triggered_by"
        })
        .execute()
    )


def log_pipeline_status(
    master_run_id,
    run_id,
    pipeline_name,
    notebook_name,
    domain,
    source_name,
    activity_name,
    status,
    level="NOTEBOOK",
    layer=None,
    source_table=None,
    target_table=None,
    rows_read=None,
    rows_written=None,
    rows_rejected=None,
    start_time=None,
    end_time=None,
    error_message=None,
    error_type=None,
    environment=None,
    workspace_name=None,
    workspace_id=None,
    triggered_by=None,
    rerun_count=0,
):

    duration_seconds = None

    if start_time and end_time:
        duration_seconds = int(
            (end_time - start_time).total_seconds()
        )

    data = [(
        _safe_str(master_run_id),
        _safe_str(run_id),
        _safe_str(pipeline_name),
        _safe_str(notebook_name),
        _safe_str(domain),
        _safe_str(source_name),   # function parameter `Source`, mapped to column `source`
        _safe_str(layer),
        _safe_str(source_table),
        _safe_str(target_table),
        _safe_str(level),
        _safe_str(activity_name),
        _safe_str(status),
        rerun_count,
        rows_read,
        rows_written,
        rows_rejected,
        start_time,
        end_time,
        duration_seconds,
        _safe_str(error_message),
        _safe_str(error_type),
        _safe_str(environment),
        _safe_str(workspace_name),
        _safe_str(workspace_id),
        _safe_str(triggered_by)
    )]

    df = spark.createDataFrame(data, schema=audit_schema)

    id_added_df = df.select(
        "*",
        F.sha2(
                F.concat_ws(
                    "||",
                    coalesce(col("master_run_id").cast("string"), lit("")),
                    coalesce(col("run_id").cast("string"), lit("")),
                    coalesce(col("activity_name").cast("string"), lit("")),
                    coalesce(col("notebook_name").cast("string"), lit("")),
                    coalesce(col("workspace_id").cast("string"), lit("")),
                    coalesce(col("target_table").cast("string"), lit(""))
                ),
                256
            ).alias("id"),
    )

    upsert_monitoring_record(id_added_df)

    print(f"pipeline Monitoring updated: {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def insert_master_status(
    pipeline_id, pipeline_name, master_run_id, source_name, domain, status,
    environment=None, start_time=None, end_time=None,
    error_message=None, triggered_by=None,
    workspace_id=None, rerun_count=0
):
    duration_seconds = None
    if start_time and end_time:
        duration_seconds = int((end_time - start_time).total_seconds())

    row = [(
        _safe_str(pipeline_id),
        _safe_str(pipeline_name),
        _safe_str(master_run_id),
        _safe_str(source_name),
        _safe_str(domain),
        _safe_str(environment),
        _safe_str(status),
        rerun_count,
        start_time,
        end_time,
        duration_seconds,
        _safe_str(error_message),
        _safe_str(triggered_by),
        _safe_str(workspace_id),
    )]
    df = spark.createDataFrame(row, schema=master_schema)

    id_added_df = df.select(
        "*",
        F.sha2(
                F.concat_ws(
                    "||",
                    coalesce(col("pipeline_id").cast("string"), lit("")),
                    coalesce(col("workspace_id").cast("string"), lit("")),
                    coalesce(col("master_run_id").cast("string"), lit(""))
                ),
                256
            ).alias("id"),
    )

    id_added_df.write.format("delta").mode("append").save(MASTER_MONITORING_TABLE_PATH)

    print(f"Master monitoring updated: {status}")


def insert_pipeline_monitoring_record(
    master_run_id,
    run_id,
    pipeline_name,
    notebook_name,
    domain,
    source_name,
    activity_name,
    status,
    level="NOTEBOOK",
    layer=None,
    source_table=None,
    target_table=None,
    rows_read=None,
    rows_written=None,
    rows_rejected=None,
    start_time=None,
    end_time=None,
    error_message=None,
    error_type=None,
    environment=None,
    workspace_name=None,
    workspace_id=None,
    triggered_by=None,
    rerun_count=0,
):

    duration_seconds = None

    if start_time and end_time:
        duration_seconds = int(
            (end_time - start_time).total_seconds()
        )

    data = [(
        _safe_str(master_run_id),
        _safe_str(run_id),
        _safe_str(pipeline_name),
        _safe_str(notebook_name),
        _safe_str(domain),
        _safe_str(source_name),   # function parameter `Source`, mapped to column `source`
        _safe_str(layer),
        _safe_str(source_table),
        _safe_str(target_table),
        _safe_str(level),
        _safe_str(activity_name),
        _safe_str(status),
        rerun_count,
        rows_read,
        rows_written,
        rows_rejected,
        start_time,
        end_time,
        duration_seconds,
        _safe_str(error_message),
        _safe_str(error_type),
        _safe_str(environment),
        _safe_str(workspace_name),
        _safe_str(workspace_id),
        _safe_str(triggered_by)
    )]

    df = spark.createDataFrame(data, schema=audit_schema)

    id_added_df = df.select(
        "*",
        F.sha2(
                F.concat_ws(
                    "||",
                    coalesce(col("master_run_id").cast("string"), lit("")),
                    coalesce(col("run_id").cast("string"), lit("")),
                    coalesce(col("activity_name").cast("string"), lit("")),
                    coalesce(col("notebook_name").cast("string"), lit("")),
                    coalesce(col("workspace_id").cast("string"), lit("")),
                    coalesce(col("target_table").cast("string"), lit(""))
                ),
                256
            ).alias("id"),
    )

    id_added_df.write.format("delta").mode("append").save(MONITORING_TABLE_PATH)

    print(f"pipeline Monitoring updated: {status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# start_time = datetime.now()
# end_time = datetime.now()
# rows_read = 0
# rows_written = 0
# rows_rejected = 0

# log_master_status(
#         pipeline_id=PIPELINE_ID,
#         pipeline_name=PIPELINE_NAME,
#         master_run_id=MASTER_RUN_ID,
#         source_name=SOURCE_NAME,
#         domain=DOMAIN,
#         status="failed",           # override to FAILED when logging throws 
#         environment=ENVIRONMENT,
#         start_time=start_time,
#         end_time=end_time,
#         error_message=str(e),       # store the logging error
#         triggered_by=TRIGGERED_BY,
#         workspace_id=WORKSPACE_ID,
#     )

# log_pipeline_status( 
#         _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
#         "inprogress", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
#         start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(TRIGGERED_BY),
#         rerun_count=0)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from pyspark.sql.types import (
#     StructType, StructField,
#     StringType, LongType,
#     IntegerType, TimestampType
# )

# audit_schema = StructType([
#     StructField("run_id", StringType(), False),
#     StructField("pipeline_name", StringType(), False),
#     StructField("notebook_name", StringType(), False),
#     StructField("layer", StringType(), True),
#     StructField("source_name", StringType(), True),
#     StructField("target_table", StringType(), True),
#     StructField("level", StringType(), False),
#     StructField("activity_name", StringType(), False),
#     StructField("status", StringType(), False),
#     StructField("rerun_count", IntegerType(), False),
#     StructField("rows_read", LongType(), True),
#     StructField("rows_written", LongType(), True),
#     StructField("rows_rejected", LongType(), True),
#     StructField("start_time", TimestampType(), False),
#     StructField("end_time", TimestampType(), True),
#     StructField("duration_seconds", IntegerType(), True),
#     StructField("error_message", StringType(), True),
#     StructField("error_type", StringType(), True),
#     StructField("environment", StringType(), True),
#     StructField("workspace_name", StringType(), True),
#     StructField("workspace_id", StringType(), True),
#     StructField("triggered_by", StringType(), True)
# ])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

