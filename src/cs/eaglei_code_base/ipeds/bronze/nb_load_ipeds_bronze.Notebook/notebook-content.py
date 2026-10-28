# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0277ffad-3784-4551-8d7d-66d1b8ff633a",
# META       "default_lakehouse_name": "lh_cs_bronze",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         },
# META         {
# META           "id": "0277ffad-3784-4551-8d7d-66d1b8ff633a"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "f45c2c8e-a6e5-4098-aaf6-567ab6b6ab98",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### The package should be installed at cluster level and mention version

# CELL ********************

%run nb_pipeline_monitoring_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from decimal import Decimal
from datetime import date, datetime
from pyspark.sql.functions import explode_outer, col, input_file_name, split, element_at
from pyspark.sql.types import *
import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# SCHEMA='ipeds'
# FILE_NAME='ipeds.drvcyear_2023.parquet'
# DESTINATION_TABLE='drvcyear'
# YEAR='2023'
# CREATED_BY='6d0d330a5d644a25bf4f84ec83525b1a'
# MASTER_RUN_ID = "test123"
# RUN_ID = "test123"
# PIPELINE_ID = "test123"
# PIPELINE_NAME = "test123"
# DOMAIN = "cs"
# SOURCE_NAME = "ipeds"
# ACTIVITY_NAME = "notebook activity"
# STATUS = "in-progress"
# LEVEL="notebook"
# LAYER="bronze"
# TRIGGERED_BY = "manual"
# ENVIRONMENT = "dev"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_bronze"
job_name = "nb_load_ipeds_bronze"
source_name = "ipeds"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_metadata_ipeds"

config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
config_df = spark.read.format("delta").load(config_abfss_path)

#display(config_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name, "lh_cs_bronze")
bronze_root_path = get_lh_root_path(
    WORKSPACE_ID,
    bronze_lakehouse_id
)
display(bronze_root_path)
#'abfss://088bba09-dd86-49a0-87f3-ad9331892945@onelake.dfs.fabric.microsoft.com/0277ffad-3784-4551-8d7d-66d1b8ff633a'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = f"{SCHEMA}.{DESTINATION_TABLE}"
display(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SOURCE_TABLE = {FILE_NAME}
TARGET_TABLE = f"{SCHEMA}.{DESTINATION_TABLE}"
# ==========================================
# LAKEHOUSE CONFIGURATION
 
# ==========================================
 
var_cs_lib_name = "var_cs_process"
landing_workspace_id = get_workspace_id(
var_cs_lib_name,
DOMAIN.lower(),
"store"
)
landing_lakehouse_id = get_lakehouse_id(
var_cs_lib_name,
"lh_cs_landing"
)
landing_root_path = get_lh_root_path(
landing_workspace_id,
landing_lakehouse_id
)
file_path = (
f"{landing_root_path}/Files/"
f"{SCHEMA}/"
f"{FILE_NAME}"

)
 
print(f"Source File Path : {file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import lit, current_timestamp

def ingest_bronze_layer():
    """
    Ingest the IPEDS parquet file from landing into the bronze Delta table.

    Uses global notebook parameters:
      - YEAR
      - RUN_ID
      - CREATED_BY
      - SCHEMA
      - DESTINATION_TABLE
      - file_path
    """
    year = YEAR
    run_id = RUN_ID
    created_by = CREATED_BY
    schema = SCHEMA
    destination_table = DESTINATION_TABLE

    # READ SOURCE FILE
    df = spark.read.parquet(file_path)
    source_count = df.count()
    print(f"Source Count : {source_count}")

    # STANDARDIZE COLUMN NAMES
    clean_columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df = df.toDF(*clean_columns)

    # REMOVE AUDIT COLUMNS IF PRESENT
    audit_columns = ["year", "run_id", "created_by", "created_datetime"]
    source_columns = [c for c in df.columns if c not in audit_columns]
    df = df.select(*source_columns)

    # ADD AUDIT COLUMNS
    df = (
        df.withColumn("year", lit(int(year)))
          .withColumn("run_id", lit(str(run_id)))
          .withColumn("created_by", lit(str(created_by)))
          .withColumn("created_datetime", current_timestamp())
    )

    # TARGET TABLE
    target_table = f"{schema}.{destination_table}"
    print(f"Target Table : {target_table}")

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # HANDLE SCHEMA DRIFT
    if spark.catalog.tableExists(target_table):
        print(f"Table Exists : {target_table}")
        target_df = spark.table(target_table)

        # Add any missing columns from target into df
        for field in target_df.schema:
            if field.name not in df.columns:
                print(f"Adding Missing Column : {field.name}")
                df = df.withColumn(field.name, lit(None).cast(field.dataType))

        # Reorder columns so existing target columns come first
        ordered_cols = target_df.columns + [c for c in df.columns if c not in target_df.columns]
        df = df.select(*ordered_cols)

        # Delete existing records for this year before writing
        print(f"Deleting Existing Records For Year : {year}")
        spark.sql(
            f"""
            DELETE FROM {target_table}
            WHERE year = {int(year)}
            """
        )
    else:
        print(f"Creating New Table : {target_table}")

    # WRITE TO DELTA
    (
        df.write
        .format("delta")
        .option("mergeSchema", "true")
        .mode("append")
        .saveAsTable(target_table)
    )

    # VALIDATION
    loaded_count = spark.sql(
        f"""
        SELECT COUNT(*)
        FROM {target_table}
        WHERE year = {int(year)}
        """
    ).collect()[0][0]

    print(f"Rows Loaded : {loaded_count}")

    return {
        "source_count": int(source_count),
        "target_count": int(loaded_count),
        "status": "success"
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# Define source/target for monitoring based on IPEDS bronze load
SOURCE_TABLE = FILE_NAME
TARGET_TABLE = f"{SCHEMA}.{DESTINATION_TABLE}"

start_time = datetime.now()
end_time = datetime.now()
rows_read = 0
rows_written = 0
rows_rejected = 0

try:
    # ==========================================
    # BUSINESS LOGIC - Bronze Ingestion
    # ==========================================
    print("Executing bronze ingestion logic...")

    result = ingest_bronze_layer()

    print("Bronze ingestion completed...")
    print(result)

    rows_read = int(result.get("source_count", 0))
    rows_written = int(result.get("target_count", 0))

    # update audit log to SUCCESS
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())

    # Safely call monitoring function only if it exists in scope
    if 'insert_pipeline_monitoring_record' in globals():
        insert_pipeline_monitoring_record(
            _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
            _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
            "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE),
            rows_read, rows_written, rows_rejected,
            start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
            _safe_str(WORKSPACE_ID), _safe_str(TRIGGERED_BY),
            rerun_count=0
        )
        print("SUCCESS logged")
    else:
        print("WARNING: insert_pipeline_monitoring_record is not defined; skipping audit logging for SUCCESS.")

except Exception as e:
    # update audit log to FAILURE
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())

    if 'insert_pipeline_monitoring_record' in globals():
        insert_pipeline_monitoring_record(
            _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
            _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
            "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE),
            rows_read, rows_written, rows_rejected,
            start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
            _safe_str(WORKSPACE_ID), _safe_str(TRIGGERED_BY),
            rerun_count=0
        )
        print("FAILED logged")
    
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
