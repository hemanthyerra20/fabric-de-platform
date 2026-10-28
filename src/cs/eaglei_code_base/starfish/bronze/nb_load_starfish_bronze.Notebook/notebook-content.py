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

# SCHEMA='starfish'
# LOAD_DATE=""
# FILE_NAME='ipeds.drvcyear_2023.parquet'
# DESTINATION_TABLE='drvcyear'
# CREATED_BY='6d0d330a5d644a25bf4f84ec83525b1a'
# MASTER_RUN_ID = "test123"
# RUN_ID = "test123"
# PIPELINE_ID = "test123"
# PIPELINE_NAME = "test123"
# DOMAIN = "cs"
# SOURCE_NAME = "starfish"
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

# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
# SCHEMA = "starfish"
# LOAD_DATE = "2026-09-11"
# FILE_NAME = "meeting-fulldetail-nwtc.csv"
# DESTINATION_TABLE = "meeting_fulldetail"
# CREATED_BY = "014b22ad158549aaa3592692584bc7c0"
# MASTER_RUN_ID = "1e4e155e-8542-4c43-91ef-3ef11c70ef32"
# RUN_ID = "1e4e155e-8542-4c43-91ef-3ef11c70ef32"
# PIPELINE_ID = "8d0285ad-d0a2-4a3b-a18d-b1923070d4c5"
# PIPELINE_NAME = "pl_starfish_landing"
# DOMAIN = "cs"
# SOURCE_NAME = "starfish"
# ACTIVITY_NAME = "notebookactivity"
# STATUS = "inprogress"
# LEVEL = "notebook"
# LAYER = "bronze"
# TRIGGERED_BY = "014b22ad158549aaa3592692584bc7c0"
# ENVIRONMENT = "dev"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_bronze"
job_name = "nb_load_starfish_bronze"
source_name = "starfish"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_metadata_starfish"

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
f"{LOAD_DATE}/"
f"{FILE_NAME}"
)
print(f"Source File Path : {file_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
def def_bronze_load():
    rows_read = 0
    rows_written = 0
    rows_rejected = 0
    try:
        # df = (
        #     spark.read
        #     .option("header", True)
        #     .option("inferSchema", True)
        #     .csv(file_path)
        # )
        # rows_read = df.count()

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("multiLine", "true")
            .csv(file_path)
            )
        rows_read = df.count()

 
 
        ###############################################################
        # Create Schema
        ###############################################################
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA}
        """)
        ###############################################################
        # Generate Hash Key
        ###############################################################
        source_cols = df.columns
        df = df.withColumn(
            "hash_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(
                            F.trim(F.col(c).cast("string")),
                            F.lit("")
                        )
                        for c in source_cols
                    ]
                ),
                256
            )

        )
        ###############################################################
        # Remove duplicates within current file only
        ###############################################################
        #df = df.dropDuplicates(["hash_key"])
        ###############################################################
        # Audit Columns
        ###############################################################
        df = (
            df
            .withColumn("run_id", F.lit(RUN_ID))
            .withColumn("created_by", F.lit(CREATED_BY))
            .withColumn("created_datetime", F.current_timestamp())
            .withColumn("load_date", F.lit(LOAD_DATE))
        )
        ###############################################################
        # Reorder Columns
        ###############################################################
        final_cols = (
            source_cols
            + [
                "hash_key",
                "run_id",
                "created_by",
                "created_datetime",
                "load_date"
            ]
        )
        df = df.select(*final_cols)
        ###############################################################
        # Target Table
        ###############################################################
        target_table = f"{SCHEMA}.{DESTINATION_TABLE}"
        ###############################################################
        # Create Table if Not Exists

        ###############################################################
        table_exists = (
        spark.sql(f"SHOW TABLES IN {SCHEMA}")
         .filter(F.col("tableName") == DESTINATION_TABLE)
         .count() > 0
        )
        if not table_exists:
            (
                df.limit(0)
                .write
                .format("delta")
                .partitionBy("load_date")
                .saveAsTable(target_table)
            )
            
        ###############################################################
        # Append Data
        ###############################################################
        (
            df.write
            .mode("overwrite")
            .format("delta")
            .option("replaceWhere",f"load_date='{LOAD_DATE}'")
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )
        rows_written = df.count()
    except Exception as ex:
        rows_rejected = rows_read
        print(f"Error Loading {FILE_NAME} : {str(ex)}")
        print(ex)
        raise
    return {
        "source_count": rows_read,
        "target_count": rows_written,
        "rejected_count": rows_rejected
    }
  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# Define source/target for monitoring based on starfish bronze load
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

    result = def_bronze_load()

    print("Bronze ingestion completed...")
    print(result)

    rows_read = result.get("source_count", 0)
    rows_written = result.get("target_count", 0)
    rows_rejected = result.get("rejected_count", 0)
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
