# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08846ef1-977d-4698-b06f-84fdbd834bc6",
# META       "default_lakehouse_name": "lh_cs_silver",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "08846ef1-977d-4698-b06f-84fdbd834bc6"
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

# SCHEMA = "starfish"
# LOAD_DATE = "2026-07-02"
# DESTINATION_TABLE = "tracking_fulldetail"
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
# LAYER = "silver"
# TRIGGERED_BY = "014b22ad158549aaa3592692584bc7c0"
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

lh_name = "lh_cs_silver"
job_name = "nb_load_ipeds_silver"
source_name ="starfish"
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

silver_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, 'lh_cs_silver')

silver_root_path = get_lh_root_path(silver_workspace_id, silver_lakehouse_id)

#display(lh_root_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = f"{DESTINATION_TABLE}"
display(target_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SOURCE_TABLE = f"{DESTINATION_TABLE}"
TARGET_TABLE = f"{DESTINATION_TABLE}"
 
# ==========================================
 
# LAKEHOUSE CONFIGURATION
 
# ==========================================
var_cs_lib_name = "var_cs_process"
bronze_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name, 'lh_cs_bronze')

bronze_root_path = get_lh_root_path(bronze_workspace_id, bronze_lakehouse_id)
source_path = (
f"{bronze_root_path}/Tables/"
f"{SCHEMA}/"
f"{SOURCE_TABLE}"
)
print(f"Source Table Path : {source_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    ShortType,
    ByteType,
    DoubleType,
    FloatType,
    DecimalType,
    BooleanType
)
def def_silver_load():
    rows_read = 0
    rows_written = 0
    rows_rejected = 0
    try:
        ###############################################################
        # Create Silver Schema
        ###############################################################
        spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA}

        """)
        ###############################################################
        # Source & Target Tables
        ###############################################################
        #source_table = f"{SCHEMA}.{DESTINATION_TABLE}"
        target_table = f"{SCHEMA}.{DESTINATION_TABLE}"

        df = (
            spark.read
            .format("delta")
            .load(source_path)
        )
 
 
        ###############################################################
        # Read Bronze
        ###############################################################
        #df = spark.table(source_table)
        ###############################################################
        # Load only Current Load Date
        ###############################################################
        df = df.filter(
            F.col("load_date") == LOAD_DATE
        )
        rows_read = df.count()
        ###############################################################
        # Remove Bronze ETL Columns
        ###############################################################
        bronze_etl_cols = [
            "run_id",
            "created_by",
            "created_datetime",
            "load_date"
        ]
        business_cols = [
            c for c in df.columns
            if c not in bronze_etl_cols
        ]
        df = df.select(*business_cols)
        ###############################################################
        # Replace NULL Values
        ###############################################################
        for field in df.schema.fields:
            col_name = field.name
            dtype = field.dataType
            if isinstance(dtype,
                          (IntegerType,
                           LongType,
                           ShortType,
                           ByteType,
                           DoubleType,
                           FloatType,
                           DecimalType)):
                df = df.withColumn(
                    col_name,
                    F.coalesce(
                        F.col(col_name),
                        F.lit(0)
                    )
                )
            elif isinstance(dtype, StringType):
                df = df.withColumn(
                    col_name,
                    F.coalesce(
                        F.trim(F.col(col_name)),
                        F.lit("")
                    )
                )
            elif isinstance(dtype, BooleanType):
                df = df.withColumn(
                    col_name,
                    F.coalesce(
                        F.col(col_name),
                        F.lit(False)
                    )
                )
        ###############################################################
        # Remove Duplicate Records
        ###############################################################
        df = df.distinct()
        ###############################################################
        # Remove Bronze Hash Key
        ###############################################################
        if "hash_key" in df.columns:
            df = df.drop("hash_key")
        ###############################################################
        # Silver Audit Columns
        ###############################################################
        df = (
            df
            .withColumn(
                "created_by",
                F.lit(CREATED_BY)
            )
            .withColumn(
                "created_datetime",
                F.current_timestamp()
            )
            .withColumn(
                "modified_by",
                F.lit(None).cast("string")
            )
            .withColumn(
                "modified_datetime",
                F.lit(None).cast("timestamp")
            )
        )
        ###############################################################
        # Reorder Columns
        ###############################################################
        source_cols = [
            c for c in df.columns
            if c not in [
                "created_by",
                "created_datetime",
                "modified_by",
                "modified_datetime"
            ]
        ]
        final_cols = (
            source_cols +
            [
                "created_by",
                "created_datetime",
                "modified_by",
                "modified_datetime"
            ]
        )
        df = df.select(*final_cols)
        ###############################################################
        # Overwrite Silver Table
        ###############################################################
        (
            df.write
            .mode("overwrite")
            .format("delta")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
        rows_written = df.count()
    except Exception as ex:
        print(f"Error Loading {SOURCE_TABLE}")
        print(ex)
        rows_rejected = rows_read
    ###############################################################
    # Return Counts
    ###############################################################
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

# Define source/target for monitoring based on starfish silver load
SOURCE_TABLE = f"{DESTINATION_TABLE}"
TARGET_TABLE = f"{DESTINATION_TABLE}"

start_time = datetime.now()
end_time = datetime.now()
rows_read = 0
rows_written = 0
rows_rejected = 0

try:
    # ==========================================
    # BUSINESS LOGIC - Silver Ingestion
    # ==========================================
    print("Executing silver ingestion logic...")

    result = def_silver_load()

    print("Silver ingestion completed...")
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
