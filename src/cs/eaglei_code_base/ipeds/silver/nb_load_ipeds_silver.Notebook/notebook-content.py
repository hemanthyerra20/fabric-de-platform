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

#print("bronze:", get_lakehouse_id(var_cs_lib_name, "lh_cs_bronze"))

#print("silver:", get_lakehouse_id(var_cs_lib_name, "lh_cs_silver"))

#print("landing:", get_lakehouse_id(var_cs_lib_name, "lh_cs_landing"))
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

SCHEMA='ipeds'
DESTINATION_TABLE='drvgryear'
YEAR=''
CREATED_BY='6d0d330a5d644a25bf4f84ec83525b1a'
MASTER_RUN_ID = "test123"
RUN_ID = "test123"
PIPELINE_ID = "test123"
PIPELINE_NAME = "test123"
DOMAIN = "cs"
SOURCE_NAME = "ipeds"
ACTIVITY_NAME = "notebook activity"
STATUS = "in-progress"
LEVEL="notebook"
LAYER="silver"
TRIGGERED_BY = "manual"
ENVIRONMENT = "dev"
MODIFIED_BY=""
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
source_name = "ipeds"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_ipeds_institutions"

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

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    sha2,
    concat_ws,
    coalesce,
    broadcast,
    trim
)
from delta.tables import DeltaTable


def ingest_silver_layer():
    """
    Ingest data from IPEDS bronze into IPEDS silver using institutions metadata filter.

    Business logic:
      - Use config_df (loaded from config lakehouse) as IPEDS institutions metadata
      - Read bronze Delta data from 'source_path' (computed from bronze lakehouse root)
      - Optionally filter by YEAR if provided
      - Keep only records matching IPEDS institutions list (join UNITID vs ipeds_number)
      - Compute record_hash over business columns
      - Drop bronze ETL columns and add silver ETL columns
      - Deduplicate by record_hash
      - Create or merge into silver table <SCHEMA>.<DESTINATION_TABLE>

    Required global variables:
      - config_df            : institutions metadata (must contain 'ipeds_number')
      - source_path          : Delta path for bronze table
      - SCHEMA               : target schema name
      - DESTINATION_TABLE    : target table name
      - YEAR (optional)      : filter year
      - CREATED_BY (optional)
      - MODIFIED_BY (optional)
    """
    # -------------------------------------------------------------------------
    # Resolve globals
    # -------------------------------------------------------------------------
    schema = SCHEMA
    destination_table_name = DESTINATION_TABLE

    spark.sql("""
            CREATE SCHEMA IF NOT EXISTS ipeds
    """)


    year_val = None
    if "YEAR" in globals() and YEAR is not None and str(YEAR).strip() != "":
        year_val = str(YEAR).strip()

    created_by_val = (
        CREATED_BY
        if "CREATED_BY" in globals() and CREATED_BY not in [None, ""]
        else "system"
    )
    modified_by_val = (
        MODIFIED_BY
        if "MODIFIED_BY" in globals() and MODIFIED_BY not in [None, ""]
        else created_by_val
    )

    # -------------------------------------------------------------------------
    # Read IPEDS institutions metadata from config_df
    # -------------------------------------------------------------------------
    ipeds_df = (
        config_df
        .select(col("ipeds_number").cast("string").alias("ipeds_number"))
        .distinct()
    )

    # -------------------------------------------------------------------------
    # Table names
    # -------------------------------------------------------------------------
    silver_table = f"{schema}.{destination_table_name}"
    print(f"Silver Table : {silver_table}")
    print(f"Bronze Source Path : {source_path}")

    # -------------------------------------------------------------------------
    # Read Bronze from Delta path (no hardcoded lakehouse name)
    # -------------------------------------------------------------------------
    bronze_df = spark.read.format("delta").load(source_path)
    source_count = bronze_df.count()
    print(f"Bronze Row Count (All) : {source_count}")

    # -------------------------------------------------------------------------
    # YEAR FILTER (if YEAR is provided)
    # -------------------------------------------------------------------------
    if year_val is not None:
        if "year" in [c.lower() for c in bronze_df.columns]:
            # Find actual 'year' column name (case-insensitive)
            year_col = [c for c in bronze_df.columns if c.lower() == "year"][0]
            bronze_df = bronze_df.filter(col(year_col) == lit(int(year_val)))
            print(f"Applied Year Filter: {year_val}")
        else:
            print("WARNING: YEAR provided but no 'year' column found in bronze data; skipping year filter.")

    bronze_filtered_count = bronze_df.count()
    print(f"Bronze Row Count After Year Filter : {bronze_filtered_count}")

    # -------------------------------------------------------------------------
    # Detect UNITID Column
    # -------------------------------------------------------------------------
    unitid_cols = [c for c in bronze_df.columns if c.lower() == "unitid"]
    if not unitid_cols:
        raise Exception(f"UNITID column not found in bronze data at {source_path}")

    unitid_col = unitid_cols[0]
    print(f"UNITID Column Found : {unitid_col}")

    # # -------------------------------------------------------------------------
    # # Prepare IPEDS lookup dataframe
    # # -------------------------------------------------------------------------
    # ipeds_lookup_df = ipeds_df.withColumn("ipeds_number", trim(col("ipeds_number")))

    # # -------------------------------------------------------------------------
    # # Filter Bronze records using IPEDS list
    # # -------------------------------------------------------------------------
    # filtered_df = (
    #     bronze_df
    #     .withColumn(unitid_col, trim(col(unitid_col).cast("string")))
    #     .join(
    #         broadcast(ipeds_lookup_df),
    #         col(unitid_col) == col("ipeds_number"),
    #         "inner"
    #     )
    #     .drop("ipeds_number")
    # )
    filtered_df=bronze_df

    filtered_count = filtered_df.count()
    print(f"Filtered Row Count : {filtered_count}")

    # -------------------------------------------------------------------------
    # ETL Columns
    # -------------------------------------------------------------------------
    etl_cols_bronze = ["run_id", "created_by", "created_datetime"]
    business_cols = [c for c in filtered_df.columns if c not in etl_cols_bronze]

    # -------------------------------------------------------------------------
    # Record Hash
    # -------------------------------------------------------------------------
    filtered_df = filtered_df.withColumn(
        "record_hash",
        sha2(
            concat_ws(
                "||",
                *[coalesce(col(c).cast("string"), lit("")) for c in business_cols]
            ),
            256
        )
    )

    # -------------------------------------------------------------------------
    # Drop Bronze ETL Columns
    # -------------------------------------------------------------------------
    filtered_df = filtered_df.drop(
        *[c for c in etl_cols_bronze if c in filtered_df.columns]
    )

    # -------------------------------------------------------------------------
    # Add Silver ETL Columns
    # -------------------------------------------------------------------------
    filtered_df = (
        filtered_df
        .withColumn("created_on", current_timestamp())
        .withColumn("created_by", lit(created_by_val))
        .withColumn("modified_on", current_timestamp())
        .withColumn("modified_by", lit(modified_by_val))
    )

    # -------------------------------------------------------------------------
    # Deduplicate
    # -------------------------------------------------------------------------
    filtered_df = filtered_df.dropDuplicates(["record_hash"])
    final_count = filtered_df.count()
    print(f"Final Row Count : {final_count}")

    if final_count == 0:
        raise Exception(
            f"No rows available after IPEDS filtering for table {destination_table_name}"
        )

    # -------------------------------------------------------------------------
    # Create or Merge into Silver Table
    # -------------------------------------------------------------------------
    if not spark.catalog.tableExists(silver_table):
        print(f"Creating Silver Table : {silver_table}")
        (
            filtered_df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table)
        )
        print(f"Created Silver Table : {silver_table}")
    else:
        print(f"Merging into Silver Table : {silver_table}")
        delta_tbl = DeltaTable.forName(spark, silver_table)

        (
            delta_tbl.alias("t")
            .merge(
                filtered_df.alias("s"),
                "t.record_hash = s.record_hash"
            )
            .whenMatchedUpdate(
                set={
                    "modified_on": "current_timestamp()",
                    "modified_by": f"'{modified_by_val}'"
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Merged into Silver Table : {silver_table}")

    print("=" * 80)
    print(f"Completed Successfully : {destination_table_name}")
    print("=" * 80)

    return {
        "bronze_count": int(source_count),
        "bronze_filtered_count": int(bronze_filtered_count),
        "filtered_count": int(filtered_count),
        "target_count": int(final_count),
        "status": "success",
    }


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# Override SOURCE_TABLE and source_path to point to correct bronze table
# The bronze folder name is 'cyear_a', while the silver destination table is 'drvcyear'
SOURCE_TABLE = DESTINATION_TABLE
TARGET_TABLE = DESTINATION_TABLE

source_path = f"{bronze_root_path}/Tables/{SCHEMA}/{SOURCE_TABLE}"
print(f"Overridden Bronze Source Path : {source_path}")

# Define source/target for monitoring based on IPEDS bronze load
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

    result = ingest_silver_layer()

    print("Silver ingestion completed...")
    print(result)

    # Use the correct keys returned by ingest_silver_layer
    # rows_read = int(result.get("bronze_count", 0))
    # rows_written = int(result.get("target_count", 0))
    # rows_rejected = int(result.get("bronze_filtered_count", 0)) - int(result.get("filtered_count", 0))

    rows_read = int(result.get("filtered_count", 0))
    rows_written = int(result.get("target_count", 0))
    rows_rejected = int(result.get("filtered_count", 0)) - int(result.get("target_count", 0))


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
    else:
        print("WARNING: insert_pipeline_monitoring_record is not defined; skipping audit logging for FAILURE.")

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
