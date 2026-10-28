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
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
# META         },
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

# PARAMETERS CELL ********************

VAR_CS_LIB_NAME = "var_cs_process"
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = "starfish_gold_load"
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = "dim_closure_reason"
LEVEL = ""
LAYER = ""
ENVIRONMENT = ""
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "starfish"
gold_schema_name = "starfish"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_closure_reason"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim closure reason

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
try:
    # Source Paths
    track_items_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "track_items_by_surveys"
    )
    

    # Read Source Tables
    track_items_df = spark.read.format("delta").load(track_items_path)
    # item_dates CTE
    item_dates_df = (
        track_items_df
        .select
        ("tracking_item_closure_reason",
        "tracking_item_closure_reason_category"
        ).distinct()
    )
    
    # base_items CTE
    final_df = (
        item_dates_df.alias("sd")
        .select(
            F.sha2(
    F.concat_ws(
        "||",
        F.upper(F.trim(F.col("sd.tracking_item_closure_reason"))),
        F.upper(F.trim(F.col("sd.tracking_item_closure_reason_category")))
        ),
    256
    ).alias("closure_reason_key"),
 
    F.col("sd.tracking_item_closure_reason").alias("closure_reason"),
    F.col("sd.tracking_item_closure_reason_category").alias("closure_reason_category")
    
        )
    )
    final_df=final_df.distinct()
    # Audit Columns
    tracking_item_audit_df = add_audit_column(
        final_df,
        PIPELINE_NAME
    )
    tracking_item_audit_df.cache()
    final_count = tracking_item_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    # Target Path
    tracking_item_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_closure_reason"
    )

    write_delta_tbl(
        tracking_item_audit_df,
        tracking_item_gold_path,
        ["closure_reason_key"]
    )
    tracking_item_audit_df.unpersist()
except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID),
        _safe_str(RUN_ID),
        _safe_str(PIPELINE_NAME),
        _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN),
        _safe_str(SOURCE_NAME),
        _safe_str(ACTIVITY_NAME),
        "failed",
        _safe_str(LEVEL),
        _safe_str(LAYER),
        _safe_str(source_table),
        _safe_str(target_table),
        0,
        0,
        0,
        start_time,
        end_time,
        str(e),
        str(e),
        _safe_str(ENVIRONMENT),
        _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID),
        _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(
        f"Failed to create/load dim_closure_reason table: {e}"
    )
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = datetime.now()
insert_pipeline_monitoring_record( 
    _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
    "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), rows_read, rows_written, 0,
    start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
    rerun_count=0
)
print("SUCCESS logged")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
