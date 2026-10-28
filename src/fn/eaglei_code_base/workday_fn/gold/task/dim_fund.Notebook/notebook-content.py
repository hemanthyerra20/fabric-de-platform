# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c1ad79e0-8d5c-445d-b093-a187bd3af4c3",
# META       "default_lakehouse_name": "lh_fn_silver",
# META       "default_lakehouse_workspace_id": "bedb4898-64aa-4f9d-b9de-e0c4b634ee27",
# META       "known_lakehouses": [
# META         {
# META           "id": "c1ad79e0-8d5c-445d-b093-a187bd3af4c3"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d6e9c4b7-fdc8-4966-afb3-443827650df9",
# META       "workspaceId": "088bba09-dd86-49a0-87f3-ad9331892945"
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
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

VAR_FN_LIB_NAME = "var_fn_process"
PIPELINE_NAME = "nb_gold_layer_ingestion"
MASTER_RUN_ID = ""
RUN_ID = ""
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = ""
LEVEL = ""
LAYER = ""
ENVIRONMENT = ""
WORKSPACE_NAME = ""
WORKSPACE_ID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_variable(VAR_FN_LIB_NAME, "fn_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_FN_LIB_NAME, "lh_fn_silver_id")
gold_lakehouse_id = get_variable(VAR_FN_LIB_NAME, "lh_fn_gold_id")
silver_schema_name = "workday_fn"
gold_schema_name = "workday_fn"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_fund"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_workspace_id = get_variable(VAR_FN_LIB_NAME, "platform_monitoring_workspace_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    fund_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "fund"
    )

    fund_df = spark.read.format("delta").load(fund_path)\
                .drop("created_on", "created_by", "modified_on", "modified_by")
    
    # ----------------------------------------------------------
    # Add Audit Columns
    # ----------------------------------------------------------
    fund_audit_df = add_audit_column(
        fund_df,
        PIPELINE_NAME
    )

    fund_audit_df.cache()

    final_count = fund_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    fund_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_fund")
    write_delta_tbl(source_df = fund_audit_df,target_table_path = fund_path
                ,keys = ["fund_key"])

    fund_audit_df.unpersist()
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    
    raise Exception(f"Failed to create/load dim_fund table: {e}")

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
