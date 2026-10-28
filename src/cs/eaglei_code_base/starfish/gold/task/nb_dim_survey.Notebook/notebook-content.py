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
NOTEBOOK_NAME = "dim_survey"
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = ""
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
target_table = f"lh_cs_gold.{gold_schema_name}.dim_survey"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim survey

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
try:
    # Source Paths
    completed_surveys_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "completed_surveys"
    )
    track_items_by_surveys_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "track_items_by_surveys"
    )

    
    # Read Source Tables
    completed_surveys_df = spark.read.format("delta").load(completed_surveys_path)
    track_items_by_surveys_df = spark.read.format("delta").load(track_items_by_surveys_path)
    #academic_period_df = spark.read.format("delta").load(academic_period_path)
    # survey_source CTE
    completed_df = (
        completed_surveys_df
        .filter(F.col("survey_name").isNotNull())
        .select(
            F.col("survey_name"),
            F.to_date("survey_date").alias("survey_date")
        )
    )

    track_items_df = (
        track_items_by_surveys_df
        .filter(F.col("survey").isNotNull())
        .select(
            F.col("survey").alias("survey_name"),
            F.to_date("survey_date").alias("survey_date")
        )
    )
    survey_source_df = completed_df.union(track_items_df).distinct()
    # survey_dates CTE
    survey_dates_df = (
        survey_source_df
        .groupBy("survey_name")
        .agg(
            F.min("survey_date").alias("first_response_date"),
            F.max("survey_date").alias("last_response_date")
        )
    )
    # Academic Period Join
    survey_dim_df = (
        survey_dates_df.alias("sd")
        # .join(
        #     academic_period_df.alias("ap"),
        #     (
        #         F.col("sd.first_response_date") >= F.col("ap.period_start_date")
        #     ) &
        #     (
        #         F.col("sd.first_response_date") <= F.col("ap.period_end_date")
        #     ),
        #     "left"
        # )
        # .filter(F.col("ap.session").isNull())
        .select(
    F.sha2(F.upper(F.trim(F.col("sd.survey_name"))), 256).alias("survey_key"),
    F.col("sd.survey_name"),
    #F.col("ap.academic_period_key"),
    F.col("sd.first_response_date"),
    F.col("sd.last_response_date")
        )
    )
 
    survey_audit_df=survey_dim_df.distinct()
    # Audit Columns
    survey_audit_df = add_audit_column(
        survey_dim_df,
        PIPELINE_NAME
    )
    survey_audit_df.cache()
    final_count = survey_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    # Target Path
    survey_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_survey"
    )

    write_delta_tbl(
        survey_audit_df,
        survey_gold_path,
        ["survey_key"]
    )

    survey_audit_df.unpersist()
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
        f"Failed to create/load dim_survey table: {e}"
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
