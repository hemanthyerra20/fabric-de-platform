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
NOTEBOOK_NAME = "dim_location"
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
target_table = f"lh_cs_gold.{gold_schema_name}.dim_location"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim location
# One row per `location_key` (key unchanged). Full rebuild (overwrite) every run.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# dim_location  -  ONE row per location_key
#   location_key = sha2(upper(trim(name)) || upper(trim(type)))  (unchanged)
#   Fix: the key is built from the trimmed/upper-cased values but the rows were
#   de-duplicated on the RAW values, so "Room 1", "ROOM 1" and "Room 1 " gave
#   three rows with the same key. Now grouped on the key; the display name is
#   the spelling used most often in Starfish.
#   Full rebuild -> overwrite (a merge never removes the duplicates already there).
# =============================================================================
try:
    meeting_fulldetail_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "meeting_fulldetail")
    meeting_df = spark.read.format("delta").load(meeting_fulldetail_path)

    loc = (meeting_df
           .select("appointment_location_name", "appointment_location_type")
           .withColumn("location_key",
                       F.sha2(F.concat_ws("||",
                                          F.upper(F.trim(F.col("appointment_location_name"))),
                                          F.upper(F.trim(F.col("appointment_location_type")))), 256)))

    # most-used spelling per key (ties -> alphabetical, so the result is stable)
    w = Window.partitionBy("location_key").orderBy(
        F.col("n").desc(),
        F.col("appointment_location_name").asc_nulls_last(),
        F.col("appointment_location_type").asc_nulls_last())
    location_dim_df = (loc.groupBy("location_key", "appointment_location_name", "appointment_location_type")
                          .agg(F.count("*").alias("n"))
                          .withColumn("_rn", F.row_number().over(w))
                          .filter("_rn = 1")
                          .select("location_key",
                                  F.trim(F.col("appointment_location_name")).alias("appointment_location_name"),
                                  F.trim(F.col("appointment_location_type")).alias("appointment_location_type")))

    location_dim_df = add_audit_column(location_dim_df, PIPELINE_NAME).cache()

    # grain guard: one row per key
    final_count = location_dim_df.count()
    n_keys = location_dim_df.select("location_key").distinct().count()
    assert final_count == n_keys, f"dim_location grain broken: {final_count - n_keys} duplicate location_key rows"
    rows_read = rows_written = final_count

    location_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_location")
    (location_dim_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
                    .save(location_gold_path))
    location_dim_df.unpersist()
    print(f"dim_location rows: {final_count}")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME), "failed", _safe_str(LEVEL),
        _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME), rerun_count=0)
    raise Exception(f"Failed to create/load dim_location table: {e}")


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
