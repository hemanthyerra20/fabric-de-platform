# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5bd544a5-340c-493c-aa88-61dc1992c614",
# META       "default_lakehouse_name": "lh_cs_landing",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "5bd544a5-340c-493c-aa88-61dc1992c614"
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

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

MASTER_RUN_ID = "test123"
RUN_ID = "test123"
PIPELINE_ID = "test123"
PIPELINE_NAME = "test123"
DOMAIN = "cs"
SOURCE_NAME = "workday"
ACTIVITY_NAME = "notebook activity"
LEVEL="notebook"
LAYER="landing"
ENVIRONMENT = "dev"
TABLENAME=""
SERVICE_NAME = "ras_report"
OPERATION_NAME =  "Get_Student_Enrollment"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_landing"
job_name = "nb_ingest_program_of_study_all"
source_name = "workday"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"
wd_filter_table_name = "workday_extraction_filters"
service_name = "Academic_Foundation"
operation_name = "Get_Students"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
wd_filter_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, wd_filter_table_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)


landing_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name, lh_name)
print('landing:',landing_workspace_id,landing_lakehouse_id)
bronze_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
bronze_lakehouse_id = get_variable(var_cs_lib_name, "lh_cs_bronze_id")
print('bronze:',bronze_workspace_id,bronze_lakehouse_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_df = spark.read.format("delta").load(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Lakehouse Params
landing_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name, lh_name)

landing_abfss_path = abfss_path_builder(landing_workspace_id, landing_lakehouse_id, "workday_cs", "students")


landing_df = spark.read.format("delta")\
        .load(landing_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(landing_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_abfss_path = abfss_path_builder(landing_workspace_id, bronze_lakehouse_id, "workday_cs", "program_of_study_all")

if table_exists(bronze_abfss_path):
        upsert_data(
            bronze_abfss_path,
            landing_df,
            ["student_id", "program_of_study", "status"]
        )
else:
    writer = (
        landing_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(bronze_abfss_path)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
