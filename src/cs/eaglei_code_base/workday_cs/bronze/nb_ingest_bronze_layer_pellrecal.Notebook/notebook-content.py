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
# META           "id": "0277ffad-3784-4551-8d7d-66d1b8ff633a"
# META         },
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

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
import json
from datetime import date, datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

JOB_NAME = "test"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
landing_schema_name = 'workday_cs'
bronze_schema_name = 'workday_cs'
landing_lakehouse_name = 'lh_cs_landing'
bronze_lakehouse_name='lh_cs_bronze'
notebook_name = notebookutils.runtime.context["currentNotebookName"]
config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
#config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
store_workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name,landing_lakehouse_name)
bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name,bronze_lakehouse_name)
landing_table_name ="academic_pell_recalc_date"

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
LAYER="bronze"
SERVICE_NAME = "Ras_Report"
OPERATION_NAME = "PELL_RECALC_DATE"
ENVIRONMENT = "dev"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    academic_pell_recalc_date_landing_path = abfss_path_builder(
        store_workspace_id,
        landing_lakehouse_id,
        landing_schema_name,
        landing_table_name
    )

    academic_pell_recalc_date_df = (
        spark.read.format("delta")
        .load(academic_pell_recalc_date_landing_path)
        .cache()
    )

    # Materialize the cache
    _ = academic_pell_recalc_date_df.count()

    academic_pell_recalc_date_df.show()
except Exception as e:
    # Add any monitoring/logging call here if available from nb_pipeline_monitoring_utils
    print(f"Error loading academic_pell_recalc_date from landing: {e}")
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

distinct_row_count = academic_pell_recalc_date_df.distinct().count()
distinct_key_count = (academic_pell_recalc_date_df.select("academicPeriod").distinct().count())
counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_key_count"])
display(counts_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

academic_pell_recalc_path = abfss_path_builder(store_workspace_id, bronze_lakehouse_id,bronze_schema_name, "academic_pell_recalc")
write_delta_tbl(source_df = academic_pell_recalc_date_df,target_table_path = academic_pell_recalc_path
                ,keys = ["academicPeriod"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
