# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
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

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

JOB_NAME = "ingest_bronze_layer"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

layer = "cs"
service_name = "Integrations"
operation_name = "Get_Educational_Taxonomy_Code_References"
source_name = "workday"

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Fetching Config object values
config_df = spark.read.format("delta").load(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_schema_name = get_param_value(config_df,source_name,"bronze_schema_name")
bronze_lh_name = get_param_value(config_df,source_name,"bronze_lakehouse_name")

silver_schema_name = get_param_value(config_df,source_name,"silver_schema_name")
silver_lh_name = get_param_value(config_df,source_name,"silver_lakehouse_name")


parent_object = get_param_value(config_df,source_name, service_name.lower())
bronze_table_name = parent_object[operation_name.lower()]["bronze_table_name"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_workspace_id(var_cs_lib_name, layer, "store")
bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name, bronze_lh_name)
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, silver_lh_name)

bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.format("delta").load(bronze_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ref_df = bronze_df.select(
    F.sha2(
        F.concat_ws(
            "||",
            F.upper(F.trim(F.split(F.col("description"), "-").getItem(0))),
            F.upper(F.trim(F.split(F.col("description"), "-").getItem(1)))
        ),
        256
    ).alias("educational_taxonomy_code_references_key"),
    F.col("id").alias("educational_taxonomy_id"),
    F.trim(F.split(F.col("description"), "-").getItem(0)).alias("code"),
    F.trim(F.split(F.col("description"), "-").getItem(1)).alias("name")
)

ref_audit_df = add_audit_column(ref_df, JOB_NAME)

ref_path = abfss_path_builder(
    workspace_id,
    silver_lakehouse_id,
    silver_schema_name,
    "educational_taxonomy_code_references"
)

write_delta_tbl(
    source_df=ref_audit_df,
    target_table_path=ref_path,
    keys=["educational_taxonomy_code_references_key"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
