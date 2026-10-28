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

JOB_NAME = "ingest_silver_layer"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

layer = "cs"
service_name = "Academic_Foundation"
operation_name = "Get_Student_Tags"
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

display(bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flat_bronze_df = bronze_df\
                        .withColumn("student_tag_data", F.explode(F.col("student_tag_data")))\
                        .select(
                            # F.col("wid"),
                            # F.col("academic_period_id"),
                            F.col("student_tag_data.*")
                        )

display(flat_bronze_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flat_bronze_df = flat_bronze_df.select(F.col("Student_Tag_ID"),
                                      F.col("Student_Tag_Name"),
                                      F.explode_outer(F.col("Student_Tag_Category_Reference")).alias("student_tag_category_reference"),
                                      F.col("Student_Tag_Inactive"))

explode_id_list = [
    {
                    "col_name": "student_tag_category_reference",
                    "col_not_req": [],
                    "col_rename": {
                        "wid":"student_tag_category_reference_wid"
                    }
    },]

exploded_ids_df = explode_ids(flat_bronze_df, explode_id_list)

exploded_ids_df = to_lowercase_columns(exploded_ids_df)

audit_df = add_audit_column(exploded_ids_df, JOB_NAME)
display(audit_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

distinct_row_count = audit_df.distinct().count()
distinct_key_count = (audit_df.select("student_tag_id", "student_tag_name").distinct().count())
counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_maintained_academic_periods_count"])
display(counts_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_tag")
# write_delta_tbl(source_df = audit_df
#                 ,target_table_path = silver_path
#                 ,keys = ["id,academic_period_name"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Academic_Unit_Shareable_Data

# CELL ********************

flat_bronze_df = bronze_df\
                        .withColumn("student_tag_data", F.explode(F.col("student_tag_data")))\
                        .select(
                            # F.col("wid"),
                            # F.col("academic_period_id"),
                            F.col("student_tag_data.*"))
flat_bronze_df = (
    flat_bronze_df
    .select(
        F.col("Student_Tag_ID"),
        F.col("academic_unit_shareable_data.Academic_Unit_Reference").alias("Academic_Unit_Reference"),
        F.col("academic_unit_shareable_data.Academic_Level_Reference").alias("Academic_Level_Reference")
    )
)

flat_bronze_df = flat_bronze_df.select(F.col("Student_Tag_ID"),
                                      F.col("academic_unit_reference")
                                    #  , F.col("academic_level_reference")
                                    )

explode_id_list = [
    {
                    "col_name": "academic_unit_reference",
                    "col_not_req": [],
                    "col_rename": {
                        "wid":"academic_unit_reference_wid"
                    }
    },
    # {
    #                 "col_name": "academic_level_reference",
    #                 "col_not_req": [],
    #                 "col_rename": {
    #                     "wid":"academic_level_reference_wid"
    #                 }
    # },
    ]

exploded_ids_df = explode_ids(flat_bronze_df, explode_id_list)

exploded_ids_df = to_lowercase_columns(exploded_ids_df)

audit_df = add_audit_column(exploded_ids_df, JOB_NAME)
display(audit_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(flat_bronze_df)
audit_df = add_audit_column(flat_bronze_df, JOB_NAME)
display(audit_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

distinct_row_count = audit_df.distinct().count()
distinct_key_count = (audit_df.select("student_tag_id").distinct().count())
counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_maintained_academic_periods_count"])
#display(counts_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_unit_shareable_data")
# write_delta_tbl(source_df = audit_df
#                 ,target_table_path = silver_path
#                 ,keys = ["id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
