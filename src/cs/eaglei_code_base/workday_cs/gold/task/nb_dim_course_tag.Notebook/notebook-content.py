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

VAR_CS_LIB_NAME = ""
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = ""
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

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_course_tag"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Course Tag

# CELL ********************

try:
    course_tag_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_course_tags")
    course_tag_cat_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_course_tag_categories")


    course_tag_df = spark.read.format("delta")\
                            .load(course_tag_path)\
                            .select("student_course_tag_key", "student_course_tag_id", "student_course_tag_name", "student_course_tag_category_id", "academic_unit_id")

    course_tag_cat_df = spark.read.format("delta")\
                            .load(course_tag_cat_path)\
                            .select("student_course_tag_category_id", "private_course_tag_category", "student_course_tag_category_name")

    category_joined_df = course_tag_df.alias("LH")\
                            .join(course_tag_cat_df.alias("RH"),
                                    F.col("LH.student_course_tag_category_id") == F.col("RH.student_course_tag_category_id"),
                                    "inner")\
                            .selectExpr("LH.student_course_tag_key AS course_tag_key", "LH.student_course_tag_id AS id", "LH.student_course_tag_name AS name",
                                    "LH.academic_unit_id", "RH.student_course_tag_category_name AS category_name"
                            ,"RH.private_course_tag_category")

    course_tag_audit_df = add_audit_column(category_joined_df, PIPELINE_NAME)
    course_tag_audit_df.cache()

    final_count = course_tag_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    course_tag_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_tag")

    write_delta_tbl(course_tag_audit_df, course_tag_gold_path, ["course_tag_key"])

    course_tag_audit_df.unpersist()

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_course_tag table: {e}")

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
