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
target_table = f"lh_cs_gold.{gold_schema_name}.dim_grade"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Grade

# CELL ********************

try:
    student_grading_schemes_sil_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_grading_schemes")
    student_grade_references_sil_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_grade_references")

    student_grade_references_df = (spark.read.format("delta").load(student_grade_references_sil_path))


    student_grading_schemes_df = (
        spark.read.format("delta")
        .load(student_grading_schemes_sil_path)
        .select(
            F.col("student_grading_scheme_id").alias("scheme_id"),
            F.col("student_grade_element_id").alias("id")
        )

    ).drop_duplicates()


    joined_df = (
        student_grading_schemes_df.alias("GRADE")
        .join(
            student_grade_references_df.alias("REFERENCE"),
            F.col("GRADE.id") == F.col("REFERENCE.student_grade_id"),
            "left"
        )
        .selectExpr(
            "sha2(concat_ws('||', coalesce(CAST(GRADE.scheme_id AS string), ''), coalesce(CAST(GRADE.id AS string), '')), 256) AS grade_key",
            "GRADE.*",
            "REFERENCE.description as grade"
        )
        .drop("student_grade_id")
    )

    calc_grade_df = (
        joined_df
        .withColumn(
            "is_passing",
            F.when(F.col("Grade").isin("A", "B", "C"), "Y")
            .when(F.col("Grade").isin("D", "F", "I", "W", "NS", "AU", "TC", "LE", "AS", "EX", "NT"), "N")
        )
        .withColumn(
            "is_dfw_grade",
            F.when(F.col("Grade").isin("D", "F", "W"), "Y")
            .when(F.col("Grade").isin("A", "B", "C", "I", "NS", "AU", "TC", "LE", "AS", "EX", "NT"), "N")
        )
        .withColumn(
            "include_in_csr",
            F.when(F.col("Grade").isin("A", "B", "C", "D", "F", "W"), "Y")
            .when(F.col("Grade").isin("I", "NS", "AU", "TC", "LE", "AS", "EX", "NT"), "N")
        )
    )

    calc_grade_df = add_audit_column(calc_grade_df, PIPELINE_NAME)

    calc_grade_df.cache()

    final_count = calc_grade_df.count()
    rows_read = final_count
    rows_written = final_count

    dim_student_grade_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_grade")
    write_delta_tbl(source_df = calc_grade_df,target_table_path = dim_student_grade_path
                    ,keys = ["grade_key"])

    calc_grade_df.unpersist()

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_grade table: {e}")

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
