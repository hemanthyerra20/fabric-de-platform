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

# MARKDOWN ********************

# ### `abfss_path_builder` usage by layer
# 
# #### Silver layer
# 
# - `pos_all`  
#   `abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "program_of_study_all")`
# 
# - `std_course_reg_path`  
#   `abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_registration")`
# 
# 
# #### Gold layer
# 
# - `dim_program_path`  
#   `abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")`
# 
# - `dim_aca_period`  
#   `abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")`
# 
# - `dim_date_path`  
#   `abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_date")`
# 
# - `students_path`  
#   `abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")`
# 
# - `dim_college_path`  
#   `abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")`
# 
# - `fact_program_enrollment_path`  
#   `abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_program_enrollment")`


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

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_program_enrollment"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact Program Enrollment

# CELL ********************

try:
    pos_all = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "program_of_study_all")
    dim_aca_period = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_date_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_date")
    students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    dim_college_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")
    std_course_reg_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_registration")
    dim_program_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")
    std_enroll_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_enrollment")

    program_df = spark.read.format("delta").load(dim_program_path)\
                    .select("program_key", "program_name", "effective_date", "expiration_date", "academic_unit")

    # FIX (2026-10-21): normalise Silver future-discontinue list (trim/lower, drop empties, dedupe)
    std_enroll_df = (
        spark.read.format("delta").load(std_enroll_path)
        .select(F.trim(F.col("student_id")).alias("student_id"),
                F.lower(F.trim(F.col("future_discontinued_program_of_study"))).alias("dc_program"))
        .filter(F.col("dc_program") != "")
        .distinct()
    )

    # NEW (2026-10-21): students with ANY future discontinue (for NWTC student-level rule, if chosen)
    std_dc_students = std_enroll_df.select("student_id").distinct().withColumn("has_dc", F.lit(1))

    # Read program of study data from the silver layer
    pos_df = spark.read.format("delta").load(pos_all)\
                .filter(F.col("program_status") == "In Progress")

    # Read and select student dimensions
    dim_student_df = (
        spark.read.format("delta")
        .load(students_path)
        .filter(F.col("is_current").cast("int") == 1)   # FIX (2026-10-21): current version only, avoids fan-out
        .select("student_key", "student_id")
    )

    # Read and select college dimensions with alias for academic_unit
    dim_college_df = (
        spark.read.format("delta")
        .load(dim_college_path)
        .selectExpr("college_key", "name as academic_unit")
    )

    # Read date dimension (kept for future joins if needed)
    dim_date_df = spark.read.format("delta").load(dim_date_path)\
                    .select("date_key", "calendar_date")

    dim_acad_period_df = spark.read.format("delta").load(dim_aca_period)\
                            .filter(F.col("session").isNull())\
                            .selectExpr("academic_period_key", "academic_period_name",
                                "declare_start_date", "declare_end_date")

    std_course_reg_df = (
        spark.read.format("delta").load(std_course_reg_path)
        .select(
                F.col("registration_key"),
                F.col("student_id"),
                F.col("academic_unit"),
                F.col("location_code"),
                F.col("campus_location"),
                F.col("program_of_study")
        ))
    joined_df = (
        pos_df.alias("POS")
        .join(dim_student_df, on="student_id", how="left")
        .join(dim_college_df, on="academic_unit", how="left")
        .join(dim_date_df.alias("DDT"), F.col("POS.declare_date") == F.col("DDT.calendar_date"), how="left")
        .join(dim_date_df.alias("EDT"), F.col("POS.expected_completion_date") == F.col("EDT.calendar_date"), how="left")
        .join(dim_date_df.alias("EFDT"), F.col("POS.effective_date") == F.col("EFDT.calendar_date"), how="left")
        .join(dim_acad_period_df.alias("ACA"), (((F.col("POS.declare_date") >= F.col("ACA.declare_start_date")))
                                        & ((F.col("POS.declare_date") <= F.col("ACA.declare_end_date")))), "left")
        .selectExpr(
            "POS.pos_key AS program_enrollment_key",
            "student_key",
            "college_key",
            "DDT.date_key AS declare_date_key",
            "EDT.date_key AS expected_completion_date_key",
            "EFDT.date_key AS effective_date_key",
            "ACA.academic_period_key AS entry_academic_period_key",
            "POS.is_primary_program",
            "POS.earned_credential",
            "POS.program_type",
            "POS.academic_status",
            "POS.academic_unit",
            "POS.program_of_study",
            "POS.declare_date",
            "POS.student_id"
        )
        .drop_duplicates()
    )

    program_joined_df = joined_df.alias("LH")\
                            .join(program_df.alias("PG")
                                ,((F.col("PG.program_name") == F.col("LH.program_of_study"))
                                        & (F.col("PG.effective_date") <= F.col("LH.declare_date"))
                                        & (F.col("PG.expiration_date") >= F.col("LH.declare_date"))
                                         & (F.col("PG.academic_unit") == F.col("LH.academic_unit")))   # FIX (2026-10-21): was >=
                                ,"left")

    # FIX (2026-10-21): flag on the POS row's own program_of_study (trim/lower), not dim_program.program_name,
    # so the flag does not depend on the dim_program match succeeding.
    filtered_df = (
        program_joined_df.alias("LH")
        .selectExpr("*", "row_number() over(partition by program_enrollment_key order by expiration_date desc) AS RN")
        .filter(F.col("RN") == 1)
        .withColumn("pos_program_norm", F.lower(F.trim(F.col("program_of_study"))))
        .withColumn("student_id_norm", F.trim(F.col("student_id")))
        .alias("LH")
        # program-level: this row's own program is in the student's future-discontinue list
        .join(std_enroll_df.alias("RH"),
              (F.col("LH.pos_program_norm") == F.col("RH.dc_program"))
              & (F.col("LH.student_id_norm") == F.col("RH.student_id")),
              "left")
        # student-level: student has ANY future discontinue
        .join(std_dc_students.alias("SL"),
              F.col("LH.student_id_norm") == F.col("SL.student_id"),
              "left")
        .selectExpr(
            "LH.program_enrollment_key",
            "LH.program_key",
            "LH.student_key",
            "LH.college_key",
            "LH.declare_date_key",
            "LH.expected_completion_date_key",
            "LH.effective_date_key",
            "LH.entry_academic_period_key",
            "LH.is_primary_program",
            "LH.earned_credential",
            "LH.program_type",
            "LH.academic_status",
            "CASE WHEN RH.student_id IS NOT NULL THEN 1 ELSE 0 END AS is_future_discontinued",
            "COALESCE(SL.has_dc, 0) AS has_any_future_discontinue"
        )
    )

    fact_enrollment_audit_df = add_audit_column(filtered_df, PIPELINE_NAME)

    final_count = fact_enrollment_audit_df.count()
    rows_read = pos_df.count()   # FIX (2026-10-21): source rows, not rows written
    rows_written = final_count

    fact_program_enrollment_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_program_enrollment")

    write_delta_tbl(fact_enrollment_audit_df, fact_program_enrollment_path, ["program_enrollment_key"])
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load fact_program_enrollment table: {e}")

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
