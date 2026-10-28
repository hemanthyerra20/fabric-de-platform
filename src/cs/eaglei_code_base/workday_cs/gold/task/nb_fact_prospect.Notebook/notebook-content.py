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
target_table = f"lh_cs_gold.{gold_schema_name}.fact_prospect"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact Prospect

# CELL ********************

try:
    dim_program_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")

    program_df = spark.read.format("delta").load(dim_program_path)\
                    .select("program_key", "program_name", "effective_date", "expiration_date")


    # Calculate duration
    df1 = (program_df
        .withColumn(
            "duration",
            F.datediff(F.col("expiration_date"), F.col("effective_date"))
        )
    )

    a = df1.alias("a")
    b = df1.alias("b")

    # Find records that have an overlapping record with a longer duration
    to_remove = (
        a.join(
            b,
            (F.col("a.program_name") == F.col("b.program_name")) &
            (F.col("a.program_key") != F.col("b.program_key")) &
            # Overlap condition
            (F.col("a.effective_date") <= F.col("b.expiration_date")) &
            (F.col("a.expiration_date") >= F.col("b.effective_date")) &
            # b has a longer duration
            (F.col("b.duration") > F.col("a.duration")),
            "inner"
        )
        .select(F.col("a.program_key"))
        .distinct()
    )

    # Keep all records except the shorter overlapping ones
    program_df = df1.join(to_remove, "program_key", "left_anti").drop("duration")
    final_count = program_df.count()
    rows_read = final_count
    rows_written = final_count

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed processing dim_program: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    prospect_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "prospect_360")
    dim_prospect_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_prospect")
    dim_students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")


    dim_prospect_df = spark.read.format("delta")\
                            .load(dim_prospect_path)\
                            .select("prospect_stage", "prospect_source_key")

    prospect_df = spark.read.format("delta")\
                            .load(prospect_path)\
                            .selectExpr("student_id", "to_date(recruitment_created_on) AS created_moment"
                            ,"recruitment_academic_level AS academic_level"
                            ,"student_name AS prospect"
                            ,"primary_program_of_study"
                            ,"entry_stage AS prospect_stage"
                            ,"prospect_wid AS prospect_key")

    student_df = spark.read.format("delta")\
                            .load(dim_students_path)\
                            .filter(F.col("is_current") == 1)\
                            .select("student_key", "student_id")

    prospect_with_source_df = (
        prospect_df
        .join(dim_prospect_df, on="prospect_stage", how="left")
        .join(student_df, on="student_id", how="left")
    )

    pos_joined_df = prospect_with_source_df.alias("LH")\
                        .join(program_df.alias("RH"),
                        ((F.col("RH.program_name") == F.col("LH.primary_program_of_study"))
                                    & (F.col("RH.effective_date") <= F.col("LH.created_moment"))
                                    & (F.col("RH.expiration_date") >= F.col("LH.created_moment")))
                            ,"left")

    prospect_select_df = pos_joined_df.selectExpr(
        "prospect_key",
        "student_key",
        "program_key as primary_program_of_study_key",
        "prospect_source_key",
        "prospect",
        "created_moment AS std_created_on",
        "academic_level"
    )

    pros_audit_df = add_audit_column(prospect_select_df, PIPELINE_NAME)

    final_count = pros_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    pros_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_prospect")

    write_delta_tbl(pros_audit_df, pros_path, ["prospect_key"])
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load fact_prospect table: {e}")

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
