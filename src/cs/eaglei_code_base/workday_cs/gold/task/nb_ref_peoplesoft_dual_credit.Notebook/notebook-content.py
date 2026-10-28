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
bronze_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_bronze_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.ref_peoplesoft_dual_credit"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:    
    # ------------------------------------------------------------
    # 1. Build ps_dual
    # ------------------------------------------------------------

    DUAL_CODES = ["1A","1B","1E","8A","8B","9B","9C","9D","9G","9H","9K"]
 
    rcc_flag_path = abfss_path_builder(
        workspace_id, bronze_lakehouse_id, "peoplesoft", "NW_RCC_FLAG"
    )
    
    ps_dual = (
        spark.read.format("delta").load(rcc_flag_path)
        .select(
            F.col("STUD_ID").cast("string").alias("EMPLID"),
            F.initcap(F.trim(F.col("SEMESTER"))).alias("semester"),
            F.col("FISCAL_YR").cast("int").alias("fiscal_yr"),
            F.trim(F.col("RCC_CODE")).alias("CODE"),
        )
        .filter(F.col("CODE").isin(DUAL_CODES))
        .distinct()
    )
    
    # ------------------------------------------------------------
    # 2. Build ps_term  —  fiscal year + semester -> period name
    #    NWTC AY runs Summer -> Fall -> Spring, named by the later year.
    #    So Summer and Fall sit in the PREVIOUS calendar year.
    #      FY2024 Summer = Summer 2023   -> "2023 Summer"
    #      FY2024 Fall   = Fall   2023   -> "2023 Fall"
    #      FY2024 Spring = Spring 2024   -> "2024 Spring"
    # ------------------------------------------------------------
    ps_term = (
        ps_dual
        .withColumn(
            "term_year",
            F.when(F.col("semester").isin("Summer", "Fall"), F.col("fiscal_yr") - 1)
            .otherwise(F.col("fiscal_yr")).cast("string")
        )
        .withColumn("acad_year", F.col("fiscal_yr").cast("string"))
        .withColumn("term_name", F.col("semester"))
        .withColumn("period_name", F.concat_ws(" ", F.col("term_year"), F.col("term_name")))
    )

    # ------------------------------------------------------------
    # 3. Load dimensions
    # ------------------------------------------------------------


    students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")

    dim_student_df = spark.read.format("delta").load(students_path)\
                            .filter(F.col("is_current") == "1")\
                            .select("student_key", "student_id").alias("s")


    dim_aca_period = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")

    dim_acad_period_df = spark.read.format("delta").load(dim_aca_period).alias("a")


    # ------------------------------------------------------------
    # 4. Final query
    # ------------------------------------------------------------

    pl_result = (
        ps_term.alias("p")
        .join(
            dim_student_df,
            (F.col("s.student_id") == F.col("p.EMPLID")),
            "inner"
        )
        .join(
            dim_acad_period_df,
            F.col("a.academic_period_name") == F.col("p.period_name"),
            "inner"
        )
        .select(
            F.col("s.student_key"),
            F.col("a.academic_period_key"),
            F.col("p.code"),
            F.col("p.period_name"),
            F.col("p.acad_year"),
            F.col("p.term_year")
        )
        .distinct()
    )
    pl_audit_df = add_audit_column(pl_result, PIPELINE_NAME)

    final_count = pl_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    pl_dual_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "ref_peoplesoft_dual_credit")
    pl_audit_df.write.format("delta").mode("overwrite").option("mergeSchema", True).save(pl_dual_path)
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
