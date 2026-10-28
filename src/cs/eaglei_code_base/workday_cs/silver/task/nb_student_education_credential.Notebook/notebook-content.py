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

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_NAME = ""
DOMAIN = ""
SERVICE_NAME = ""
OPERATION_NAME = ""
SOURCE_NAME = ""
VAR_CS_LIB_NAME = ""
AUDIT_LAKEHOUSE_NAME = ""
CONFIG_SCHEMA_NAME = ""
CONFIG_TABLE_NAME = ""
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = ""
NOTEBOOK_NAME = ""
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

PIPELINE_NAME = "ingest_silver_layer"
DOMAIN = "cs"
SERVICE_NAME = "Ras_Report"
OPERATION_NAME = "Get_Student_Education_Credential"
SOURCE_NAME = "workday"
VAR_CS_LIB_NAME = "var_cs_process"
AUDIT_LAKEHOUSE_NAME = "lh_platform_monitoring"
CONFIG_SCHEMA_NAME = "config"
CONFIG_TABLE_NAME = "config_param"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    start_time = datetime.now()
    config_workspace_id = get_variable(VAR_CS_LIB_NAME, "platform_monitoring_workspace_id")
    config_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, AUDIT_LAKEHOUSE_NAME)
    config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, CONFIG_SCHEMA_NAME, CONFIG_TABLE_NAME)

    ## Fetching Config object values
    config_df = spark.read.format("delta").load(config_abfss_path)


    bronze_schema_name = get_param_value(config_df,SOURCE_NAME,"bronze_schema_name")
    bronze_lh_name = get_param_value(config_df,SOURCE_NAME,"bronze_lakehouse_name")
    silver_schema_name = get_param_value(config_df,SOURCE_NAME,"silver_schema_name")
    silver_lh_name = get_param_value(config_df,SOURCE_NAME,"silver_lakehouse_name")
    parent_object = get_param_value(config_df,SOURCE_NAME, SERVICE_NAME.lower())
    bronze_table_name = parent_object[OPERATION_NAME.lower()]["bronze_table_name"]
    workspace_id = get_workspace_id(VAR_CS_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, silver_lh_name)

    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.student_education_credential"

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(""), _safe_str(""), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("FAILED logged")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.window import Window

try:
    bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)
    bronze_df = spark.read.format("delta").load(bronze_abfss_path)

    # Replace ancient/invalid dates (< 1582-10-15) with nulls BEFORE casting/writing, to avoid
    # SparkUpgradeException [INCONSISTENT_BEHAVIOR_CROSS_VERSION.WRITE_ANCIENT_DATETIME]
    date_cols = [
        "exit_date",
        "academic_exit_date",
        "educational_credential_earned_date",
        "issued_date",
        "received_date",
        "graduation_date",
        "applicationdate",
    ]

    # Helper function to null‑out dates prior to 1582‑10‑15 and then cast to DateType safely
    def sanitize_and_cast_dates(df, cols, cutoff_str):
        cutoff = F.lit(cutoff_str).cast(DateType())
        for c in cols:
            if c in df.columns:
                # First cast to date (legacy values may be before cutoff)
                df = df.withColumn(c, F.to_date(F.col(c)))
                # Then null‑out truly ancient dates
                df = df.withColumn(
                    c,
                    F.when(F.col(c) < cutoff, F.lit(None).cast(DateType())).otherwise(F.col(c))
                )
        return df

    bronze_df = sanitize_and_cast_dates(bronze_df, date_cols, "1900-01-01")

    flatten_df = bronze_df\
                    .withColumn("educational_institution_attendances_group", F.explode_outer(F.col("educational_institution_attendances_group")))\
                    .withColumn("cf_emi_eagle1_student_external_transcript___official_group", F.explode_outer(F.col("cf_emi_eagle1_student_external_transcript___official_group")))\
                    .withColumn("active_student_applications_group", F.explode_outer(F.col("active_student_applications_group")))\
                    .selectExpr("*", "educational_institution_attendances_group.*",
                        "cf_emi_eagle1_student_external_transcript___official_group.*",
                        "active_student_applications_group.*")\
                    .drop("educational_institution_attendances_group", "cf_emi_eagle1_student_external_transcript___official_group"
                    ,"active_student_applications_group", "created_on", "created_by", "modified_on", "modified_by")\
                    .drop_duplicates()

    col_convert_df = to_lowercase_columns(flatten_df)

    # All date columns have already been cast & sanitized above; avoid extra CAST during selectExpr
    dtype_df = col_convert_df.selectExpr(
                    'UUID() AS credential_key',
                    'educational_institution_attendances',
                    'active_student_applications',
                    'student_external_transcript',
                    'student_id',
                    'cf_lrv_type_from_education_institution',
                    'educational_institution1',
                    'exit_date',
                    'student_educational_attendance_source',
                    'academic_exit_date',
                    'educational_credential',
                    'educational_credential_earned_date',
                    'issued_date',
                    'received_date',
                    'educational_institution',
                    'graduation_date',
                    'student_application',
                    'student_tags',
                    'applicationdate as application_date'
                )

    clean_df = (
    dtype_df
 
    .withColumn(
        "app_dt",
        F.to_date(F.trim("application_date"))
    )
 
    .withColumn(
        "ext_institution",
        F.when(
            F.upper(F.trim("educational_institution1")).isin(
                "", "UNKNOWN", "NULL", "HSED", "GED"
            ),
            None
        ).otherwise(F.trim("educational_institution1"))
    )
 
    .withColumn(
        "app_institution",
        F.when(
            F.upper(F.trim("educational_institution")).isin(
                "", "UNKNOWN", "NULL", "HSED", "GED"
            ),
            None
        ).otherwise(F.trim("educational_institution"))
    )
 
    .withColumn(
        "earned_dt",
        F.when(
            F.to_date("educational_credential_earned_date") >= F.lit("1950-01-01"),
            F.to_date("educational_credential_earned_date")
        )
    )
 
    .withColumn(
        "acad_exit_dt",
        F.when(
            F.to_date("academic_exit_date") >= F.lit("1950-01-01"),
            F.to_date("academic_exit_date")
        )
    )
 
    .withColumn(
        "att_exit_dt",
        F.when(
            F.to_date("exit_date") >= F.lit("1950-01-01"),
            F.to_date("exit_date")
        )
    )
 
    .withColumn(
        "app_grad_dt",
        F.when(
            F.to_date("graduation_date") >= F.lit("1950-01-01"),
            F.to_date("graduation_date")
        )
    )
 
    .withColumn(
        "transcript_recv_dt",
        F.coalesce(
            F.to_date("received_date"),
            F.to_date("issued_date")
        )
    )
 
    .withColumn(
        "has_credential",
        F.when(
            F.trim("educational_credential").isNotNull(),
            1
        ).otherwise(0)
    )
 
    .withColumn(
        "is_hs",
        F.when(
            (
                F.upper("cf_lrv_type_from_education_institution") == "HIGH SCHOOL"
            )
            |
            (
                F.upper("educational_credential").like("%HIGH SCHOOL%")
            )
            |
            (
                F.upper("educational_credential").like("GENERAL EDUCATION DEV%")
            )
            |
            (
                F.upper("educational_institution1").like("%HIGH SCHOOL%")
            )
            |
            (
                F.upper("educational_institution").like("%HIGH SCHOOL%")
            ),
            1
        ).otherwise(0)
    )
)   
    w_student = Window.partitionBy("student_id")
 
    flags_df = (
        clean_df
        .withColumn(
            "has_hs_credential",
            F.max(
                F.when(
                    (F.col("is_hs") == 1)
                    & (F.col("has_credential") == 1),
                    1
                ).otherwise(0)
            ).over(w_student)
        )
    )

    # Per-student HS flags for reuse (ensures hs_institution only for HS students)
    hs_flags = (
        flags_df
        .select("student_id", "is_hs", "has_hs_credential")
        .dropDuplicates(["student_id"])
    )

    latest_app_df = (
    clean_df
    .filter(F.col("student_application").isNotNull())
    .groupBy(
        "student_id",
        "student_application",
        "app_dt"
    )
    .agg(
        F.max("app_institution").alias("app_institution")
    )
)
 
    w_latest = Window.partitionBy("student_id") \
        .orderBy(
            F.col("app_dt").desc_nulls_last(),
            F.col("student_application").desc()
        )
    
    latest_app_df = (
        latest_app_df
        .withColumn(
            "rn",
            F.row_number().over(w_latest)
        )
        .filter("rn = 1")
        .drop("rn")
    )

    latest_app_rows_df = (
    clean_df.alias("c")
    .join(
        latest_app_df.alias("l"),
        [
            F.col("c.student_id")
            == F.col("l.student_id"),
 
            F.col("c.student_application")
            == F.col("l.student_application"),
 
            F.col("c.app_dt")
            .eqNullSafe(F.col("l.app_dt"))
        ],
        "inner"
    )
    .select("c.*")
)

    # Tags only for HS students (is_hs = 1 only)
    tags_df = (
        latest_app_rows_df.alias("la")
        .join(hs_flags.alias("h"), "student_id", "inner")
        .filter(F.col("h.is_hs") == 1)
        .filter(F.col("la.student_tags").isNotNull())
        .withColumn(
            "tag",
            F.explode(
                F.split("la.student_tags", ";")
            )
        )
        .select(
            F.col("la.student_id").alias("student_id"),
            F.col("la.app_institution").alias("app_institution"),
            F.upper(
                F.trim("tag")
            ).alias("tag")
        )
        .distinct()
    )

    # All candidates must be HS rows: is_hs = 1
    cand2 = (
    flags_df
    .filter(
        (F.col("is_hs") == 1)
        & F.col("earned_dt").isNotNull()
    )
    .select(
        "student_id",
        F.col("ext_institution").alias("hs_institution"),
        F.col("earned_dt").alias("grad_dt"),
        F.year("earned_dt").alias("grad_year"),
        F.lit(2).alias("step"),
        F.lit(1).alias("sub_rank"),
        F.col("transcript_recv_dt").alias("recv_dt")
    )
)
 
    cand3 = (
    flags_df
    .filter(
        (F.col("is_hs") == 1)
        & (F.col("acad_exit_dt").isNotNull())
    )
    .select(
        F.col("student_id"),
        F.col("ext_institution").alias("hs_institution"),
        F.col("acad_exit_dt").alias("grad_dt"),
        F.year("acad_exit_dt").alias("grad_year"),
        F.lit(3).alias("step"),
        F.lit(1).alias("sub_rank"),
        F.col("transcript_recv_dt").alias("recv_dt")
    )
)
 
    cand4 = (
    flags_df
    .filter(
        (F.col("is_hs") == 1)
        & (F.col("has_hs_credential") == 1)
        & (F.col("att_exit_dt").isNotNull())
    )
    .select(
        F.col("student_id"),
        F.col("ext_institution").alias("hs_institution"),
        F.col("att_exit_dt").alias("grad_dt"),
        F.year("att_exit_dt").alias("grad_year"),
        F.lit(4).alias("step"),
        F.lit(1).alias("sub_rank"),
        F.col("transcript_recv_dt").alias("recv_dt")
    )
)
 
    # Application-based candidate only for HS students
    cand6 = (
    latest_app_rows_df.alias("la")
    .join(hs_flags.alias("h"), "student_id", "inner")
    .filter(F.col("h.is_hs") == 1)
    .filter(F.col("la.app_grad_dt").isNotNull())
    .select(
        F.col("la.student_id").alias("student_id"),
        F.col("la.app_institution").alias("hs_institution"),
        F.col("la.app_grad_dt").alias("grad_dt"),
        F.year("la.app_grad_dt").alias("grad_year"),
        F.lit(6).alias("step"),
        F.lit(1).alias("sub_rank"),
        F.lit(None).cast("date").alias("recv_dt")
    )
)

 
    cand7_exact = (
    tags_df
    .filter(
        F.col("tag").rlike("^GRADUATION YEAR:\\s*\\d{4}")
    )
    .select(
        F.col("student_id"),
        F.col("app_institution").alias("hs_institution"),
        F.lit(None).cast("date").alias("grad_dt"),
        F.regexp_extract(
            "tag",
            "^GRADUATION YEAR:\\s*(\\d{4})",
            1
        ).cast("int").alias("grad_year"),
        F.lit(7).alias("step"),
        F.lit(1).alias("sub_rank"),
        F.lit(None).cast("date").alias("recv_dt")
    )
)
 

    cand7_before = (
    tags_df
    .filter(
        F.col("tag").rlike("^GRADUATION YEAR ON OR BEFORE\\s*\\d{4}")
    )
    .select(
        F.col("student_id"),
        F.col("app_institution").alias("hs_institution"),
        F.lit(None).cast("date").alias("grad_dt"),
        F.regexp_extract(
            "tag",
            "ON OR BEFORE\\s*(\\d{4})",
            1
        ).cast("int").alias("grad_year"),
        F.lit(7).alias("step"),
        F.lit(2).alias("sub_rank"),
        F.lit(None).cast("date").alias("recv_dt")
    )
)
 
    cand_df = (
    cand2
    .unionByName(cand3)
    .unionByName(cand4)
    .unionByName(cand6)
    .unionByName(cand7_exact)
    .unionByName(cand7_before)
)

    # Additional HS-only safeguard during reranking: prefer institution names containing 'HIGH SCHOOL'
    cand_df = cand_df.withColumn(
        "is_hs_institution_name",
        F.when(F.upper(F.col("hs_institution")).like("%HIGH SCHOOL%"), 1).otherwise(0)
    )

    # Ranking: within same step/sub_rank, prefer HS-looking institution names, then latest recv_dt, then grad_dt, then grad_year
    rank_window = (
    Window.partitionBy("student_id")
    .orderBy(
        "step",
        "sub_rank",
        F.col("is_hs_institution_name").desc(),
        F.col("recv_dt").desc_nulls_last(),
        F.col("grad_dt").desc_nulls_last(),
        F.col("grad_year").desc_nulls_last()
    )
)
 
    ranked_df = (
        cand_df
        .withColumn(
            "rn",
            F.row_number().over(rank_window)
        )
    )

    fallback_window = (
    Window.partitionBy(
        "student_id",
        "grad_year"
    )
    .orderBy(
        "step",
        "sub_rank",
        F.col("is_hs_institution_name").desc(),
        F.col("recv_dt").desc_nulls_last(),
        F.col("grad_dt").desc_nulls_last()
    )
)
 
    inst_fallback_df = (
        cand_df
        .filter(
            F.col("hs_institution").isNotNull()
        )
        .withColumn(
            "rn",
            F.row_number().over(fallback_window)
        )
        .filter("rn = 1")
    )
    final_df = (
    ranked_df.alias("r")
    .join(
        inst_fallback_df.alias("f"),
        [
            F.col("r.student_id") == F.col("f.student_id"),
            F.col("r.grad_year") == F.col("f.grad_year")
        ],
        "left"
    )
    .filter(F.col("r.rn") == 1)
    .select(
        F.col("r.student_id"),
 
        F.coalesce(
            F.col("r.hs_institution"),
            F.col("f.hs_institution"),
            F.lit("Unknown")
        ).alias("hs_institution"),
 
        F.col("r.grad_dt").alias("hs_grad_date"),
 
        F.col("r.grad_year").alias("hs_grad_year")
 
        #F.col("r.step").alias("hs_grad_source_step")
    )
)

    # display(final_df)
         
    audit_df = add_audit_column(final_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    distinct_row_count = audit_df.distinct().count()
    distinct_key_count = (audit_df.select("student_id").distinct().count())
    counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_sg_count"])

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_education_credential")

    # Write using CORRECTED mode (default in Spark 3); dates before cutoff have been nulled out already
    (audit_df
        .write
        .format("delta")
        .mode("overwrite")
        .option("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .save(silver_path)
    )

    audit_df.unpersist()
                   
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("FAILED logged")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

debug_id = "24799536"

print("=== cand_df for student ===", debug_id)
# Show all HS candidates considered, with ranking fields
cand_debug = (cand_df
    .filter(F.col("student_id") == debug_id)
    .orderBy("step", "sub_rank", F.col("grad_dt").desc_nulls_last(), F.col("grad_year").desc_nulls_last())
)
display(cand_debug)

print("=== ranked_df for student (with rn) ===", debug_id)
ranked_debug = ranked_df.filter(F.col("student_id") == debug_id).orderBy("rn")
display(ranked_debug)

print("=== final_df for student ===", debug_id)
final_debug = final_df.filter(F.col("student_id") == debug_id)
display(final_debug)

# Also compare with the separate filtered_final_df if it exists
try:
    print("=== filtered_final_df for student_id_to_filter ===")
    display(filtered_final_df)
except NameError:
    print("filtered_final_df is not defined in this session.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": true,
# META   "editable": false
# META }

# CELL ********************

# Set the student_id you want to filter
student_id_to_filter = "24799536"  # e.g. "123456"

filtered_final_df = final_df.filter(F.col("student_id") == student_id_to_filter)
display(filtered_final_df)

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
