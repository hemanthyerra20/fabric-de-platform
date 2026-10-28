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

# CELL ********************

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

from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.window import Window

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_completion"

INCLUDED_PROGRAM_TYPES = ["Associate Degree", "Certificate", "Technical Diploma"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact Completion (conferred credentials)
# 
# **Grain:** one row per student + program of study + conferral academic period (a conferred credential).
# 
# **Changes (20 Oct 2026):**
# - **[CONF-1]** Population = credentials with a `conferral_date` (any program status), conferred on or before today. Was `program_status = 'Complete'` with `completion_date`.
# - **[CONF-2]** One row per student + program + conferral period, deduplicated across status rows (Silver's `pos_key` includes status, so the same credential appears under Complete / In Progress / Matriculated / Discontinued). Prefer the Complete row.
# - **[CONF-3]** `completion_key` = sha2(student_id || program_of_study || conferral_academic_period). The old `pos_key` contained status, so the table is fully overwritten (see the write cell).
# - **[CONF-4]** `completion_academic_period_key` comes from `conferral_academic_period` (period-name match, major terms), not completion-date ranges. The semantic model keeps working unchanged.
# - **[CONF-5]** New `conferral_date_key`; `completion_date_key` (Workday program completion date) kept for reference.
# - **[PGM-1]** Academic unit compared with equality (was `>=`), used as a ranking preference, not a filter.
# - **[PGM-2]** Deterministic choice when one name maps to several programs: unit match → credential match → version closest to the conferral date → latest expiration → program_key.
# - **[PGM-3]** Version picked on `conferral_date` (was `declare_date`); if no version covers that date, the nearest version of the same program is used; if the name is not in dim_program at all, `program_key = '-1'`.
# - **[PGM-4]** Uniqueness asserted on each join that could fan out, instead of row_number hiding it.


# CELL ********************

try:
    # ---------- paths ----------
    pos_all              = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "program_of_study_all")
    std_course_reg_path  = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_registration")
    dim_aca_period_path  = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_date_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_date")
    students_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    dim_college_path     = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")
    dim_campus_loc_path  = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_campus_location")
    dim_program_path     = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")

    def assert_unique(df, key_cols, label):
        dup = df.groupBy(*key_cols).count().filter("count > 1")
        n = dup.count()
        if n > 0:
            dup.show(10, truncate=False)
            raise Exception(f"[PGM-4] {label}: {n} duplicate key(s) on {key_cols}")

    # ---------- [CONF-1] conferred credentials ----------
    pos_df = (
        spark.read.format("delta").load(pos_all)
            .filter(F.col("conferral_date").isNotNull())
            .filter(F.col("conferral_date") <= F.current_date())
    )
    # [CONF-7] Workday sometimes sends conferral_date without the period -> derive it from TERM dates
    # Spring 01/01-05/22, Summer 05/23-08/05, Fall 08/06-12/31 (e.g. 2024-12-20 -> "2024 Fall")
    cd = F.col("conferral_date")
    md = F.month(cd) * 100 + F.dayofmonth(cd)
    yr = F.year(cd).cast("string")
    derived_period = (F.when(md <= 522, F.concat(yr, F.lit(" Spring")))
                       .when(md <= 805, F.concat(yr, F.lit(" Summer")))
                       .otherwise(F.concat(yr, F.lit(" Fall"))))
    pos_df = pos_df.withColumn("conferral_academic_period",
                               F.coalesce(F.col("conferral_academic_period"), derived_period))
 
    # [CONF-6] credential program types only
    pos_df = pos_df.filter(F.col("program_type").isin(INCLUDED_PROGRAM_TYPES))

    # ---------- [CONF-2] one row per student + program + conferral period ----------
    w_cred = (Window.partitionBy("student_id", "program_of_study", "conferral_academic_period")
                    .orderBy(F.when(F.col("program_status") == "Complete", 0).otherwise(1),
                             F.col("effective_date").desc_nulls_last(),
                             F.col("pos_key")))
    cred_df = (
        pos_df.withColumn("rn", F.row_number().over(w_cred))
              .filter(F.col("rn") == 1).drop("rn")
              # [CONF-3] credential key (status removed)
              .withColumn("completion_key", F.sha2(F.concat_ws("||",
                    F.col("student_id").cast("string"),
                    F.col("program_of_study").cast("string"),
                    F.col("conferral_academic_period").cast("string")), 256))
    )

    # ---------- campus: first registration per student + program (unchanged logic) ----------
    loc_expr = lambda out_col: (
        F.when((F.col("location_code") == "") | (F.col("campus_location") == ""), "Unknown")
         .when(F.col("location_code").isNull() | F.col("campus_location").isNull(), "Unknown")
         .when(F.lower(F.col("location_code")).like("%online%") | F.lower(F.col("campus_location")).like("%online%"), "Online")
         .otherwise(F.col(out_col))
    )
    base_reg_df = (
        spark.read.format("delta").load(std_course_reg_path)
            .select(
                "student_id",
                loc_expr("location_code").alias("location_code"),
                loc_expr("campus_location").alias("campus_location"),
                F.explode(F.split(F.col("program_of_study"), ";")).alias("program_of_study"),
                F.coalesce(
                    F.to_timestamp(F.col("registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                    F.to_timestamp(F.col("waitlist_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                ).alias("converted_reg_ts"))
            .withColumn("program_of_study", F.trim(F.col("program_of_study")))
    )
    w_reg = Window.partitionBy("student_id", "program_of_study").orderBy(F.col("converted_reg_ts"))
    std_reg_df = (base_reg_df.withColumn("rn", F.row_number().over(w_reg))
                             .filter(F.col("rn") == 1)
                             .select("student_id", "location_code", "campus_location", "program_of_study"))

    # ---------- dimensions ----------
    dim_student_df = spark.read.format("delta").load(students_path).select("student_key", "student_id")\
                            .filter(F.col("is_current") == '1')
    dim_college_df = spark.read.format("delta").load(dim_college_path).selectExpr("college_key", "name AS academic_unit")
    dim_date_df    = spark.read.format("delta").load(dim_date_path).select("date_key", "calendar_date")
    dim_campus_loc_df = (spark.read.format("delta").load(dim_campus_loc_path)
                              .select("campus_location_key", "name", "primary_campus_location"))
    # [CONF-4] major terms only; academic_period_name is year-first ("2025 Fall"), same as the RaaS
    dim_acad_period_df = (spark.read.format("delta").load(dim_aca_period_path)
                               .filter(F.col("session").isNull())
                               .select("academic_period_key", "academic_period_name"))

    # [PGM-4] the joins below must not fan out
    assert_unique(dim_student_df, ["student_id"], "dim_students")
    assert_unique(dim_college_df, ["academic_unit"], "dim_college")
    assert_unique(dim_date_df, ["calendar_date"], "dim_date")
    assert_unique(dim_campus_loc_df, ["name", "primary_campus_location"], "dim_campus_location")
    assert_unique(dim_acad_period_df, ["academic_period_name"], "dim_academic_period (major terms)")

    joined_df = (
        cred_df.alias("POS")
        .join(dim_student_df.alias("STU"), F.col("POS.student_id") == F.col("STU.student_id"), "left")
        .join(dim_college_df.alias("COL"), F.col("POS.academic_unit") == F.col("COL.academic_unit"), "left")
        .join(dim_date_df.alias("DDT"), F.col("POS.declare_date") == F.col("DDT.calendar_date"), "left")
        .join(dim_date_df.alias("EDT"), F.col("POS.completion_date") == F.col("EDT.calendar_date"), "left")
        .join(dim_date_df.alias("CDT"), F.col("POS.conferral_date") == F.col("CDT.calendar_date"), "left")
        .join(dim_acad_period_df.alias("ACA"),
              F.col("POS.conferral_academic_period") == F.col("ACA.academic_period_name"), "left")
        .join(std_reg_df.alias("SR"),
              (F.col("POS.student_id") == F.col("SR.student_id")) &
              (F.col("POS.program_of_study") == F.col("SR.program_of_study")), "left")
        .join(dim_campus_loc_df.alias("LOC"),
              (F.col("SR.location_code") == F.col("LOC.name")) &
              (F.col("SR.campus_location") == F.col("LOC.primary_campus_location")), "left")
        .select(
            F.col("POS.completion_key"),
            F.col("LOC.campus_location_key"),
            F.col("STU.student_key"),
            F.col("COL.college_key"),
            F.col("DDT.date_key").alias("declare_date_key"),
            F.col("EDT.date_key").alias("completion_date_key"),
            F.col("CDT.date_key").alias("conferral_date_key"),
            F.col("ACA.academic_period_key").alias("completion_academic_period_key"),
            F.col("POS.is_primary_program"),
            F.col("POS.earned_credential"),
            F.col("POS.program_type"),
            F.col("POS.academic_status"),
            F.col("POS.academic_unit"),
            F.col("POS.program_of_study"),
            F.col("POS.conferral_date"),
            F.col("POS.conferral_academic_period"))
    )
    # [PGM-4] fail instead of hiding a fan-out
    assert_unique(joined_df, ["completion_key"], "joined_df before program join")

    # ---------- [PGM-1..3] program resolution ----------
    program_df = (spark.read.format("delta").load(dim_program_path)
                       .filter(F.col("program_key") != "-1")
                       .select("program_key", "program_name", "effective_date", "expiration_date",
                               "academic_unit", "credential_type"))

    cand = (
        joined_df.alias("LH")
        .join(program_df.alias("PG"), F.col("PG.program_name") == F.col("LH.program_of_study"), "left")
        .withColumn("unit_rank",                                   # [PGM-1] equality, as a preference
                    F.when(F.col("PG.academic_unit") == F.col("LH.academic_unit"), 0).otherwise(1))
        .withColumn("cred_rank",                                   # [PGM-2] "Technical Diploma" vs "Technical Diploma - Technical Diploma"
                    F.when(F.col("PG.credential_type").startswith(F.col("LH.earned_credential")), 0).otherwise(1))
        .withColumn("date_gap",                                    # [PGM-3] 0 = version covers the conferral date
                    F.when(F.col("LH.conferral_date").between(F.col("PG.effective_date"), F.col("PG.expiration_date")), 0)
                     .when(F.col("LH.conferral_date") < F.col("PG.effective_date"),
                           F.datediff(F.col("PG.effective_date"), F.col("LH.conferral_date")))
                     .otherwise(F.datediff(F.col("LH.conferral_date"), F.col("PG.expiration_date"))))
    )
    w_pg = (Window.partitionBy(F.col("LH.completion_key"))
                  .orderBy(F.col("unit_rank"), F.col("cred_rank"), F.col("date_gap").asc_nulls_last(),
                           F.col("PG.expiration_date").desc_nulls_last(), F.col("PG.program_key")))

    fact_df = (
        cand.withColumn("rn", F.row_number().over(w_pg))
            .filter(F.col("rn") == 1)
            .select(
                F.col("LH.completion_key").alias("completion_key"),
                F.coalesce(F.col("PG.program_key"), F.lit("-1")).alias("program_key"),   # [PGM-3]
                F.col("LH.campus_location_key").alias("campus_location_key"),
                F.col("LH.student_key").alias("student_key"),
                F.col("LH.college_key").alias("college_key"),
                F.col("LH.declare_date_key").alias("declare_date_key"),
                F.col("LH.completion_date_key").alias("completion_date_key"),
                F.col("LH.conferral_date_key").alias("conferral_date_key"),
                F.col("LH.completion_academic_period_key").alias("completion_academic_period_key"),
                F.col("LH.is_primary_program").alias("is_primary_program"),
                F.col("LH.earned_credential").alias("earned_credential"),
                F.col("LH.program_type").alias("program_type"),
                F.col("LH.academic_status").alias("academic_status"))
    )

    fact_audit_df = add_audit_column(fact_df, PIPELINE_NAME)
    fact_audit_df.cache()
    final_count = fact_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    assert_unique(fact_audit_df, ["completion_key"], "fact_completion final")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to build fact_completion: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Checks before writing
# Expected (NWTC): unduplicated students AY2024-2025 ≈ 3,450 and AY2025-2026 ≈ 3,475 (Silver gave 3,449 / 3,474). Nursing Assistant AY2025-2026 = 498.

# CELL ********************

dim_ap_years = (spark.read.format("delta").load(dim_aca_period_path)
                    .select("academic_period_key", "academic_year"))
dim_pg_names = spark.read.format("delta").load(dim_program_path).select("program_key", "program_name")

chk = fact_audit_df.join(dim_ap_years, fact_audit_df.completion_academic_period_key == dim_ap_years.academic_period_key, "left")

print("Unduplicated students / credentials by academic year:")
(chk.groupBy("academic_year")
    .agg(F.countDistinct("student_key").alias("students"), F.count("*").alias("credentials"))
    .orderBy(F.col("academic_year").desc()).show(12, truncate=False))

print("Nursing Assistant, 2025-2026 (expect 498):")
(chk.join(dim_pg_names, "program_key", "left")
    .filter((F.col("program_name") == "Nursing Assistant") & (F.col("academic_year") == "2025-2026"))
    .agg(F.countDistinct("student_key").alias("students")).show())

print("Unresolved keys (should be 0 or close to it):")
fact_audit_df.select(
    F.sum(F.when(F.col("program_key") == "-1", 1).otherwise(0)).alias("program_unknown"),
    F.sum(F.when(F.col("completion_academic_period_key").isNull(), 1).otherwise(0)).alias("period_null"),
    F.sum(F.when(F.col("student_key").isNull(), 1).otherwise(0)).alias("student_null"),
    F.sum(F.when(F.col("conferral_date_key").isNull(), 1).otherwise(0)).alias("conferral_date_null"),
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Write
# Full overwrite: `completion_key` changed in [CONF-3], so a merge on the new key would leave the old status-based rows in the table. The fact is fully recomputed from Silver on every run, so overwrite stays correct for regular runs too.

# CELL ********************

try:
    fact_completion_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_completion")
    (fact_audit_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")        # new column conferral_date_key
        .save(fact_completion_path))
    fact_audit_df.unpersist()
except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), rows_read, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to write fact_completion: {e}")

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
