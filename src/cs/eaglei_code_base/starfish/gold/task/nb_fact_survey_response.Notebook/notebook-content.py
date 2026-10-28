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
PIPELINE_NAME = "starfish_gold_load"
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = "fact_survey_response"
LEVEL = ""
LAYER = ""
ENVIRONMENT = ""
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "starfish"
gold_schema_name = "starfish"
ws_gold_schema_name="workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_survey_response"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_survey_response
# One row per progress-survey assignment (survey x respondent x course x survey date). Full rebuild (overwrite) every run - fixes the MERGE duplicate-match error. Columns unchanged.
# 
# **[FIX 17 Oct-A]** Section match also understands Workday-era Starfish IDs (`course_section_code`). Columns unchanged.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# fact_survey_response  -  one row per progress survey assignment
#   (survey x respondent x Starfish course x survey date)
#   Fix for DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE:
#     1. Grain fixed: one row per assignment (copies of the same assignment
#        collapse to the most complete one).
#     2. One Workday section per assignment (the section join key has no term,
#        so it can match the same course/section number in several terms).
#     3. Full rebuild -> overwrite (no merge on nullable keys).
#   Columns unchanged.
#   17 Oct: [FIX 17 Oct-A] Workday-era section IDs matched on course_section_code.
# =============================================================================
NK = ["survey_name", "survey_respondent", "course_id", "survey_date"]   # grain


def section_join_key(col, second_part):
    # Starfish: <course>_x_<section#>  (parts 0 and 2);  Workday: <course>_<section#>_...  (parts 0 and 1)
    return F.trim(F.concat_ws("_", F.split(F.col(col), "_").getItem(0),
                              F.split(F.col(col), "_").getItem(second_part)))


# [FIX 17 Oct-A] Starfish section IDs come in two formats:
#   PeopleSoft era : catalog_termcode_classnbr_campus_section      e.g. 30543300_2255_51088_B_1
#                    -> existing key (course + class number), unchanged
#   Workday era    : Workday course_section_code                   e.g. 10410105_2025_Fall_Session_1_1
#                    = course_code + academic_period (spaces -> "_") + section_number
#                    (same rule as nb_student_xref / Canvas crosswalk) -> matched on the full ID
# Workday-format IDs never matched the old key, so nothing that matched before changes.
WD_FORMAT_RX = r"^[^_]+_20[0-9]{2}_"          # 2nd part is a calendar year -> Workday format


def sf_section_key(col):
    """Starfish section ID -> course_section_join_key (Workday format: full ID; else the existing key)."""
    c = F.trim(F.col(col).cast("string"))
    return (F.when(c.isNull() | (c == ""), F.lit(None).cast("string"))
             .when(c.rlike(WD_FORMAT_RX), c)
             .otherwise(section_join_key(col, 2)))


def build_sections(cs_df, listing_df):
    """Workday sections with BOTH lookup keys: the existing course + class-number key and the
    Workday course_section_code (section_number from Silver section_listing_data).
    The two key shapes never collide; one_section still picks one section per event."""
    base = (cs_df.select("course_section_key", "course_section_id", "course_code", "academic_period",
                         F.to_date("section_start_date").alias("_sec_start"),
                         F.to_date("section_end_date").alias("_sec_end"))
                 .dropDuplicates(["course_section_key"]))
    legacy = base.select("course_section_key", "_sec_start", "_sec_end",
                         section_join_key("course_section_id", 1).alias("course_section_join_key"))
    code = (base.join(listing_df.select("course_section_key", "section_number"), "course_section_key", "inner")
                .select("course_section_key", "_sec_start", "_sec_end",
                        F.concat_ws("_", F.trim(F.col("course_code")),
                                    F.trim(F.regexp_replace(F.trim(F.col("academic_period")), " ", "_")),
                                    F.trim(F.col("section_number").cast("string"))).alias("course_section_join_key")))
    return (legacy.unionByName(code)
                  .filter(F.col("course_section_join_key").isNotNull())
                  .dropDuplicates(["course_section_join_key", "course_section_key"]))


def one_section(df, id_cols, date_col, sections):
    """Attach course_section_key. If the join key matches several Workday sections,
    keep the section running on the event date, else the one starting closest to it."""
    d = F.to_date(F.col(date_col))
    w = Window.partitionBy(*id_cols).orderBy(
        F.coalesce((d >= F.col("_sec_start")) & (d <= F.col("_sec_end")), F.lit(False)).cast("int").desc(),
        F.abs(F.datediff(d, F.col("_sec_start"))).asc_nulls_last(),
        F.col("course_section_key").asc_nulls_last())
    return (df.join(sections, "course_section_join_key", "left")
              .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
              .drop("_rn", "_sec_start", "_sec_end"))


try:
    completed_surveys_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "completed_surveys")
    survey_dim_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_survey")
    course_sections_path = abfss_path_builder(workspace_id, gold_lakehouse_id, ws_gold_schema_name, "dim_course_sections")
    academic_period_path = abfss_path_builder(workspace_id, gold_lakehouse_id, ws_gold_schema_name, "dim_academic_period")
    respondent_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_respondent")

    survey_dim_df = spark.read.format("delta").load(survey_dim_path)
    academic_period_df = spark.read.format("delta").load(academic_period_path).filter(F.col("session").isNull())
    respondent_df = spark.read.format("delta").load(respondent_path)
    # [FIX 17 Oct-A] both lookup keys (existing + Workday course_section_code)
    section_listing_path = abfss_path_builder(workspace_id, silver_lakehouse_id, ws_gold_schema_name, "section_listing_data")
    sections = build_sections(spark.read.format("delta").load(course_sections_path),
                              spark.read.format("delta").load(section_listing_path))

    # [1] one row per assignment: completed copy first, then the latest activity
    is_done = F.upper(F.col("completed").cast("string")).isin("YES", "Y", "TRUE", "1", "T")
    w_dup = Window.partitionBy(*NK).orderBy(
        F.coalesce(is_done, F.lit(False)).cast("int").desc(),
        F.col("draft_date").desc_nulls_last(),
        F.col("survey_viewed").desc_nulls_last(),
        F.col("reminder_count").desc_nulls_last(),
        F.col("days_to_submit").asc_nulls_last())
    base_survey_df = (spark.read.format("delta").load(completed_surveys_path)
                      .select("survey_name", "survey_respondent", "course_id", "survey_date", "survey_viewed",
                              "draft_date", "completed", "reminder_count", "days_to_submit", "flagged_students",
                              "total_flags_raised", "respondent_email", "course_name")
                      .withColumn("_rn", F.row_number().over(w_dup)).filter("_rn = 1").drop("_rn")
                      .withColumn("course_section_join_key", sf_section_key("course_id")))   # [FIX 17 Oct-A]

    # [2] one section per assignment
    base_survey_df = one_section(base_survey_df, NK, "survey_date", sections)

    fact_df = (
        base_survey_df.alias("src")
        .join(survey_dim_df.alias("dsur"), F.trim(F.col("src.survey_name")) == F.trim(F.col("dsur.survey_name")), "left")
        .join(academic_period_df.alias("dap"),
              (F.to_date(F.col("src.survey_date")) >= F.col("dap.period_start_date"))
              & (F.to_date(F.col("src.survey_date")) <= F.col("dap.period_end_date")), "left")
        .join(respondent_df.alias("dr"),
              F.trim(F.col("src.survey_respondent")) == F.trim(F.col("dr.survey_respondent")), "left")
        .select(F.col("src.*"),
                F.col("dsur.survey_key").alias("survey_key"),
                F.col("dap.academic_period_key").alias("academic_period_key"),
                F.col("dr.respondent_key").alias("respondent_key"))
    )

    # Final Dataset - columns unchanged
    final_df = fact_df.select(
        F.col("survey_key"),
        F.col("course_section_key"),
        F.col("academic_period_key"),
        F.col("respondent_key"),
        F.date_format(F.to_date(F.col("survey_date")), "yyyyMMdd").cast("int").alias("survey_date_key"),
        F.when(F.col("survey_viewed").isNotNull(),
               F.date_format(F.to_date(F.col("survey_viewed")), "yyyyMMdd").cast("int")).alias("survey_viewed_date_key"),
        F.when(F.col("draft_date").isNotNull(),
               F.date_format(F.to_date(F.col("draft_date")), "yyyyMMdd").cast("int")).alias("draft_date_key"),
        F.when(F.upper(F.col("completed").cast("string")).isin("YES", "Y", "TRUE", "1", "T"), 1)
         .otherwise(0).alias("is_completed"),
        F.col("reminder_count"),
        F.col("days_to_submit"),
        F.col("flagged_students"),
        F.col("total_flags_raised"),
    )

    survey_response_audit_df = add_audit_column(final_df, PIPELINE_NAME).cache()

    # grain guard: one row per assignment
    final_count = survey_response_audit_df.count()
    n_assign = base_survey_df.select(*NK).distinct().count()
    assert final_count == n_assign, \
        f"fact_survey_response grain broken: {final_count - n_assign} extra rows (a dimension has duplicate names)"
    rows_read = final_count
    rows_written = final_count

    survey_response_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_survey_response")
    # full rebuild every run -> overwrite (fixes the MERGE duplicate-match error)
    (survey_response_audit_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
                             .save(survey_response_gold_path))
    survey_response_audit_df.unpersist()
    print(f"fact_survey_response rows: {final_count}")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME), "failed", _safe_str(LEVEL),
        _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME), rerun_count=0)
    raise Exception(f"Failed to create/load fact_survey_response table: {e}")


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
