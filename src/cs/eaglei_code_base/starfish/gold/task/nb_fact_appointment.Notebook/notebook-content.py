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
ACTIVITY_NAME = "fact_appointment"
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
target_table = f"lh_cs_gold.{gold_schema_name}.fact_appointment"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact appointment
# One row per appointment (`appointment_ext_id`). Full rebuild (overwrite) every run.
# 
# New column (appended): `student_term_key` (links to `fact_student_term_persistence`). No start time -> `time_of_day_band = 'Unknown'`.
# 
# **[FIX 17 Oct-A]** Section match also understands Workday-era Starfish IDs (`course_section_code`). Note: `appointment_key` hashes `course_section_key`, so its value changes for appointments that now find a section. Columns unchanged.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# fact_appointment  -  one row per Starfish appointment (appointment_ext_id)
#   Changes vs previous version:
#     1. One Workday section per appointment (the section join key has no term,
#        so it can match the same course/section number in several terms and
#        duplicate the meeting).
#     2. time_of_day_band: no start time -> "Unknown" (was "5:00 PM or Later").
#     3. New column at the end: student_term_key.
#     4. Full rebuild -> overwrite (no merge).
#   17 Oct: [FIX 17 Oct-A] Workday-era section IDs matched on course_section_code.
# =============================================================================


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


def one_per_name(df, dim_name, key_col, *name_cols):
    """The fact joins dimensions on trimmed / upper-cased names. If two dimension rows
    give the same name after that, the fact row would be duplicated. Keep the lowest
    key per name and print a warning so the dimension can be fixed."""
    w = Window.partitionBy(*name_cols).orderBy(F.col(key_col).asc_nulls_last())
    out = df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n")
    extra = df.count() - out.count()
    if extra:
        print(f"WARNING {dim_name}: {extra} row(s) share a name with another row - lowest key kept; fix the dimension")
    return out


try:
    # Source Paths
    meeting_fulldetail_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "meeting_fulldetail")
    student_dim_path = abfss_path_builder(workspace_id, gold_lakehouse_id, ws_gold_schema_name, "dim_students")
    academic_period_path = abfss_path_builder(workspace_id, gold_lakehouse_id, ws_gold_schema_name, "dim_academic_period")
    course_sections_path = abfss_path_builder(workspace_id, gold_lakehouse_id, ws_gold_schema_name, "dim_course_sections")
    meeting_dim_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_meeting")
    location_dim_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_location")
    provider_dim_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_provider")

    # Read Data
    meeting_df = (spark.read.format("delta").load(meeting_fulldetail_path)
                  .withColumn("course_section_join_key", sf_section_key("course_section_id")))   # [FIX 17 Oct-A]
    student_df = one_per_name(spark.read.format("delta").load(student_dim_path).filter(F.col("is_current") == 1),
                              "dim_students", "student_key", F.trim(F.col("student_id")))
    academic_period_df = spark.read.format("delta").load(academic_period_path).filter(F.col("session").isNull())
    meeting_dim_df = one_per_name(spark.read.format("delta").load(meeting_dim_path), "dim_meeting", "meeting_key",
                                  F.upper(F.trim(F.col("meeting_type"))), F.upper(F.trim(F.col("appointment_type"))), F.upper(F.trim(F.col("appointment_reason"))))
    location_dim_df = one_per_name(spark.read.format("delta").load(location_dim_path), "dim_location", "location_key",
                                   F.upper(F.trim(F.col("appointment_location_name"))), F.upper(F.trim(F.col("appointment_location_type"))))
    provider_dim_df = one_per_name(spark.read.format("delta").load(provider_dim_path), "dim_provider", "provider_key",
                                   F.upper(F.trim(F.col("provider"))), F.col("provider_id").cast("string"))
    # [FIX 17 Oct-A] both lookup keys (existing + Workday course_section_code)
    section_listing_path = abfss_path_builder(workspace_id, silver_lakehouse_id, ws_gold_schema_name, "section_listing_data")
    sections = build_sections(spark.read.format("delta").load(course_sections_path),
                              spark.read.format("delta").load(section_listing_path))

    # Deduplicate by appointment_ext_id (latest record wins) - unchanged
    dedup_window = Window.partitionBy("appointment_ext_id").orderBy(F.col("date").desc())
    appointment_df = (meeting_df
                      .filter(F.col("appointment_ext_id").isNotNull())
                      .withColumn("rn", F.row_number().over(dedup_window))
                      .filter(F.col("rn") == 1)
                      .drop("rn"))

    # [1] one section per appointment
    appointment_df = one_section(appointment_df, ["appointment_ext_id"], "date", sections)

    fact_df = (
        appointment_df.alias("f")
        .join(student_df.alias("ds"), F.trim(F.col("f.student_id")) == F.trim(F.col("ds.student_id")), "left")
        .join(academic_period_df.alias("dap"),
              (F.to_date(F.col("f.date")) >= F.col("dap.period_start_date"))
              & (F.to_date(F.col("f.date")) <= F.col("dap.period_end_date")), "left")
        .join(meeting_dim_df.alias("dm"),
              (F.upper(F.trim(F.col("f.meeting_type"))) == F.upper(F.trim(F.col("dm.meeting_type"))))
              & (F.upper(F.trim(F.col("f.appointment_type"))) == F.upper(F.trim(F.col("dm.appointment_type"))))
              & (F.upper(F.trim(F.col("f.reason"))) == F.upper(F.trim(F.col("dm.appointment_reason")))), "left")
        .join(location_dim_df.alias("dl"),
              # null-safe: a meeting with no location type still finds its row (same as the dim key)
              (F.upper(F.trim(F.col("f.appointment_location_name"))).eqNullSafe(F.upper(F.trim(F.col("dl.appointment_location_name")))))
              & (F.upper(F.trim(F.col("f.appointment_location_type"))).eqNullSafe(F.upper(F.trim(F.col("dl.appointment_location_type"))))),
              "left")
        .join(provider_dim_df.alias("dp"),
              (F.upper(F.trim(F.col("f.provider"))) == F.upper(F.trim(F.col("dp.provider"))))
              & (F.col("f.provider_id").cast("string") == F.col("dp.provider_id").cast("string")), "left")
    )

    start_hour = F.hour(F.col("appointment_start_time"))

    # Final Dataset - existing columns unchanged, student_term_key added at the end
    final_df = fact_df.select(
        F.sha2(F.concat_ws("||",
                           F.col("appointment_ext_id").cast("string"),
                           F.col("f.course_section_key").cast("string"),
                           F.col("date").cast("string"),
                           F.col("ds.student_key").cast("string")), 256).alias("appointment_key"),
        F.col("ds.student_key").alias("student_key"),
        F.date_format(F.to_date("date"), "yyyyMMdd").cast("int").alias("appointment_date_key"),
        F.col("dap.academic_period_key").alias("academic_period_key"),
        F.col("f.course_section_key").alias("course_section_key"),
        F.coalesce(F.col("dm.meeting_key"), F.lit(-1)).alias("meeting_key"),
        F.coalesce(F.col("dl.location_key"), F.lit(-1)).alias("location_key"),
        F.coalesce(F.col("dp.provider_key"), F.lit(-1)).alias("provider_key"),
        F.date_format(F.to_date("referral_created_date"), "yyyyMMdd").cast("int").alias("referral_created_date_key"),
        F.date_format(F.to_date("referral_complete_date"), "yyyyMMdd").cast("int").alias("referral_complete_date_key"),
        F.col("appointment_start_time").alias("scheduled_start_time"),
        F.col("appointment_end_time").alias("scheduled_end_time"),
        F.col("actual_start_time"),
        F.col("actual_end_time"),
        F.col("scheduled_duration_in_minutes"),
        F.col("actual_duration_in_minutes"),
        F.col("wait_time_in_minutes"),
        F.when(F.upper(F.col("attended_session").cast("string")).isin("YES", "Y", "TRUE", "1", "T"), 1)
         .otherwise(0).alias("is_attended"),
        F.when(F.col("cancel_date").isNotNull(), 1).otherwise(0).alias("is_cancelled"),
        F.when((F.col("cancel_date").isNull())
               & (~F.upper(F.coalesce(F.col("attended_session").cast("string"), F.lit(""))).isin("YES", "Y", "TRUE", "1", "T"))
               & (F.to_date("date") < F.current_date()), 1).otherwise(0).alias("is_no_show"),
        # [2] no / unreadable start time -> Unknown
        F.when(start_hour.isNull(), "Unknown")
         .when(start_hour < 8, "Before 8 AM")
         .when(start_hour.between(8, 10), "8:00-10:59 AM")
         .when(start_hour.between(11, 13), "11:00 AM-1:59 PM")
         .when(start_hour.between(14, 16), "2:00-4:59 PM")
         .otherwise("5:00 PM or Later").alias("time_of_day_band"),
        F.when(F.col("f.scheduled_by").isNull(), "other/system")
         .when(F.upper(F.trim(F.col("f.scheduled_by"))) == F.upper(F.trim(F.col("f.student_name"))), "student")
         .when(F.upper(F.trim(F.col("f.scheduled_by"))) == F.upper(F.trim(F.col("f.provider"))), "staff")
         .otherwise("other/system").alias("scheduled_by_type"),
        F.when(F.upper(F.col("f.appointment_type")).like("%ADVIS%"), "Advising")
         .when(F.upper(F.col("f.appointment_type")).like("%COACH%"), "Academic Coaching")
         .when(F.upper(F.col("f.appointment_type")).like("%TUTOR%"), "Tutoring")
         .when(F.upper(F.col("f.appointment_type")).like("%FINANCIAL%"), "Financial Aid")
         .when(F.upper(F.col("f.appointment_type")).like("%TEST%"), "Testing")
         .otherwise("Other").alias("service_area"),
        F.when(F.upper(F.col("prospective_student").cast("string")).isin("YES", "Y", "TRUE", "1", "T"), 1)
         .otherwise(0).alias("is_prospective"),
        F.when(F.col("ds.student_key").isNotNull(), 1).otherwise(0).alias("is_student_matched"),
        # [3] same expression as fact_student_term_persistence (academic_period_key here is the major term)
        F.when(F.col("ds.student_key").isNotNull() & F.col("dap.academic_period_key").isNotNull(),
               F.sha2(F.concat_ws("||", F.col("ds.student_key").cast("string"),
                                  F.col("dap.academic_period_key").cast("string")), 256)).alias("student_term_key"),
    )

    # Audit Columns
    fact_audit_df = add_audit_column(final_df, PIPELINE_NAME).cache()

    # grain guard: one row per appointment
    final_count = fact_audit_df.count()
    n_keys = fact_audit_df.select("appointment_key").distinct().count()
    n_appts = appointment_df.select("appointment_ext_id").distinct().count()
    assert final_count == n_keys == n_appts, \
        f"fact_appointment grain broken: rows={final_count}, keys={n_keys}, appointments={n_appts} (check dimension joins)"
    rows_read = final_count
    rows_written = final_count

    fact_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_appointment")
    # full rebuild every run -> overwrite (no merge, so no duplicate build-up)
    (fact_audit_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
                  .save(fact_gold_path))
    fact_audit_df.unpersist()
    print(f"fact_appointment rows: {final_count}")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME), "failed", _safe_str(LEVEL),
        _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME), rerun_count=0)
    raise Exception(f"Failed to create/load fact_appointment table: {e}")


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
