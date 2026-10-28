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
ACTIVITY_NAME = "fact_tracking_item"
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
target_table = f"lh_cs_gold.{gold_schema_name}.fact_tracking_item"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_tracking_item
# One row per Starfish tracking item. Reads Silver + Gold dimensions only (no Gold facts). Full rebuild (overwrite) every run.
# 
# New columns (appended): `student_term_key` (links to `fact_student_term_persistence`), `final_grade_key` (student's final grade in that section, `-1` if none).
# 
# **[FIX 17 Oct-A]** Section match also understands Workday-era Starfish IDs (`course_section_code`), so items from 2025 Fall onward get their section and final grade.
# **[FIX 17 Oct-B]** Adds the items not raised by a survey (manual / system flags, kudos, referrals and To-Dos) from Silver `tracking_fulldetail`, one row per `item_ext_id`. Columns unchanged.

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# =============================================================================
# fact_tracking_item  -  one row per Starfish tracking item
#   Reads Silver + Gold DIMENSIONS only (no Gold facts).
#   Changes vs previous version:
#     1. Grain fixed: one row per Silver item (student, item, raise date,
#        section, survey); exact duplicate copies collapse to one.
#     2. One Workday section per item (the section join key has no term, so
#        it can match the same course/section number in several terms).
#     3. First intervention from Silver meeting_fulldetail (was Gold
#        fact_appointment) - same rule: first attended meeting on/after raise.
#     4. New columns at the end: student_term_key, final_grade_key.
#     5. Full rebuild -> overwrite (no merge).
#   17 Oct:
#     [FIX 17 Oct-A] Workday-era section IDs matched on course_section_code.
#     [FIX 17 Oct-B] + manual / system items (flags, kudos, referrals, To-Dos)
#                    from tracking_fulldetail; item_ext_id added to the grain;
#                    'Cleared' counts as closed; own clear date for those items;
#                    one-row-per-name guard on the dimensions (as fact_appointment).
#   Columns unchanged.
# =============================================================================
YES = ["YES", "Y", "TRUE", "1", "T"]
NK = ["student_id", "tracking_item_name", "create_date", "section", "survey", "item_ext_id"]   # grain [FIX 17 Oct-B: + item_ext_id, NULL for survey items]


def rd(path):
    return spark.read.format("delta").load(path)


def sf_silver(t):
    return abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, t)


def sf_gold(t):
    return abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, t)


def wd_silver(t):
    return abfss_path_builder(workspace_id, silver_lakehouse_id, ws_gold_schema_name, t)


def wd_gold(t):
    return abfss_path_builder(workspace_id, gold_lakehouse_id, ws_gold_schema_name, t)


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
    """[FIX 17 Oct-B] Same guard as nb_fact_appointment: the fact joins dimensions on trimmed /
    upper-cased names; if two dimension rows give the same name, keep the lowest key and warn."""
    w = Window.partitionBy(*name_cols).orderBy(F.col(key_col).asc_nulls_last())
    out = df.withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n")
    extra = df.count() - out.count()
    if extra:
        print(f"WARNING {dim_name}: {extra} row(s) share a name with another row - lowest key kept; fix the dimension")
    return out


def sf_extra_items(survey_items, read=None):
    """[FIX 17 Oct-B] Starfish items NOT raised by a progress survey (trigger MANUAL / SYSTEM:
    flags, kudos, referrals, To-Dos). track_items_by_surveys holds survey-raised items only
    (checked 17 Oct: Gold matched the SURVEY counts exactly; 71,677 manual/system items missing).
    Source: tracking_fulldetail (full Starfish export), one row per item_ext_id, reshaped to the
    columns (and data types) of survey_items. Columns survey_items lacks are dropped."""
    read = read or (lambda p: spark.read.format("delta").load(p))
    fd = read(sf_silver("tracking_fulldetail"))
    fd = (fd.filter(F.upper(F.trim(F.coalesce(F.col("trigger").cast("string"), F.lit("")))) != "SURVEY")
            .filter(F.col("item_ext_id").isNotNull() & F.col("item_name").isNotNull())
            .withColumn("_xrn", F.row_number().over(
                Window.partitionBy("item_ext_id").orderBy(F.col("clear_date").desc_nulls_last())))
            .filter("_xrn = 1"))
    sid_type = fd.schema["student_id"].dataType.simpleString()
    sid = (F.col("student_id").cast("bigint").cast("string")
           if sid_type in ("tinyint", "smallint", "int", "bigint", "float", "double") or sid_type.startswith("decimal")
           else F.trim(F.col("student_id").cast("string")))
    mapped = fd.select(
        sid.alias("student_id"),
        F.trim(F.col("item_name")).alias("tracking_item_name"),
        F.col("raise_date").alias("create_date"),
        F.coalesce(F.trim(F.col("section")), F.trim(F.col("course_external_id"))).alias("section"),
        F.col("survey").alias("survey"),
        F.col("raiser_name").alias("instructor"),
        F.col("status").alias("status"),
        F.col("closure_reason").alias("tracking_item_closure_reason"),
        F.col("closure_reason_category").alias("tracking_item_closure_reason_category"),
        F.col("item_ext_id").cast("string").alias("item_ext_id"),
        F.col("clear_date").alias("_own_clear_date"))
    return mapped.select(*[(F.col(f.name) if f.name in mapped.columns else F.lit(None)).cast(f.dataType).alias(f.name)
                           for f in survey_items.schema.fields])


try:
    # ---- dimensions (Gold) ---------------------------------------------------
    # [FIX 17 Oct-B] one row per joined name (no-op unless a dimension has duplicates)
    student_df = one_per_name(rd(wd_gold("dim_students")).filter(F.col("is_current") == 1),
                              "dim_students", "student_key", F.trim(F.col("student_id")))
    tracking_item_df = one_per_name(rd(sf_gold("dim_tracking_item")), "dim_tracking_item", "tracking_item_key",
                                    F.trim(F.col("tracking_item_name")))
    survey_df = one_per_name(rd(sf_gold("dim_survey")), "dim_survey", "survey_key", F.trim(F.col("survey_name")))
    respondent_df = one_per_name(rd(sf_gold("dim_respondent")), "dim_respondent", "respondent_key",
                                 F.upper(F.trim(F.col("survey_respondent"))))
    closure_reason_df = one_per_name(rd(sf_gold("dim_closure_reason")), "dim_closure_reason", "closure_reason_key",
                                     F.trim(F.col("closure_reason")), F.trim(F.col("closure_reason_category")))
    academic_period_df = rd(wd_gold("dim_academic_period")).filter(F.col("session").isNull())
    # [FIX 17 Oct-A] both lookup keys (existing + Workday course_section_code)
    sections = build_sections(rd(wd_gold("dim_course_sections")), rd(wd_silver("section_listing_data")))

    # ---- tracking items (Silver): one row per item ----------------------------
    w_dup = Window.partitionBy(*NK).orderBy(           # same item loaded twice: keep the most complete copy
        F.upper(F.coalesce(F.col("status"), F.lit(""))).isin("T").cast("int").desc(),
        (F.trim(F.coalesce(F.col("tracking_item_closure_reason_category"), F.lit(""))) != "").cast("int").desc(),
        F.col("survey_date").desc_nulls_last(),
        F.col("instructor").asc_nulls_last())
    fd_clear_type = rd(sf_silver("tracking_fulldetail")).schema["clear_date"].dataType
    survey_items = (rd(sf_silver("track_items_by_surveys"))
                    .filter(F.col("tracking_item_name").isNotNull())
                    .withColumn("student_id", F.trim("student_id"))
                    .withColumn("tracking_item_name", F.trim("tracking_item_name"))
                    .withColumn("item_ext_id", F.lit(None).cast("string"))             # [FIX 17 Oct-B]
                    .withColumn("_own_clear_date", F.lit(None).cast(fd_clear_type))    # [FIX 17 Oct-B]
                    .withColumn("_rn", F.row_number().over(w_dup)).filter("_rn = 1").drop("_rn"))
    # [FIX 17 Oct-B] + items not raised by a survey;  [FIX 17 Oct-A] new section key
    tracking_df = (survey_items.unionByName(sf_extra_items(survey_items, rd))
                   .withColumn("course_section_join_key", sf_section_key("section")))
    tracking_df = one_section(tracking_df, NK, "create_date", sections)

    # clear date per student x item (as before; grouped on trimmed values)
    tracking_full_detail_df = (rd(sf_silver("tracking_fulldetail"))
                               .groupBy(F.trim("student_id").alias("student_id"),
                                        F.trim("item_name").alias("item_name"))
                               .agg(F.max("clear_date").alias("clear_date")))

    fact_df = (
        tracking_df.alias("src")
        .join(student_df.alias("ds"), F.col("src.student_id") == F.trim(F.col("ds.student_id")), "left")
        .join(tracking_item_df.alias("dti"),
              F.col("src.tracking_item_name") == F.trim(F.col("dti.tracking_item_name")), "left")
        .join(survey_df.alias("dsur"), F.trim(F.col("src.survey")) == F.trim(F.col("dsur.survey_name")), "left")
        .join(respondent_df.alias("dr"),
              F.upper(F.trim(F.col("src.instructor"))) == F.upper(F.trim(F.col("dr.survey_respondent"))), "left")
        .join(closure_reason_df.alias("dcr"),
              (F.trim(F.col("src.tracking_item_closure_reason")) == F.trim(F.col("dcr.closure_reason")))
              & (F.trim(F.col("src.tracking_item_closure_reason_category")) == F.trim(F.col("dcr.closure_reason_category"))),
              "left")
        .join(academic_period_df.alias("dap"),
              (F.to_date(F.col("src.create_date")) >= F.col("dap.period_start_date"))
              & (F.to_date(F.col("src.create_date")) <= F.col("dap.period_end_date")), "left")
        .join(tracking_full_detail_df.alias("tfd"),
              (F.col("src.student_id") == F.col("tfd.student_id"))
              & (F.col("src.tracking_item_name") == F.col("tfd.item_name")), "left")
        .select(
            F.col("src.*"),
            F.coalesce(F.col("ds.student_key"), F.lit(-1)).alias("student_key"),
            F.coalesce(F.col("dti.tracking_item_key"), F.lit(-1)).alias("tracking_item_key"),
            F.coalesce(F.col("dsur.survey_key"), F.lit(-1)).alias("survey_key"),
            F.coalesce(F.col("dr.respondent_key"), F.lit(-1)).alias("respondent_key"),
            F.col("dap.academic_period_key"),
            F.col("dcr.closure_reason_key"),
            # [FIX 17 Oct-B] manual / system items carry their own clear date
            F.coalesce(F.col("src._own_clear_date"), F.col("tfd.clear_date")).alias("clear_date"),
        )
    )

    # ---- first intervention: first ATTENDED meeting on/after the raise date ------
    # (same rule as before, read from Silver: dedup by appointment_ext_id, latest date)
    meetings = (rd(sf_silver("meeting_fulldetail"))
                .filter(F.col("appointment_ext_id").isNotNull())
                .withColumn("_rn", F.row_number().over(
                    Window.partitionBy("appointment_ext_id").orderBy(F.col("date").desc())))
                .filter("_rn = 1")
                .filter(F.upper(F.col("attended_session").cast("string")).isin(YES))
                .join(student_df.select(F.trim("student_id").alias("_sid"), F.col("student_key").alias("_m_student_key")),
                      F.trim(F.col("student_id")) == F.col("_sid"), "inner")
                .select("_m_student_key", F.to_date("date").alias("_meet_dt")))

    first_intervention_df = (
        fact_df.select(*NK, "student_key")
        .join(meetings, (F.col("student_key").cast("string") == F.col("_m_student_key").cast("string"))
              & (F.col("_meet_dt") >= F.to_date(F.col("create_date"))), "inner")
        .groupBy(*[F.col(c).alias(f"_fi_{c}") for c in NK])
        .agg(F.date_format(F.min("_meet_dt"), "yyyyMMdd").cast("int").alias("first_intervention_date_key")))
    fact_df = (fact_df.join(first_intervention_df,
                            [F.col(c).eqNullSafe(F.col(f"_fi_{c}")) for c in NK], "left")
                      .drop(*[f"_fi_{c}" for c in NK]))

    # ---- final grade of the student in that section (Silver course_registration,
    #      grade logic copied from nb_fact_registration; one registration per
    #      student x section: graded first, then latest registered)
    ref_std_df = (rd(wd_gold("ref_student_grade"))
                  .withColumn("calc_term", F.regexp_extract("term", r"^([^ ]+)", 1))
                  .withColumn("calc_term_year", F.regexp_extract("term", r"^[^ ]+ (.*)$", 1))
                  .select(F.col("student_id").alias("_r_student_id"),
                          F.concat(F.col("catalog_#").cast("string"), F.lit("_"), F.col("class_#").cast("string"),
                                   F.lit("_"), F.col("calc_term_year"), F.lit("_"), F.col("calc_term")).alias("_r_section"),
                          F.col("original_grade").alias("_r_original_grade"),
                          F.col("current_grade").alias("_r_current_grade")))
    calc_section = (F.when(F.col("course_section_id").endswith("Session_1"),
                           F.trim(F.regexp_replace("course_section_id", "_Session_1$", "")))
                     .when(F.col("course_section_id").endswith("Session_2"),
                           F.trim(F.regexp_replace("course_section_id", "_Session_2$", "")))
                     .when(F.col("course_section_id").contains("J_Term"),
                           F.trim(F.regexp_replace("course_section_id", "_J_Term", "Spring")))
                     .otherwise(F.col("course_section_id")))
    grade_df = rd(wd_gold("dim_grade")).select(F.lower("grade").alias("_g_grade"), F.col("scheme_id").alias("_g_scheme"),
                                               F.col("grade_key").cast("string").alias("final_grade_key"))
    reg_grade = (rd(wd_silver("course_registration"))
                 .select("registration_key", "student_id", "course_section_id", "grade", "grading_scheme",
                         "registered_date")
                 .withColumn("_calc_section", calc_section)
                 .join(ref_std_df,
                       (F.upper(F.col("_calc_section")) == F.upper(F.col("_r_section")))
                       & (F.upper(F.col("_r_current_grade")) == F.upper(F.col("grade")))
                       & (F.col("_r_student_id") == F.col("student_id")), "left")
                 .withColumn("_grade", F.lower(F.coalesce(F.col("_r_original_grade"), F.col("grade"))))
                 .join(grade_df, (F.col("_grade") == F.col("_g_grade")) & (F.col("grading_scheme") == F.col("_g_scheme")), "left")
                 .join(student_df.select("student_id", F.col("student_key").cast("string").alias("_reg_student_key")),
                       "student_id", "inner")
                 .join(rd(wd_gold("dim_course_sections")).select("course_section_id", "course_section_key")
                       .dropDuplicates(["course_section_id"]), "course_section_id", "inner")
                 .withColumn("_reg_dt", F.to_timestamp(F.col("registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")))
    w_reg = Window.partitionBy("_reg_student_key", "course_section_key").orderBy(
        F.col("final_grade_key").isNull().asc(), F.col("_reg_dt").desc_nulls_last(), F.col("registration_key").desc())
    reg_grade = (reg_grade.withColumn("_rn", F.row_number().over(w_reg)).filter("_rn = 1")
                 .select("_reg_student_key", F.col("course_section_key").alias("_reg_section_key"), "final_grade_key"))

    fact_df = (fact_df.join(reg_grade,
                            (F.col("student_key").cast("string") == F.col("_reg_student_key"))
                            & (F.col("course_section_key").cast("string") == F.col("_reg_section_key").cast("string")),
                            "left")
                      .drop("_reg_student_key", "_reg_section_key"))

    # ---- grain guard: one row per Silver tracking item ------------------------
    fact_df = fact_df.cache()
    n_rows = fact_df.count()
    n_items = fact_df.select(*NK).distinct().count()
    assert n_rows == n_items, f"fact_tracking_item grain broken: {n_rows - n_items} extra rows (a dimension has duplicate names)"

    clear_status = (F.when(F.col("tracking_item_closure_reason_category").isNull()
                           | (F.trim(F.col("tracking_item_closure_reason_category")) == ""), "Active")
                     .when(F.upper(F.col("tracking_item_closure_reason_category")) == "NEGATIVE", "Negative")
                     .when(F.upper(F.col("tracking_item_closure_reason_category")) == "ENROLLMENT ENDED", "Negative")
                     .when(F.upper(F.col("tracking_item_closure_reason_category")).isin("UNKNOWN/OTHER"), "Positive")
                     .when(F.upper(F.col("tracking_item_closure_reason_category")).isin("IRRELEVANT", "MISTAKE"), "Excluded")
                     .otherwise("Positive"))

    # ---- final dataset: existing columns unchanged, 2 new columns at the end -----
    final_df = fact_df.select(
        F.col("student_key"),
        F.col("tracking_item_key"),
        F.col("survey_key"),
        F.col("course_section_key"),
        F.col("academic_period_key"),
        F.date_format(F.to_date("create_date"), "yyyyMMdd").cast("int").alias("raise_date_key"),
        F.when(F.col("survey_date").isNotNull(),
               F.date_format(F.to_date("survey_date"), "yyyyMMdd").cast("int")).alias("survey_date_key"),
        F.col("respondent_key"),
        F.col("closure_reason_key"),
        F.when(F.col("clear_date").isNotNull(),
               F.date_format(F.to_date(F.col("clear_date")), "yyyyMMdd").cast("int")).alias("close_date_key"),
        F.col("first_intervention_date_key"),
        F.when(F.col("first_intervention_date_key").isNotNull(),
               F.datediff(F.to_date(F.col("first_intervention_date_key").cast("string"), "yyyyMMdd"),
                          F.to_date("create_date"))).alias("days_to_first_intervention"),
        F.col("status"),
        clear_status.alias("clear_status_category"),
        F.when(clear_status != "Excluded", 1).otherwise(0).alias("is_reportable_item"),
        F.when(F.col("clear_date").isNotNull(),
               F.datediff(F.to_date(F.col("clear_date")), F.to_date(F.col("create_date")))).alias("days_to_clear"),
        F.when(F.upper(F.col("status")).isin("T", "CLEARED"), 1).otherwise(0).alias("is_closed"),   # [FIX 17 Oct-B] + Cleared
        F.when(F.col("student_key").cast("string") != "-1", 1).otherwise(0).alias("is_student_matched"),
        # ---- new ----
        # same expression as fact_student_term_persistence (academic_period_key here is the major term)
        F.when((F.col("student_key").cast("string") != "-1") & F.col("academic_period_key").isNotNull(),
               F.sha2(F.concat_ws("||", F.col("student_key").cast("string"),
                                  F.col("academic_period_key").cast("string")), 256)).alias("student_term_key"),
        F.coalesce(F.col("final_grade_key"), F.lit("-1")).alias("final_grade_key"),
    )

    fact_audit_df = add_audit_column(final_df, PIPELINE_NAME)
    final_count = n_rows
    rows_read = final_count
    rows_written = final_count

    fact_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_tracking_item")
    # full rebuild every run -> overwrite (no merge, so no duplicate build-up)
    (fact_audit_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
                  .save(fact_gold_path))
    fact_df.unpersist()
    print(f"fact_tracking_item rows: {final_count}")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME), "failed", _safe_str(LEVEL),
        _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME), rerun_count=0)
    raise Exception(f"Failed to create/load fact_tracking_item table: {e}")


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

# CELL ********************

# MAGIC %%sql
# MAGIC -- after load: row count and how many rows found each key
# MAGIC SELECT COUNT(*)                                                        AS rows_,
# MAGIC        SUM(CASE WHEN student_key = '-1' THEN 1 ELSE 0 END)             AS no_student,
# MAGIC        SUM(CASE WHEN academic_period_key IS NULL THEN 1 ELSE 0 END)    AS no_term,
# MAGIC        SUM(CASE WHEN course_section_key IS NULL THEN 1 ELSE 0 END)     AS no_section,
# MAGIC        SUM(CASE WHEN student_term_key IS NULL THEN 1 ELSE 0 END)       AS no_student_term,
# MAGIC        SUM(CASE WHEN final_grade_key = '-1' THEN 1 ELSE 0 END)         AS no_final_grade,
# MAGIC        SUM(CASE WHEN first_intervention_date_key IS NOT NULL THEN 1 ELSE 0 END) AS with_first_intervention
# MAGIC FROM lh_cs_gold.starfish.fact_tracking_item

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
