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
ACTIVITY_NAME = "fact_student_term_persistence"
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
cs_gold_schema_name="canvas"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_student_term_persistence"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # fact_student_term_persistence
# 
# **Grain:** one row per student x **major term** (Fall / Spring / Summer). Key: `student_term_key = sha2(student_key || major_term_period_key)`, the same expression the Canvas fact uses. Sessions roll up to their major term.
# 
# **Sources:** Silver + Gold **dimensions** only (no Gold facts).
# 
# | Need | Source | Logic copied from |
# |---|---|---|
# | Registrations, census, credits, load status, program, college, campus, dual credit | Silver `workday_cs.course_registration` (+ `course_sections`, `delivery_mode_references`, `course_section_academic_unit_data`, `student_transfer_credit`) | `nb_fact_enrollment_snapshot` (census rule) |
# | Final grade for CSR counts | same registrations + `ref_student_grade` + `dim_grade` | `nb_fact_registration` |
# | Completions | Silver `workday_cs.program_of_study_all` (Complete) | `nb_fact_completion` |
# | Tracking items | Silver `starfish.track_items_by_surveys` + manual / system items from `tracking_fulldetail` [FIX 17 Oct-B] | `nb_fact_tracking_item` |
# | Meetings | Silver `starfish.meeting_fulldetail` | `nb_fact_appointment` |
# | Pell | `dim_student_term` | - |
# 
# **Population:** every student x major term with a registration, a tracking item or a meeting. `is_prd_enrolled = 1` only when at least one registration counts at census.
# 
# **Rules**
# - Census (copied from the snapshot): not Waitlisted; Unregistered rows with no drop date ignored; census date = later of PRD (`pell_recalculation_date`) and period start; counted if not dropped / withdrawn by the census date, or - for a late-start section - by the section's start date.
# - Next major term: Fall -> Spring, Spring -> Fall, Summer -> Fall (Summer is never the target).
# - Retention measured only once the next term's census date has passed. Retention flags are NULL for students not enrolled at census.
# - Completed = a 'Complete' program of study with a completion date from the start of the term up to the next term's census date.
# - Clear segment: Positive wins, then Negative, then Active. Only reportable items (Irrelevant / Mistake excluded). Kudos have no clear segment.
# - Graduation status: Graduated / Not Graduated (at least one completed credential as of refresh).
# - Overwrite every run (full rebuild). Run after the Workday and Starfish Silver loads.


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from zoneinfo import ZoneInfo

# ---- Settings copied from nb_fact_enrollment_snapshot (keep in step) ---------
INCLUDE_LATE_START = True      # late-start sections counted on their own start date
RCC_DUAL = ["1A", "1B", "1E", "8A", "8B", "9B", "9C", "9D", "9G", "9H", "9K"]
DROP_COL = "drop_date"
WITHDRAW_COL = "withdrawal_date"
TODAY = datetime.now(ZoneInfo("America/Chicago")).date()

YES = ["1", "Y", "YES", "TRUE", "T"]


def wd_silver(t):
    return abfss_path_builder(workspace_id, silver_lakehouse_id, ws_gold_schema_name, t)


def wd_gold(t):          # Workday Gold - DIMENSIONS / REF tables only
    return abfss_path_builder(workspace_id, gold_lakehouse_id, ws_gold_schema_name, t)


def sf_silver(t):
    return abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, t)


def sf_gold(t):          # Starfish Gold - DIMENSIONS only
    return abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, t)


def rd(path):
    return spark.read.format("delta").load(path)


def is_yes(c):
    return F.upper(F.trim(F.col(c).cast("string"))).isin(YES)


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


def sf_all_items():
    """[FIX 17 Oct-B] All Starfish items, as nb_fact_tracking_item: survey-raised
    (track_items_by_surveys) + manual / system (tracking_fulldetail, one per item_ext_id)."""
    survey = (rd(sf_silver("track_items_by_surveys"))
              .filter(F.col("tracking_item_name").isNotNull())
              .select(F.trim("student_id").alias("student_id"), F.trim("tracking_item_name").alias("tracking_item_name"),
                      "create_date", "section", "survey", "tracking_item_closure_reason_category")
              .withColumn("item_ext_id", F.lit(None).cast("string")))
    return survey.unionByName(sf_extra_items(survey, rd))


try:
    # =========================================================================
    # 0. Dimensions
    # =========================================================================
    periods = rd(wd_gold("dim_academic_period"))

    # every period (session or full term) -> its major term
    major = (periods.filter(F.col("is_major_term") == 1)
                    .select("academic_period_name",
                            F.col("academic_period_key").alias("major_term_period_key"),
                            F.col("period_start_date").alias("term_start_date"),
                            F.col("period_end_date").alias("term_end_date"),
                            F.greatest("pell_recalculation_date", "period_start_date").alias("term_census_date")))
    period_to_major = (periods.select("academic_period_key", "academic_period_name")
                              .join(major.select("academic_period_name", "major_term_period_key"),
                                    "academic_period_name", "inner")
                              .select("academic_period_key", "major_term_period_key"))

    students = (rd(wd_gold("dim_students")).filter(F.col("is_current") == 1)
                .select(F.trim("student_id").alias("student_id"), "student_key")
                .dropDuplicates(["student_id"]))

    # =========================================================================
    # 1. REGISTRATIONS + CENSUS  (build_events / census_member copied from
    #    nb_fact_enrollment_snapshot; grade fields added for CSR counts)
    # =========================================================================
    cr = rd(wd_silver("course_registration"))

    campus_list = F.transform(F.split(F.col("campus_location"), ";"), lambda x: F.trim(x))
    keyword = F.regexp_extract(F.col("location_code"), "-(.*)", 1)
    matches = F.filter(campus_list, lambda x: x.contains(keyword))
    campus_clean = (F.when(F.coalesce(F.col("campus_location"), F.lit("")) == "", F.col("campus_location"))
                     .when(F.size(matches) > 0, matches.getItem(0))
                     .otherwise(campus_list.getItem(0)))

    reg = (cr.filter(F.coalesce(F.col("registration_status"), F.lit("")) != "Waitlisted")
             .filter(~((F.col("registration_status") == "Unregistered") & F.col(DROP_COL).isNull()))
             .select("registration_key", F.trim("student_id").alias("student_id"),
                     "primary_program_of_study", "academic_period", "course_section_id",
                     "course_listing", "location_code", "credits", "grade", "grading_scheme", "registration_status",
                     campus_clean.alias("campus_location_clean"),
                     F.to_date(DROP_COL).alias("drop_dt"),
                     F.to_date(WITHDRAW_COL).alias("withdrawal_dt"))
             .dropDuplicates(["registration_key"]))

    modality = (rd(wd_silver("course_sections")).select("course_section_id", "course_section_key", "delivery_mode_id")
                .dropDuplicates(["course_section_id"])
                .join(rd(wd_silver("delivery_mode_references"))
                      .select("delivery_mode_id", F.col("description").alias("course_modality")),
                      "delivery_mode_id", "left")
                .drop("delivery_mode_id"))
    unit = (rd(wd_silver("course_section_academic_unit_data"))
            .groupBy("course_section_key").agg(F.min("academic_unit_id").alias("academic_unit_id")))
    sec_start = (rd(wd_gold("dim_course_sections"))
                 .groupBy("course_section_id").agg(F.min("section_start_date").alias("section_start_date")))
    tax = (rd(wd_gold("dim_course_section_taxonomy"))
           .groupBy("course_section_id").agg(F.concat_ws(",", F.collect_set("code")).alias("tax_code_list")))
    tc = (rd(wd_silver("student_transfer_credit"))
          .groupBy(F.col("course").alias("course_listing"), F.trim("student_id").alias("student_id"))
          .agg(F.sum("credits").alias("transfer_credits")))
    ap = periods.select(F.col("period_name").alias("academic_period"), "academic_period_key",
                        "period_start_date", "pell_recalculation_date")

    no_loc = ((F.coalesce(F.col("location_code"), F.lit("")) == "")
              | (F.coalesce(F.col("campus_location_clean"), F.lit("")) == ""))
    location = (F.when(no_loc & F.col("course_modality").isin("Online Live", "Online"), F.lit("Online_Online"))
                 .when(no_loc, F.lit("Unknown_Unknown"))
                 .otherwise(F.concat(F.col("location_code"), F.lit("_"), F.col("campus_location_clean"))))

    def still_enrolled_on(d):
        return F.col("end_dt").isNull() | (F.col("end_dt") > d)

    started_by_census = F.col("sec_start") <= F.col("census_dt")
    late_start = still_enrolled_on(F.col("sec_start")) if INCLUDE_LATE_START else F.lit(False)
    census_member = F.when(started_by_census, still_enrolled_on(F.col("census_dt"))).otherwise(late_start)

    rcc = F.array(*[F.lit(c) for c in RCC_DUAL])
    tax_codes = F.transform(F.split(F.coalesce(F.col("tax_code_list"), F.lit("")), ","),
                            lambda c: F.upper(F.trim(c)))

    events = (reg.join(modality, "course_section_id", "inner")
                 .join(unit, "course_section_key", "left")
                 .join(sec_start, "course_section_id", "left")
                 .join(tax, "course_section_id", "left")
                 .join(tc, ["student_id", "course_listing"], "left")
                 .join(ap, "academic_period", "inner")
                 .withColumn("end_dt", F.least("drop_dt", "withdrawal_dt"))
                 .withColumn("sec_start", F.coalesce("section_start_date", "period_start_date"))
                 .withColumn("census_dt", F.greatest("pell_recalculation_date", "period_start_date"))
                 .withColumn("location", location)
                 # counted only once the period's census date has passed (snapshot: census_dt < TODAY)
                 .withColumn("is_census", F.coalesce(census_member & (F.col("census_dt") < F.lit(TODAY)), F.lit(False)))
                 .withColumn("is_rcc_dual", F.arrays_overlap(tax_codes, rcc))
                 .join(period_to_major, "academic_period_key", "inner")
                 .join(students, "student_id", "inner"))

    # ---- final grade for CSR counts (copied from nb_fact_registration) ------
    ref_grade = (rd(wd_gold("ref_student_grade"))
                 .withColumn("calc_term", F.regexp_extract("term", r"^([^ ]+)", 1))
                 .withColumn("calc_term_year", F.regexp_extract("term", r"^[^ ]+ (.*)$", 1))
                 .select(F.trim("student_id").alias("student_id"),
                         F.concat(F.col("catalog_#").cast("string"), F.lit("_"), F.col("class_#").cast("string"),
                                  F.lit("_"), F.col("calc_term_year"), F.lit("_"), F.col("calc_term")).alias("_ref_section"),
                         "original_grade", "current_grade"))
    calc_section = (F.when(F.col("course_section_id").endswith("Session_1"),
                           F.trim(F.regexp_replace("course_section_id", "_Session_1$", "")))
                     .when(F.col("course_section_id").endswith("Session_2"),
                           F.trim(F.regexp_replace("course_section_id", "_Session_2$", "")))
                     .when(F.col("course_section_id").contains("J_Term"),
                           F.trim(F.regexp_replace("course_section_id", "_J_Term", "Spring")))
                     .otherwise(F.col("course_section_id")))
    grades = rd(wd_gold("dim_grade")).select(F.lower("grade").alias("_g_grade"), F.col("scheme_id").alias("_g_scheme"),
                                             "is_passing", "include_in_csr")

    reg_grade = (events.withColumn("_calc_section", calc_section).alias("e")
                 .join(ref_grade.alias("r"),
                       (F.upper(F.col("e._calc_section")) == F.upper(F.col("r._ref_section")))
                       & (F.upper(F.col("r.current_grade")) == F.upper(F.col("e.grade")))
                       & (F.col("r.student_id") == F.col("e.student_id")), "left")
                 .select(F.col("e.registration_key").alias("registration_key"),
                         F.lower(F.coalesce(F.col("r.original_grade"), F.col("e.grade"))).alias("_grade"),
                         F.col("e.grading_scheme").alias("grading_scheme"))
                 .dropDuplicates(["registration_key"])
                 .join(grades, (F.col("_grade") == F.col("_g_grade")) & (F.col("grading_scheme") == F.col("_g_scheme")), "left")
                 .withColumn("_rn", F.row_number().over(Window.partitionBy("registration_key").orderBy(F.col("_g_scheme"))))
                 .filter("_rn = 1")
                 .select("registration_key",
                         F.coalesce(is_yes("include_in_csr"), F.lit(False)).alias("_in_csr"),
                         F.coalesce(is_yes("is_passing"), F.lit(False)).alias("_passing")))
    events = events.join(reg_grade, "registration_key", "left")

    ps_dual = (rd(wd_gold("ref_peoplesoft_dual_credit")).select("student_key", "academic_period_key")
               .join(period_to_major, "academic_period_key", "inner")
               .select("student_key", "major_term_period_key").distinct()
               .withColumn("_ps_dual", F.lit(True)))

    # ---- one row per student x major term -----------------------------------
    k = ["student_key", "major_term_period_key"]
    reg_term = (events.groupBy(*k).agg(
        F.max(F.col("is_census").cast("int")).alias("is_prd_enrolled"),
        F.sum(F.when(F.col("is_census"), F.col("credits").cast("decimal(18,5)"))).alias("credits"),
        F.sum(F.when(F.col("is_census"), F.col("transfer_credits"))).cast("double").alias("transfer_credits"),
        F.max(F.when(F.col("is_census"), F.col("is_rcc_dual").cast("int"))).alias("_rcc_dual"),
        F.sum(F.when(F.coalesce(F.col("_in_csr"), F.lit(False)), 1).otherwise(0)).alias("csr_eligible_count"),
        F.sum(F.when(F.coalesce(F.col("_in_csr"), F.lit(False)) & F.coalesce(F.col("_passing"), F.lit(False)), 1)
               .otherwise(0)).alias("csr_passed_count"),
    ))

    # program / college / campus: from census rows (else any row); program with the most
    # credits, college + campus of the earliest-starting section (as the snapshot does)
    pick = Window.partitionBy(*k).orderBy(F.col("is_census").desc(), F.col("credits").desc_nulls_last(),
                                          F.col("sec_start").asc_nulls_last(), F.col("registration_key"))
    attrs = (events.withColumn("_rn", F.row_number().over(pick)).filter("_rn = 1")
                   .select(*k, "primary_program_of_study", "academic_unit_id", "location",
                           # program valid on the census snapshot date, as the snapshot does
                           F.coalesce("pell_recalculation_date", "census_dt").alias("_prog_dt")))

    programs = rd(wd_gold("dim_program")).select("program_name", "program_key", "effective_date", "expiration_date")
    attrs = (attrs.join(programs, (F.col("primary_program_of_study") == F.col("program_name"))
                                  & (F.col("_prog_dt") >= F.to_date("effective_date"))
                                  & (F.col("_prog_dt") <= F.to_date("expiration_date")), "left")
                  .withColumn("_rn", F.row_number().over(Window.partitionBy(*k).orderBy(
                      F.col("expiration_date").desc_nulls_last(), F.col("effective_date").desc_nulls_last())))
                  .filter("_rn = 1")
                  .join(rd(wd_gold("dim_college")).select(F.col("college_key").alias("primary_college_key"),
                                                          F.col("id").alias("academic_unit_id")),
                        "academic_unit_id", "left")
                  .join(rd(wd_gold("dim_campus_location")).select(
                            "campus_location_key",
                            F.concat(F.col("name"), F.lit("_"), F.col("primary_campus_location")).alias("location")),
                        "location", "left")
                  .select(*k, F.col("program_key").alias("primary_program_key"),
                          "primary_college_key", "campus_location_key"))

    reg_term = (reg_term.join(attrs, k, "left")
                        .join(ps_dual, k, "left")
                        .withColumn("is_dual_credit_flag",
                                    F.when((F.coalesce("_rcc_dual", F.lit(0)) == 1) | F.col("_ps_dual").isNotNull(), 1)
                                     .otherwise(0))
                        .drop("_rcc_dual", "_ps_dual"))

    # load status per major term (all sessions together), copied from the snapshot
    full = F.when(F.col("_term_name").contains("Summer"), F.lit(6)).otherwise(F.lit(12))
    reg_term = (reg_term.join(major.select(F.col("major_term_period_key"), F.col("academic_period_name").alias("_term_name")),
                              "major_term_period_key", "left")
                        .withColumn("load_status",
                                    F.when(F.col("is_prd_enrolled") != 1, F.lit(None).cast("string"))
                                     .when(F.col("credits") >= full, "Full-Time")
                                     .when(F.col("credits") >= full / 2, "Half-Time")
                                     .otherwise("Part-Time"))
                        .drop("_term_name"))

    # =========================================================================
    # 2. TRACKING ITEMS  (Silver; student, term and clear status as in
    #    nb_fact_tracking_item)
    # =========================================================================
    term_ranges = major.select("major_term_period_key", "term_start_date", "term_end_date")

    raw_cat = F.col("tracking_item_closure_reason_category")
    clear_status = (F.when(raw_cat.isNull() | (F.trim(raw_cat) == ""), "Active")
                     .when(F.upper(raw_cat).isin("NEGATIVE", "ENROLLMENT ENDED"), "Negative")
                     .when(F.upper(raw_cat) == "UNKNOWN/OTHER", "Positive")
                     .when(F.upper(raw_cat).isin("IRRELEVANT", "MISTAKE"), "Excluded")
                     .otherwise("Positive"))

    items = (sf_all_items()                                                      # [FIX 17 Oct-B]
             .select("student_id", "tracking_item_name", "create_date", "section", "survey", "item_ext_id",
                     clear_status.alias("clear_status_category"))
             .dropDuplicates(["student_id", "tracking_item_name", "create_date", "section", "survey", "item_ext_id"])
             .filter(F.col("clear_status_category") != "Excluded")                 # reportable items only
             .join(students, "student_id", "inner")
             .join(term_ranges, (F.to_date("create_date") >= F.col("term_start_date"))
                                & (F.to_date("create_date") <= F.col("term_end_date")), "inner")
             .join(rd(sf_gold("dim_tracking_item")).select(F.trim("tracking_item_name").alias("tracking_item_name"),
                                                           "item_type")
                       .dropDuplicates(["tracking_item_name"]),
                   "tracking_item_name", "left")
             .withColumn("_type", F.upper(F.col("item_type")))
             .withColumn("_rank", F.when(F.col("_type") == "KUDO", 0)                # kudos: no clear segment
                                   .when(F.col("clear_status_category") == "Positive", 3)
                                   .when(F.col("clear_status_category") == "Negative", 2)
                                   .when(F.col("clear_status_category") == "Active", 1)
                                   .otherwise(0)))

    def seg(rank_col):
        return (F.when(F.col(rank_col) == 3, "Positive").when(F.col(rank_col) == 2, "Negative")
                 .when(F.col(rank_col) == 1, "Active").otherwise("No Tracking Items"))

    def type_rank(t):
        return F.max(F.when(F.col("_type") == t, F.col("_rank")))

    track_term = (items.groupBy(*k).agg(
                        F.count("*").alias("tracking_item_count"),
                        F.max(F.when(F.col("_type") == "KUDO", 1).otherwise(0)).alias("has_kudo"),
                        F.max(F.when(F.col("_type") == "FLAG", 1).otherwise(0)).alias("has_flag"),
                        F.max(F.when(F.col("_type") == "REFERRAL", 1).otherwise(0)).alias("has_referral"),
                        F.max(F.when(F.col("_type") == "TO_DO", 1).otherwise(0)).alias("has_todo"),
                        F.max("_rank").alias("_all"),
                        type_rank("FLAG").alias("_flag"),
                        type_rank("REFERRAL").alias("_referral"),
                        type_rank("TO_DO").alias("_todo"))
                  .withColumn("has_tracking_item", F.lit(1))
                  .withColumn("tracking_segment", F.lit("With Tracking Items"))
                  .withColumn("clear_segment", seg("_all"))
                  .withColumn("flag_clear_segment", seg("_flag"))
                  .withColumn("referral_clear_segment", seg("_referral"))
                  .withColumn("todo_clear_segment", seg("_todo"))
                  .drop("_all", "_flag", "_referral", "_todo"))

    # =========================================================================
    # 3. MEETINGS  (Silver; dedup, attended and service area as in
    #    nb_fact_appointment)
    # =========================================================================
    appt_type = F.upper(F.col("appointment_type"))
    service_area = (F.when(appt_type.like("%ADVIS%"), "Advising")
                     .when(appt_type.like("%COACH%"), "Academic Coaching")
                     .when(appt_type.like("%TUTOR%"), "Tutoring")
                     .when(appt_type.like("%FINANCIAL%"), "Financial Aid")
                     .when(appt_type.like("%TEST%"), "Testing")
                     .otherwise("Other"))

    meetings = (rd(sf_silver("meeting_fulldetail"))
                .filter(F.col("appointment_ext_id").isNotNull())
                .withColumn("_rn", F.row_number().over(Window.partitionBy("appointment_ext_id")
                                                             .orderBy(F.col("date").desc())))
                .filter("_rn = 1")
                .filter(F.upper(F.col("attended_session").cast("string")).isin(YES))   # attended only
                .select(F.trim("student_id").alias("student_id"), "date", service_area.alias("service_area"))
                .join(students, "student_id", "inner")
                .join(term_ranges, (F.to_date("date") >= F.col("term_start_date"))
                                   & (F.to_date("date") <= F.col("term_end_date")), "inner"))

    def has_area(*areas):
        return F.max(F.when(F.col("service_area").isin(*areas), 1).otherwise(0))

    meet_term = (meetings.groupBy(*k).agg(
                        F.count("*").alias("attended_meeting_count"),
                        has_area("Advising").alias("has_advising_meeting"),
                        has_area("Academic Coaching").alias("has_coaching_meeting"),
                        has_area("Tutoring").alias("has_tutoring_meeting"),
                        has_area("Financial Aid").alias("has_financial_meeting"),
                        has_area("Other").alias("has_other_meeting"))
                 .withColumn("has_attended_meeting", F.lit(1)))

    # =========================================================================
    # 3b. WAVE 1 STUDENT-TERM SLICERS (same rules as the Wave 1 semantic model
    #     columns on fact_registration, moved here so all reports share them)
    # =========================================================================
    # major terms in date order (1, 2, 3 ...), academic year and term type
    major_info = (periods.filter(F.col("is_major_term") == 1)
                  .select(F.col("academic_period_key").alias("major_term_period_key"), "academic_year",
                          F.when(F.col("academic_period_name").contains("Summer"), "Summer")
                           .when(F.col("academic_period_name").contains("Fall"), "Fall")
                           .when(F.col("academic_period_name").contains("Spring"), "Spring")
                           .alias("_term_type"),
                          F.dense_rank().over(Window.orderBy("period_start_date")).alias("_term_idx")))
    # Fall PRD date of each academic year (age is taken on this date, as Wave 1)
    fall_prd = (periods.filter(F.col("academic_period_name").contains("Fall"))
                .groupBy("academic_year").agg(F.max("pell_recalculation_date").alias("_fall_prd_date")))

    # Enrollment type (Wave 1 rule): New = no earlier registered term;
    # Continuing = registered in one of the two previous terms; else Returning.
    # Terms are ordered by start date (the Wave 1 DAX sequence put Summer before Spring).
    reg_terms = events.select(*k).distinct().join(major_info.select("major_term_period_key", "_term_idx"),
                                                   "major_term_period_key", "inner")
    prev_idx = F.lag("_term_idx").over(Window.partitionBy("student_key").orderBy("_term_idx"))
    enroll_type = (reg_terms.withColumn("_prev_idx", prev_idx)
                   .withColumn("enrollment_type",
                               F.when(F.col("_prev_idx").isNull(), "New Student")
                                .when(F.col("_term_idx") - F.col("_prev_idx") <= 2, "Continuing Student")
                                .otherwise("Returning Student"))
                   .select(*k, "enrollment_type"))

    # Full / part time for the academic year (Wave 1 rule): credits of the AY
    # (not Unregistered / Waitlisted / Withdrawn) / required credits
    # (12 per Fall or Spring, 6 per Summer the student has a registration in).
    ev_ay = events.join(major_info.select("major_term_period_key", "academic_year", "_term_type"),
                        "major_term_period_key", "inner")
    ay_credits = (ev_ay.filter(~F.coalesce(F.col("registration_status"), F.lit(""))
                                 .isin("Unregistered", "Waitlisted", "Withdrawn"))
                  .groupBy("student_key", "academic_year")
                  .agg(F.sum(F.col("credits").cast("double")).alias("_ay_credits")))
    ay_required = (ev_ay.select("student_key", "academic_year", "_term_type").distinct()
                   .groupBy("student_key", "academic_year")
                   .agg(F.sum(F.when(F.col("_term_type") == "Summer", 6)
                               .when(F.col("_term_type").isin("Fall", "Spring"), 12)
                               .otherwise(0)).alias("_ay_required")))
    ay_load = (ay_required.join(ay_credits, ["student_key", "academic_year"], "left")
               .withColumn("_pct", F.col("_ay_credits") / F.col("_ay_required"))
               .withColumn("ft_pt_ay",
                           F.when(F.coalesce(F.col("_ay_required"), F.lit(0)) == 0, "Unknown")
                            .when(F.coalesce(F.col("_pct"), F.lit(0)) >= 1, "Full-Time")
                            .when(F.coalesce(F.col("_pct"), F.lit(0)) >= 0.5, "Half-Time")
                            .otherwise("Part-Time"))
               .select("student_key", "academic_year", "ft_pt_ay"))
    ft_pt = (reg_terms.select(*k)
             .join(major_info.select("major_term_period_key", "academic_year"), "major_term_period_key", "inner")
             .join(ay_load, ["student_key", "academic_year"], "left")
             .select(*k, "ft_pt_ay"))

    # Age on the Fall PRD date of the academic year (Wave 1 rule: year difference)
    dob = (rd(wd_gold("dim_students")).filter(F.col("is_current") == 1)
           .select("student_key", F.to_date("date_of_birth").alias("_dob"))
           .dropDuplicates(["student_key"]))
    term_fall_prd = (major_info.select("major_term_period_key", "academic_year")
                     .join(fall_prd, "academic_year", "left")
                     .select("major_term_period_key", "_fall_prd_date"))
    bands = [(18, "Under 18", 1), (20, "18–19", 2), (22, "20–21", 3), (25, "22–24", 4), (30, "25–29", 5),
             (35, "30–34", 6), (40, "35–39", 7), (50, "40–49", 8), (65, "50–64", 9)]

    def age_band(age, label):
        e = F.when(age.isNull(), F.lit(None) if label else F.lit(99))
        for upper, name, sort in bands:
            e = e.when(age < upper, F.lit(name if label else sort))
        return e.otherwise(F.lit("65 and over" if label else 10))

    # =========================================================================
    # 4. POPULATION = every student x major term with a registration, ANY
    #    tracking item or ANY meeting (incl. excluded items and meetings not
    #    attended), so every fact_tracking_item / fact_appointment row finds its
    #    student-term. The tracking / meeting flags above still count only
    #    reportable items and attended meetings.
    # =========================================================================
    pop_items = (sf_all_items()                                                  # [FIX 17 Oct-B]
                 .select("student_id", "create_date")
                 .join(students, "student_id", "inner")
                 .join(term_ranges, (F.to_date("create_date") >= F.col("term_start_date"))
                                    & (F.to_date("create_date") <= F.col("term_end_date")), "inner")
                 .select(*k))
    pop_meet = (rd(sf_silver("meeting_fulldetail"))
                .filter(F.col("appointment_ext_id").isNotNull())
                .withColumn("_rn", F.row_number().over(Window.partitionBy("appointment_ext_id")
                                                             .orderBy(F.col("date").desc())))
                .filter("_rn = 1")
                .select(F.trim("student_id").alias("student_id"), "date")
                .join(students, "student_id", "inner")
                .join(term_ranges, (F.to_date("date") >= F.col("term_start_date"))
                                   & (F.to_date("date") <= F.col("term_end_date")), "inner")
                .select(*k))
    pop = (reg_term.select(*k)
           .unionByName(pop_items)
           .unionByName(pop_meet)
           .distinct())

    # =========================================================================
    # 5. RETENTION  (next major term: Fall->Spring, Spring->Fall, Summer->Fall)
    # =========================================================================
    terms = major.withColumn("_is_summer", F.col("academic_period_name").contains("Summer"))
    nxt = (terms.alias("a")
           .join(terms.alias("b"),
                 (F.col("b.term_start_date") > F.col("a.term_start_date")) & (~F.col("b._is_summer")), "inner")
           .withColumn("_rn", F.row_number().over(Window.partitionBy("a.major_term_period_key")
                                                        .orderBy(F.col("b.term_start_date"))))
           .filter("_rn = 1")
           .select(F.col("a.major_term_period_key").alias("major_term_period_key"),
                   F.col("b.major_term_period_key").alias("next_major_period_key"),
                   F.col("a.term_start_date").alias("_term_start"),
                   F.col("b.term_census_date").alias("_next_census")))

    census_set = (reg_term.filter(F.col("is_prd_enrolled") == 1)
                  .select("student_key", F.col("major_term_period_key").alias("next_major_period_key"))
                  .withColumn("_retained", F.lit(1)))

    # completions: 'Complete' programs of study (as nb_fact_completion)
    completions = (rd(wd_silver("program_of_study_all"))
                   .filter((F.col("program_status") == "Complete") & F.col("earned_credential").isNotNull())
                   .select(F.trim("student_id").alias("student_id"), F.to_date("completion_date").alias("completion_date"))
                   .join(students, "student_id", "inner")
                   .select("student_key", "completion_date"))

    completed_term = (pop.join(nxt, "major_term_period_key", "inner")
                         .join(completions, "student_key", "inner")
                         .filter((F.col("completion_date") >= F.col("_term_start"))
                                 & (F.col("completion_date") <= F.col("_next_census")))
                         .select(*k).distinct()
                         .withColumn("_completed", F.lit(1)))

    graduated = completions.select("student_key").distinct().withColumn("_graduated", F.lit(1))

    pell = (rd(wd_gold("dim_student_term"))
            .join(period_to_major, "academic_period_key", "inner")
            .groupBy("student_key", "major_term_period_key")
            .agg(F.max(F.when(is_yes("pell_eligible"), 1).otherwise(0)).alias("pell_eligible")))
    # dim_student_term only lists Pell-eligible students. In a term that has Pell data,
    # a census-enrolled student with no row is "not Pell" (0); terms with no Pell data stay NULL.
    pell_terms = pell.select("major_term_period_key").distinct().withColumn("_pell_term", F.lit(1))

    # =========================================================================
    # 6. ASSEMBLE  (one row per student x major term)
    # =========================================================================
    df = (pop.join(reg_term, k, "left")
             .join(nxt, "major_term_period_key", "left")
             .join(census_set, ["student_key", "next_major_period_key"], "left")
             .join(completed_term, k, "left")
             .join(graduated, "student_key", "left")
             .join(pell, k, "left")
             .join(pell_terms, "major_term_period_key", "left")
             .withColumn("pell_eligible",
                         F.when(F.col("pell_eligible").isNotNull(), F.col("pell_eligible"))
                          .when((F.col("_pell_term") == 1) & (F.col("is_prd_enrolled") == 1), F.lit(0)))
             .join(track_term, k, "left")
             .join(meet_term, k, "left")
             .join(enroll_type, k, "left")
             .join(ft_pt, k, "left")
             .join(dob, "student_key", "left")
             .join(term_fall_prd, "major_term_period_key", "left")
             .withColumn("_age", F.year("_fall_prd_date") - F.year("_dob"))
             .withColumn("is_prd_enrolled", F.coalesce("is_prd_enrolled", F.lit(0)))
             .withColumn("measurement_established_flag",
                         F.when(F.col("_next_census") < F.lit(TODAY), 1).otherwise(0))
             .withColumn("_measured", (F.col("is_prd_enrolled") == 1) & (F.col("measurement_established_flag") == 1))
             .withColumn("retained_next_major_term_flag",
                         F.when(F.col("_measured"), F.coalesce("_retained", F.lit(0))))
             .withColumn("completed_by_next_major_term_flag",
                         F.when(F.col("_measured"), F.coalesce("_completed", F.lit(0))))
             .withColumn("retained_or_completed_flag",
                         F.when(F.col("_measured"), F.greatest("retained_next_major_term_flag",
                                                               "completed_by_next_major_term_flag")))
             .withColumn("retention_status",
                         F.when(F.col("is_prd_enrolled") == 0, "Not Enrolled at PRD")
                          .when(F.col("measurement_established_flag") == 0, "Not Yet Established")
                          .when(F.col("retained_next_major_term_flag") == 1, "Retained")
                          .when(F.col("completed_by_next_major_term_flag") == 1, "Completed")
                          .otherwise("Not Retained"))
             .withColumn("graduation_status",
                         F.when(F.col("_graduated") == 1, "Graduated").otherwise("Not Graduated"))
             .withColumn("_n_meet", F.coalesce("attended_meeting_count", F.lit(0))))

    final_df = df.select(
        F.sha2(F.concat_ws("||", F.col("student_key").cast("string"),
                           F.col("major_term_period_key").cast("string")), 256).alias("student_term_key"),
        "student_key",
        F.col("major_term_period_key").alias("academic_period_key"),
        "primary_program_key",
        "primary_college_key",
        F.col("is_prd_enrolled").cast("int").alias("is_prd_enrolled"),
        F.col("credits").cast("decimal(18,5)").alias("credits"),
        F.col("transfer_credits").cast("double").alias("transfer_credits"),
        "load_status",
        F.col("pell_eligible").cast("int").alias("pell_eligible"),
        F.col("enrollment_type").cast("string").alias("enrollment_type"),   # Wave 1 rule (see 3b)
        "next_major_period_key",
        F.col("measurement_established_flag").cast("int").alias("measurement_established_flag"),
        F.col("retained_next_major_term_flag").cast("int").alias("retained_next_major_term_flag"),
        F.col("completed_by_next_major_term_flag").cast("int").alias("completed_by_next_major_term_flag"),
        F.col("retained_or_completed_flag").cast("int").alias("retained_or_completed_flag"),
        "retention_status",
        F.coalesce("has_tracking_item", F.lit(0)).cast("int").alias("has_tracking_item"),
        F.coalesce("tracking_item_count", F.lit(0)).cast("bigint").alias("tracking_item_count"),
        F.coalesce("has_kudo", F.lit(0)).cast("int").alias("has_kudo"),
        F.coalesce("has_flag", F.lit(0)).cast("int").alias("has_flag"),
        F.coalesce("has_referral", F.lit(0)).cast("int").alias("has_referral"),
        F.coalesce("has_todo", F.lit(0)).cast("int").alias("has_todo"),
        F.coalesce("tracking_segment", F.lit("No Tracking Items")).alias("tracking_segment"),
        F.coalesce("clear_segment", F.lit("No Tracking Items")).alias("clear_segment"),
        F.coalesce("flag_clear_segment", F.lit("No Tracking Items")).alias("flag_clear_segment"),
        F.coalesce("referral_clear_segment", F.lit("No Tracking Items")).alias("referral_clear_segment"),
        F.coalesce("todo_clear_segment", F.lit("No Tracking Items")).alias("todo_clear_segment"),
        F.coalesce("has_attended_meeting", F.lit(0)).cast("int").alias("has_attended_meeting"),
        F.col("_n_meet").cast("bigint").alias("attended_meeting_count"),
        F.when(F.col("_n_meet") > 0, "With Meetings").otherwise("No Meetings").alias("meeting_segment"),
        F.when(F.col("_n_meet") == 0, "0").when(F.col("_n_meet") == 1, "1")
         .when(F.col("_n_meet") <= 3, "2-3").otherwise("4+").alias("meeting_frequency_bucket"),
        F.when(F.col("_n_meet") == 0, 0).when(F.col("_n_meet") == 1, 1)
         .when(F.col("_n_meet") <= 3, 2).otherwise(3).cast("int").alias("meeting_frequency_sort"),
        F.coalesce("has_advising_meeting", F.lit(0)).cast("int").alias("has_advising_meeting"),
        F.coalesce("has_coaching_meeting", F.lit(0)).cast("int").alias("has_coaching_meeting"),
        F.coalesce("has_tutoring_meeting", F.lit(0)).cast("int").alias("has_tutoring_meeting"),
        F.coalesce("has_financial_meeting", F.lit(0)).cast("int").alias("has_financial_meeting"),
        F.coalesce("has_other_meeting", F.lit(0)).cast("int").alias("has_other_meeting"),
        "graduation_status",
        # ---- new columns (appended; existing names unchanged) ----
        F.coalesce("csr_eligible_count", F.lit(0)).cast("int").alias("csr_eligible_count"),
        F.coalesce("csr_passed_count", F.lit(0)).cast("int").alias("csr_passed_count"),
        "campus_location_key",
        F.coalesce("is_dual_credit_flag", F.lit(0)).cast("int").alias("is_dual_credit_flag"),
        # ---- Wave 1 slicers (appended) ----
        F.col("ft_pt_ay").cast("string").alias("ft_pt_ay"),
        age_band(F.col("_age"), True).cast("string").alias("age_band"),
        age_band(F.col("_age"), False).cast("int").alias("age_band_sort"),
        F.when(F.col("pell_eligible") == 1, "Yes").when(F.col("pell_eligible") == 0, "No")
         .otherwise("Not Available").alias("pell_status"),
    )

    fact_audit_df = add_audit_column(final_df, PIPELINE_NAME).cache()

    # ---- grain guard: one row per student x major term ----------------------
    final_count = fact_audit_df.count()
    n_keys = fact_audit_df.select("student_term_key").distinct().count()
    assert final_count == n_keys, f"grain broken: {final_count - n_keys} duplicate student_term_key rows"
    rows_read = rows_written = final_count

    fact_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name,
                                        "fact_student_term_persistence")
    # full rebuild every run -> overwrite (no merge, so no duplicate build-up)
    (fact_audit_df.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
                  .save(fact_gold_path))
    fact_audit_df.unpersist()
    print(f"fact_student_term_persistence rows: {final_count}")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME), "failed", _safe_str(LEVEL),
        _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME), rerun_count=0)
    raise Exception(f"Failed to create/load fact_student_term_persistence table: {e}")


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
