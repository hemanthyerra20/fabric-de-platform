# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fd975be1-c831-4267-b419-318d73f5b16b",
# META       "default_lakehouse_name": "lh_cs_gold",
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

# CELL ********************

# nb_fact_learning_outcome_canvas_result
# Version markers: [FIX 16 Oct] [FIX 16 Oct-b] [FIX 17 Oct-A] [FIX 17 Oct-B]
from pyspark.sql import functions as F
from pyspark.sql.window import Window

JOB_NAME = notebookutils.runtime.context["currentNotebookName"]

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
silver_schema_name = "canvas"
gold_schema_name = "canvas"
workday_gold_schema_name = "workday_cs"
bronze_schema_name = "canvas"
silver_lakehouse_name = "lh_cs_silver"
gold_lakehouse_name = "lh_cs_gold"
bronze_lakehouse_name = "lh_cs_bronze"

notebook_name = notebookutils.runtime.context["currentNotebookName"]
config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)

workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, silver_lakehouse_name)
gold_lakehouse_id = get_lakehouse_id(var_cs_lib_name, gold_lakehouse_name)
bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name, bronze_lakehouse_name)

def bronze_path(table: str) -> str:
    return abfss_path_builder(
        workspace_id,
        bronze_lakehouse_id,
        bronze_schema_name,
        f"canv_{table}",
    )

def silver_path(table: str) -> str:
    return abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        table,
    )

def gold_path(table: str) -> str:
    return abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        table,
    )

def workday_gold_path(table: str) -> str:
    return abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        workday_gold_schema_name,
        table,
    )

def bronze(table: str):
    df = spark.read.format("delta").load(bronze_path(table))
    w = Window.partitionBy("key_id").orderBy(F.col("ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    if "is_deleted" in df.columns:
        df = df.filter(~F.col("is_deleted"))
    return df

def dk(col):
    """Convert timestamp to yyyymmdd int, -1 for NULL."""
    return (
        F.when(F.col(col).isNotNull(), F.date_format(F.col(col), "yyyyMMdd").cast("int"))
        .otherwise(F.lit(-1))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Common xref / xwalk / program-period stamping inputs (from Silver)
xref = (
    spark.read.format("delta").load(silver_path("slv_canvas_student_xref"))
    .select("canvas_user_id", "student_key", "is_student_matched")
)

# Course/section crosswalk (one row per Canvas section)
xwalk = (
    spark.read.format("delta").load(silver_path("slv_canvas_course_xwalk"))
    .select(
        "canvas_course_id",
        "canvas_section_id",
        "course_section_key",
        "match_method",
    )
)

# Canvas enrollments (for student-specific section mapping)   [FIX A: + type]
enroll = (
    bronze("enrollments")
    .select("user_id", "course_id", "course_section_id", "type", "workflow_state")
)

pstamp = spark.read.format("delta").load(silver_path("slv_canvas_program_stamp"))
pm = spark.read.format("delta").load(silver_path("slv_canvas_period_map"))

# Term from the old-format Canvas SIS ID (built in nb_student_xref, Fix 2)
sis_term = (
    spark.read.format("delta").load(silver_path("slv_canvas_section_term"))
    .select("canvas_section_id", F.col("academic_period_key").alias("_sis_period_key"))
)

# Term of each Workday section, from the registrations on that section
sec_period = (
    spark.read.format("delta").load(workday_gold_path("fact_registration"))
    .groupBy("course_section_key")
    .agg(F.min("academic_period_key").alias("_sec_period_key"))
)

# [DE-09] Major term for each period (sessions roll up to their full term)
ap_all = spark.read.format("delta").load(workday_gold_path("dim_academic_period"))
major_term = (
    ap_all.select("academic_period_key", "academic_period_name")
    .join(
        ap_all.filter(F.col("is_major_term") == 1)
              .select("academic_period_name", F.col("academic_period_key").alias("major_term_period_key")),
        "academic_period_name", "left")
    .select("academic_period_key", "major_term_period_key")
)

# [DE-09] Final grade + campus from the student's registration in that section
reg_gc = spark.read.format("delta").load(workday_gold_path("fact_registration")).select(
    "student_key", "course_section_key", "grade_key", "campus_location_key", "registered_date"
)
w_reg = Window.partitionBy("student_key", "course_section_key").orderBy(
    F.col("grade_key").isNull().asc(), F.col("registered_date").desc_nulls_last()
)
reg_gc = (reg_gc.withColumn("_rn", F.row_number().over(w_reg)).filter("_rn = 1")
          .select("student_key", "course_section_key",
                  F.col("grade_key").alias("final_grade_key"), "campus_location_key"))


# -----------------------------------------------------------------------------
# [FIX 16 Oct-b] Program on the student's own registration in each Workday section
# (Silver course_registration), version valid on the term's census date (PRD, else
# period start). Main source of primary_program_key; slv_canvas_program_stamp is the
# fallback only (it gave "Undeclared" for many enrolled students, RPT-037 2024 Fall).
# One row per student x section: Registered first, then latest program version,
# then registration_key. Waitlisted and blank programs ignored.
# -----------------------------------------------------------------------------
workday_silver_path = lambda t: abfss_path_builder(workspace_id, silver_lakehouse_id, workday_gold_schema_name, t)

_rp_students = (spark.read.format("delta").load(workday_gold_path("dim_students"))
                .filter(F.col("is_current") == 1)
                .select(F.trim("student_id").alias("student_id"), F.col("student_key").cast("string").alias("student_key")))
_rp_sections = (spark.read.format("delta").load(workday_silver_path("course_sections"))
                .select("course_section_id", F.col("course_section_key").cast("string").alias("course_section_key"))
                .dropDuplicates(["course_section_id"]))
_rp_dates = (spark.read.format("delta").load(workday_gold_path("dim_academic_period"))
             .select(F.col("period_name").alias("academic_period"),
                     F.to_date(F.coalesce("pell_recalculation_date", "period_start_date")).alias("_prog_dt"))
             .dropDuplicates(["academic_period"]))
_rp_programs = (spark.read.format("delta").load(workday_gold_path("dim_program"))
                .select(F.trim("program_name").alias("program_name"),
                        F.col("program_key").cast("string").alias("_reg_program_key"),
                        F.to_date("effective_date").alias("_eff"), F.to_date("expiration_date").alias("_exp")))
reg_prog = (
    spark.read.format("delta").load(workday_silver_path("course_registration"))
    .filter(F.coalesce(F.col("registration_status"), F.lit("")) != "Waitlisted")
    .filter(F.trim(F.coalesce(F.col("primary_program_of_study"), F.lit(""))) != "")
    .select("registration_key", F.trim("student_id").alias("student_id"), "course_section_id", "academic_period",
            "registration_status", F.trim("primary_program_of_study").alias("program_name"))
    .join(_rp_students, "student_id", "inner")
    .join(_rp_sections, "course_section_id", "inner")
    .join(_rp_dates, "academic_period", "inner")
    .join(_rp_programs, "program_name", "inner")
    .filter((F.col("_prog_dt") >= F.col("_eff")) & (F.col("_prog_dt") <= F.col("_exp")))
)
w_rp = Window.partitionBy("student_key", "course_section_key").orderBy(
    (F.col("registration_status") == "Registered").cast("int").desc(),
    F.col("_exp").desc_nulls_last(), F.col("_eff").desc_nulls_last(), F.col("registration_key").desc())
reg_prog = (reg_prog.withColumn("_rn", F.row_number().over(w_rp)).filter("_rn = 1")
                    .select("student_key", "course_section_key", "_reg_program_key"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SCALE_TOLERANCE = 0.001

try:

    res = bronze("learning_outcome_results").select(
        F.col("key_id").cast("bigint").alias("lor_bk"),
        F.col("user_id").cast("bigint").alias("canvas_user_id"),
        F.col("user_uuid"),
        F.col("learning_outcome_id").cast("bigint").alias("outcome_key_src"),
        F.col("content_tag_id").cast("bigint").alias("alignment_key_src"),
        F.col("context_type"),
        F.col("context_id").cast("bigint").alias("context_id"),
        F.col("association_type"),
        F.col("association_id").cast("bigint").alias("association_id"),
        F.col("artifact_type"),
        F.col("artifact_id").cast("bigint").alias("artifact_id"),
        F.col("attempt").cast("int").alias("attempt"),
        F.col("score").cast("double").alias("outcome_score"),
        F.col("possible").cast("double").alias("outcome_possible"),
        F.col("percent").cast("double").alias("mastery_percent"),
        F.col("mastery").cast("boolean").alias("is_mastery"),
        F.col("original_score").cast("double").alias("original_score"),
        F.col("original_possible").cast("double").alias("original_possible"),
        F.col("original_mastery").cast("boolean").alias("original_mastery"),
        F.col("hidden").cast("boolean").alias("is_hidden"),
        F.col("hide_points").cast("boolean").alias("hide_points"),
        F.col("assessed_at").cast("timestamp").alias("assessed_ts"),
        F.col("submitted_at").cast("timestamp").alias("submitted_ts"),
        F.col("workflow_state"),
    )

    # Course context
    res = res.withColumn(
        "canvas_course_id",
        F.when(F.col("context_type") == "Course", F.col("context_id")),
    )

    # Date anchor (fallback only): assessed_at, then submitted_at
    res = res.withColumn("anchor_ts", F.coalesce(F.col("assessed_ts"), F.col("submitted_ts")))

    # ------------------------------------------------------------------
    # student_key from xref
    # ------------------------------------------------------------------
    res = (
        res.join(xref, "canvas_user_id", "left")
        .withColumn("student_key", F.coalesce(F.col("student_key").cast("string"), F.lit("-1")))
        .withColumn("is_student_matched", F.coalesce(F.col("is_student_matched"), F.lit(False)))
    )

    # ------------------------------------------------------------------
    # [FIX B1 / DE-06] Section = the student's own Canvas section in that
    # course, mapped to Workday through the xwalk (was: xwalk joined on the
    # course, which gave an arbitrary section when a course had several)
    # ------------------------------------------------------------------
    enr1 = (
        enroll
        .filter((F.col("type") == "StudentEnrollment")
                & (F.coalesce(F.col("workflow_state"), F.lit("")) != "deleted"))
        .select(
            F.col("user_id").cast("bigint").alias("canvas_user_id"),
            F.col("course_id").cast("bigint").alias("canvas_course_id"),
            F.col("course_section_id").cast("bigint").alias("canvas_section_id"),
        )
        .join(xwalk.select("canvas_section_id", "course_section_key"), "canvas_section_id", "left")
    )
    # one section per student per course; prefer one that maps to Workday
    w_e = Window.partitionBy("canvas_user_id", "canvas_course_id").orderBy(
        F.col("course_section_key").isNull().asc(), F.col("canvas_section_id").asc()
    )
    enr1 = enr1.withColumn("_rn", F.row_number().over(w_e)).filter("_rn = 1").drop("_rn")

    res = (
        res.join(enr1, ["canvas_user_id", "canvas_course_id"], "left")
        .withColumn("course_section_key",
                    F.coalesce(F.col("course_section_key").cast("string"), F.lit("-1")))
    )

    # ------------------------------------------------------------------
    # [FIX B2 / DE-08 + Fix 2] Term, in priority order:
    #   1) the matched Workday section's term
    #   2) the term in the old-format Canvas SIS ID (slv_canvas_section_term)
    #   3) only then the date the score was given (period map)
    # ------------------------------------------------------------------
    pm_s = pm.select("academic_period_key", "period_start_date", "period_end_date_derived")
    j = res.join(
        pm_s,
        (F.to_date(F.col("anchor_ts")) >= pm_s.period_start_date)
        & (F.to_date(F.col("anchor_ts")) <= pm_s.period_end_date_derived),
        "left",
    )
    # [FIX 16 Oct] one period per result, keyed on lor_bk (was a random row id)
    w = Window.partitionBy("lor_bk").orderBy(F.col("period_start_date").desc())
    res = (
        j.withColumn("_prn", F.row_number().over(w)).filter("_prn = 1")
        .withColumnRenamed("academic_period_key", "_date_period_key")
        .drop("_prn", "period_start_date", "period_end_date_derived")
        .join(sec_period, "course_section_key", "left")
        .join(sis_term, "canvas_section_id", "left")
        .withColumn("academic_period_key",
                    F.coalesce("_sec_period_key", "_sis_period_key", "_date_period_key",
                               F.lit("-1")).cast("string"))
        .withColumn("period_source",
                    F.when(F.col("_sec_period_key").isNotNull(), "workday_section")
                     .when(F.col("_sis_period_key").isNotNull(), "canvas_sis_term")
                     .when(F.col("_date_period_key").isNotNull(), "assessed_date")
                     .otherwise("none"))
        .drop("_sec_period_key", "_sis_period_key", "_date_period_key")
    )

    # ------------------------------------------------------------------
    # [FIX 16 Oct-b] primary_program_key = program on the student's registration
    # in that section (reg_prog, cell above); program stamp only as fallback
    # ------------------------------------------------------------------
    res = (
        res.join(reg_prog, ["student_key", "course_section_key"], "left")
        .join(
            pstamp.select("student_key", "academic_period_key", "primary_program_key"),
            ["student_key", "academic_period_key"],
            "left",
        )
        .withColumn("primary_program_key",
                    F.coalesce(F.col("_reg_program_key").cast("string"),
                               F.col("primary_program_key").cast("string"), F.lit("-1")))
        .drop("_reg_program_key")
    )


    # ------------------------------------------------------------------
    # [DE-09] Keys that link the fact to persistence, grade and campus
    #   student_term_key MUST use the same expression as
    #   starfish.fact_student_term_persistence (check with V12).
    # ------------------------------------------------------------------
    res = (
        res.join(major_term, "academic_period_key", "left")
        .withColumn("student_term_key",
           F.when(F.col("major_term_period_key").isNotNull(),
                  F.sha2(F.concat_ws("||",
                                     F.col("student_key").cast("string"),
                                     F.col("major_term_period_key").cast("string")), 256)))
        .join(reg_gc, ["student_key", "course_section_key"], "left")
        .withColumn("final_grade_key", F.coalesce(F.col("final_grade_key").cast("string"), F.lit("-1")))
        .withColumn("campus_location_key", F.coalesce(F.col("campus_location_key").cast("string"), F.lit("-1")))
    )

    # ------------------------------------------------------------------
    # [FIX C / DE-07] Unmatched students go to a reject table, not the fact
    # ------------------------------------------------------------------
    rej = (
        res.filter(F.col("student_key") == "-1")
        .select("lor_bk", "canvas_user_id", "outcome_key_src", "canvas_course_id",
                F.lit("no_workday_student_match").alias("reject_reason"),
                F.current_timestamp().alias("rejected_on"))
    )
    (rej.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
        .save(gold_path("rej_learning_outcome_result")))

    res = res.filter(F.col("student_key") != "-1")

    # ----------------------------------------------------------------------
    # BLOCK 1  -  RPT-037  is_primary_program_outcome  (flag)
    #
    # RDD RPT-037 Students Assessed: "outcome program = student's primary program".
    # Compared on the 6-digit WTCS program number, not on program_key, because
    # dim_outcome resolves the PLO to the CURRENT dim_program row while the
    # student's primary_program_key comes from the PRD snapshot and can be an
    # older version of the same program.
    # -----------------------------------------------------------------------------
    stu_prog = (
        spark.read.format("delta").load(workday_gold_path("dim_program"))
        .select(
            F.col("program_key").cast("string").alias("primary_program_key"),
            F.regexp_replace(F.col("program_code").cast("string"), r"[^0-9]", "").alias("_stu_prog_digits"),
        )
        .dropDuplicates(["primary_program_key"])
    )

    plo = (
        spark.read.format("delta").load(gold_path("dim_outcome"))
        .filter(F.col("outcome_level") == "PLO")
        .select(
            F.col("outcome_key").alias("outcome_key_src"),
            F.col("program_number").alias("_plo_prog_digits"),
        )
        .dropDuplicates(["outcome_key_src"])
    )

    res = (
        res.join(stu_prog, "primary_program_key", "left")
           .join(plo, "outcome_key_src", "left")
           .withColumn(
               "is_primary_program_outcome",
               F.coalesce(
                   F.col("_plo_prog_digits").isNotNull()
                   & (F.col("_plo_prog_digits") == F.col("_stu_prog_digits")),
                   F.lit(False),
               ),
           )
           .drop("_stu_prog_digits", "_plo_prog_digits")
    )

    # ----------------------------------------------------------------------
    # BLOCK 2  -  RPT-039  section_tracking_status_key  (key)
    #
    # Matt (MP159): limit Starfish items to those whose course context matches the
    # course AND section the CLO was assessed in.
    # Status per student x section, from reportable, non-kudo items only:
    #   3 Positive  - any item cleared positively (Matt: "if a student has any items
    #                 cleared with a positive reason, move them into the positive group")
    #   2 Negative  - else any Negative / Enrollment Ended   (Negative over Active is
    #   1 Active    - else any item still open                our default - NWTC to confirm)
    #   0 No Tracking Items
    # Irrelevant / Mistake are excluded.
    #
    # Reads Starfish SILVER (not Gold fact_tracking_item). Item dedup, section
    # pick and clear status are the same as nb_fact_tracking_item - keep in step.
    # [FIX 17 Oct-A] Workday-era section IDs matched on course_section_code.
    # [FIX 17 Oct-B] + manual / system items (tracking_fulldetail), as nb_fact_tracking_item.
    # -----------------------------------------------------------------------------
    sf_silver = lambda t: abfss_path_builder(workspace_id, silver_lakehouse_id, "starfish", t)
    sf_gold = lambda t: abfss_path_builder(workspace_id, gold_lakehouse_id, "starfish", t)
    SF_NK = ["student_id", "tracking_item_name", "create_date", "section", "survey", "item_ext_id"]   # [FIX 17 Oct-B]

    def sf_join_key(col, second_part):
        return F.trim(F.concat_ws("_", F.split(F.col(col), "_").getItem(0),
                                  F.split(F.col(col), "_").getItem(second_part)))

    # [FIX 17 Oct-A] same rule as the Starfish fact notebooks (sf_section_key / build_sections)
    SF_WD_FORMAT_RX = r"^[^_]+_20[0-9]{2}_"

    def sf_section_key(col):
        c = F.trim(F.col(col).cast("string"))
        return (F.when(c.isNull() | (c == ""), F.lit(None).cast("string"))
                 .when(c.rlike(SF_WD_FORMAT_RX), c)
                 .otherwise(sf_join_key(col, 2)))
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

    _sf_cs = (
        spark.read.format("delta").load(workday_gold_path("dim_course_sections"))
        .select(F.col("course_section_key").cast("string").alias("course_section_key"), "course_section_id",
                "course_code", "academic_period",
                F.to_date("section_start_date").alias("_sec_start"),
                F.to_date("section_end_date").alias("_sec_end"))
        .dropDuplicates(["course_section_key"])
    )
    _sf_listing = (spark.read.format("delta").load(workday_silver_path("section_listing_data"))
                   .select(F.col("course_section_key").cast("string").alias("course_section_key"), "section_number"))
    # [FIX 17 Oct-A] both lookup keys: existing course + class number, and course_section_code
    sf_sections = (
        _sf_cs.select("course_section_key", "_sec_start", "_sec_end",
                      sf_join_key("course_section_id", 1).alias("course_section_join_key"))
        .unionByName(
            _sf_cs.join(_sf_listing, "course_section_key", "inner")
                  .select("course_section_key", "_sec_start", "_sec_end",
                          F.concat_ws("_", F.trim(F.col("course_code")),
                                      F.trim(F.regexp_replace(F.trim(F.col("academic_period")), " ", "_")),
                                      F.trim(F.col("section_number").cast("string"))).alias("course_section_join_key")))
        .filter(F.col("course_section_join_key").isNotNull())
        .dropDuplicates(["course_section_join_key", "course_section_key"])
    )
    sf_students = (
        spark.read.format("delta").load(workday_gold_path("dim_students"))
        .filter(F.col("is_current") == 1)
        .select(F.trim("student_id").alias("student_id"), F.col("student_key").cast("string").alias("student_key"))
    )
    ti_type = (
        spark.read.format("delta").load(sf_gold("dim_tracking_item"))
        .select(F.trim("tracking_item_name").alias("tracking_item_name"), "item_type")
        .dropDuplicates(["tracking_item_name"])
    )

    _sf_cat = F.col("tracking_item_closure_reason_category")
    sf_clear = (F.when(_sf_cat.isNull() | (F.trim(_sf_cat) == ""), "Active")
                 .when(F.upper(_sf_cat).isin("NEGATIVE", "ENROLLMENT ENDED"), "Negative")
                 .when(F.upper(_sf_cat) == "UNKNOWN/OTHER", "Positive")
                 .when(F.upper(_sf_cat).isin("IRRELEVANT", "MISTAKE"), "Excluded")
                 .otherwise("Positive"))

    w_sf_dup = Window.partitionBy(*SF_NK).orderBy(
        F.upper(F.coalesce(F.col("status"), F.lit(""))).isin("T").cast("int").desc(),
        (F.trim(F.coalesce(_sf_cat, F.lit(""))) != "").cast("int").desc(),
        F.col("survey_date").desc_nulls_last(),
        F.col("instructor").asc_nulls_last())
    _sf_d = F.to_date(F.col("create_date"))
    w_sf_sec = Window.partitionBy(*SF_NK).orderBy(
        F.coalesce((_sf_d >= F.col("_sec_start")) & (_sf_d <= F.col("_sec_end")), F.lit(False)).cast("int").desc(),
        F.abs(F.datediff(_sf_d, F.col("_sec_start"))).asc_nulls_last(),
        F.col("course_section_key").asc_nulls_last())

    _sf_survey = (
        spark.read.format("delta").load(sf_silver("track_items_by_surveys"))
        .filter(F.col("tracking_item_name").isNotNull())
        .withColumn("student_id", F.trim("student_id"))
        .withColumn("tracking_item_name", F.trim("tracking_item_name"))
        .withColumn("item_ext_id", F.lit(None).cast("string"))                      # [FIX 17 Oct-B]
        .withColumn("_rn", F.row_number().over(w_sf_dup)).filter("_rn = 1").drop("_rn")
    )
    sec_status = (
        _sf_survey.unionByName(sf_extra_items(_sf_survey))                         # [FIX 17 Oct-B]
        .withColumn("course_section_join_key", sf_section_key("section"))           # [FIX 17 Oct-A]
        .join(sf_sections, "course_section_join_key", "left")
        .withColumn("_rn", F.row_number().over(w_sf_sec)).filter("_rn = 1").drop("_rn")
        .filter(F.col("course_section_key").isNotNull())
        .withColumn("_clear", sf_clear)
        .filter(F.col("_clear") != "Excluded")
        .join(sf_students, "student_id", "inner")
        .join(ti_type, "tracking_item_name", "left")
        .filter(~F.upper(F.coalesce(F.col("item_type"), F.lit(""))).startswith("KUDO"))
        .withColumn("_rank", F.when(F.col("_clear") == "Positive", 3)
                              .when(F.col("_clear") == "Negative", 2)
                              .when(F.col("_clear") == "Active", 1)
                              .otherwise(0))
        .groupBy("student_key", "course_section_key")
        .agg(F.max("_rank").alias("_sec_status"))
    )

    res = (
        res.join(sec_status, ["student_key", "course_section_key"], "left")
           .withColumn("section_tracking_status_key",
                       F.coalesce(F.col("_sec_status"), F.lit(0)).cast("int"))
           .drop("_sec_status")
    )


    # ---- rating_key logic (scale + rating bands) - unchanged ----------------
    scale = (
        spark.read.format("delta").load(silver_path("slv_canvas_outcome_scale"))
        .select(
            F.col("outcome_id").alias("outcome_key_src"),
            "scale_key",
            "scale_source",
            "scale_inheritance",
        )
    )

    bounds = (
        spark.read.format("delta").load(silver_path("slv_canvas_outcome_rating"))
        .groupBy("scale_key")
        .agg(F.max("rating_points").alias("scale_max"))
    )

    rating = (
        spark.read.format("delta").load(silver_path("slv_canvas_outcome_rating"))
        .select("scale_key", "outcome_rating_key", "rating_points", "rating_sequence")
    )

    res = (
        res.join(scale, "outcome_key_src", "left")
        .join(bounds, "scale_key", "left")
        .withColumn(
            "score_on_scale",
            F.when(F.col("outcome_score").isNull() | F.col("scale_max").isNull(), None)
            .when(
                F.col("outcome_possible").isNotNull()
                & (F.col("outcome_possible") > 0)
                & (F.abs(F.col("outcome_possible") - F.col("scale_max")) > SCALE_TOLERANCE),
                F.coalesce(
                    F.col("mastery_percent"),
                    F.col("outcome_score") / F.col("outcome_possible"),
                )
                * F.col("scale_max"),
            )
            .otherwise(F.col("outcome_score")),
        )
    )

    # [FIX 16 Oct] Match each result to its rating band on lor_bk (unique, asserted below).
    # Was a monotonically_increasing_id row id: Spark can number the rows differently in
    # the two branches of this self-join, so some results got ANOTHER result's rating
    # (seen as 94 ILO results carrying other outcomes' "Exceeds/Near Mastery" labels).
    band = (
        res.select("lor_bk", "scale_key", "score_on_scale")
        .join(rating, "scale_key")
        .filter(F.col("rating_points") <= F.col("score_on_scale"))
    )

    w_b = Window.partitionBy("lor_bk").orderBy(
        F.col("rating_points").desc(), F.col("rating_sequence").asc()
    )

    band1 = (
        band.withColumn("_bn", F.row_number().over(w_b))
        .filter("_bn = 1")
        .select("lor_bk", F.col("outcome_rating_key").alias("_band_key"))
    )

    res = (
        res.join(band1, "lor_bk", "left")
        .withColumn(
            "rating_key",
            F.when(F.col("score_on_scale").isNull(), F.lit(-1)).otherwise(
                F.coalesce(F.col("_band_key"), F.lit(-2))
            ),
        )
        .drop("_band_key", "scale_max")
    )

    # ---- latest-score flags ------------------------------------------------
    # academic_year of the (corrected) term
    pm_df = (
        pm.select("academic_period_key", "academic_year").dropDuplicates()
        .withColumnRenamed("academic_year", "pm_academic_year")
    )
    res = res.join(pm_df, "academic_period_key", "left")

    # [FIX D / DE-05] Valid row test that can never be NULL.
    # Was: student_key (text hash) != -1 (number) -> NULL -> every flag NULL.
    valid = (
        (~F.coalesce(F.col("is_hidden"), F.lit(False)))
        & (F.coalesce(F.col("workflow_state"), F.lit("")) != "deleted")
        & F.col("outcome_score").isNotNull()
        & (F.col("student_key") != F.lit("-1"))
    )
    res = res.withColumn("_valid", F.coalesce(valid, F.lit(False)))

    w_ay = Window.partitionBy(
        "_valid", "student_key", "outcome_key_src", "pm_academic_year"
    ).orderBy(F.col("anchor_ts").desc(), F.col("lor_bk").desc())

    w_st = Window.partitionBy(
        "_valid", "student_key", "outcome_key_src", "course_section_key", "academic_period_key"
    ).orderBy(F.col("anchor_ts").desc(), F.col("lor_bk").desc())

    res = (
        res.withColumn("is_latest_in_ay",
                       F.when(F.col("_valid") & (F.row_number().over(w_ay) == 1), True).otherwise(False))
           .withColumn("is_latest_in_section_term",
                       F.when(F.col("_valid") & (F.row_number().over(w_st) == 1), True).otherwise(False))
           .drop("_valid")
    )

    # Guard: stop the run if the flags come out empty or NULL again
    chk = res.agg(
        F.sum(F.col("is_latest_in_ay").cast("int")).alias("n_latest_ay"),
        F.sum(F.col("is_latest_in_section_term").cast("int")).alias("n_latest_st"),
        F.sum((F.col("is_latest_in_ay").isNull() | F.col("is_latest_in_section_term").isNull())
              .cast("int")).alias("n_null"),
    ).first()
    assert chk.n_null == 0 and chk.n_latest_ay > 0 and chk.n_latest_st > 0, \
        f"latest flags broken: {chk}"

    # ---- final shape ---------------------------------------------------------
    fact = res.select(
        "lor_bk",
        "student_key",
        F.coalesce(F.col("outcome_key_src"), F.lit(-1)).alias("outcome_key"),
        F.coalesce(F.col("alignment_key_src"), F.lit(-1)).alias("alignment_key"),
        "rating_key",
        "course_section_key",
        "canvas_course_id",
        "academic_period_key",
        "major_term_period_key",
        "student_term_key",
        "period_source",
        "final_grade_key",
        "campus_location_key",
        "primary_program_key",
        F.when(F.col("association_type") == "Assignment", F.col("association_id")).otherwise(
            F.lit(-1)
        ).alias("assignment_key"),
        "artifact_id",
        "artifact_type",
        dk("assessed_ts").alias("assessed_date_key"),
        dk("submitted_ts").alias("submitted_date_key"),
        "assessed_ts",
        "attempt",
        "outcome_score",
        "outcome_possible",
        "mastery_percent",
        "is_mastery",
        "original_score",
        "original_possible",
        "original_mastery",
        "is_hidden",
        "hide_points",
        "score_on_scale",
        "scale_source",
        "scale_inheritance",
        "is_latest_in_ay",
        "is_latest_in_section_term",
        "is_primary_program_outcome",      # RPT-037
        "section_tracking_status_key",     # RPT-039
        "workflow_state",
    )

    # Audit columns
    fact = (
        fact.withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit(notebook_name))
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit(notebook_name))
    )

    # Hard check: one row per Canvas result
    n_rows, n_keys = fact.count(), fact.select("lor_bk").distinct().count()
    assert n_rows == n_keys, f"duplicate lor_bk: {n_rows - n_keys}"

    # Full rebuild every run -> overwrite, so removed rows (rejects, changed
    # sections) do not linger in Gold as they would with a merge/upsert.
    (
        fact.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
        .save(gold_path("fact_learning_outcome_result"))
    )

    print("fact_learning_outcome_result build completed successfully")

except Exception as e:
    print("Error while building fact_learning_outcome_result:", str(e))
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------------------------------------------------
# BLOCK 4  -  new cell: 4-row lookup for the RPT-039 Starfish visual
#             (static; overwrite every run is harmless)
#   Model: fact_learning_outcome_result[section_tracking_status_key]
#          -> dim_section_tracking_status[section_tracking_status_key]
#          many-to-one, single direction. Sort status_label by sort_order.
# -----------------------------------------------------------------------------
status_dim = spark.createDataFrame(
    [(0, "No Tracking Items", 4),
     (1, "Active",            1),
     (2, "Negative",          3),
     (3, "Positive",          2)],
    "section_tracking_status_key int, status_label string, sort_order int",
)
(status_dim.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
    .save(gold_path("dim_section_tracking_status")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
