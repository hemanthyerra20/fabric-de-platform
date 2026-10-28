# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7e2b017e-2bbe-44d3-9849-ca440a90d7a7",
# META       "default_lakehouse_name": "lh_cs_silver",
# META       "default_lakehouse_workspace_id": "bd8e5690-a001-4896-8ed1-ae46d585f7b6",
# META       "known_lakehouses": [
# META         {
# META           "id": "7e2b017e-2bbe-44d3-9849-ca440a90d7a7"
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
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

JOB_NAME = "ingest_silver_layer"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
silver_schema_name = 'canvas'
gold_schema_name = 'canvas'
workday_gold_schema_name='workday_cs'
bronze_schema_name = 'canvas'
silver_lakehouse_name = 'lh_cs_silver'
gold_lakehouse_name = 'lh_cs_gold'
bronze_lakehouse_name = 'lh_cs_bronze'
notebook_name = notebookutils.runtime.context["currentNotebookName"]
config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
#config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, silver_lakehouse_name)
gold_lakehouse_id = get_lakehouse_id(var_cs_lib_name, gold_lakehouse_name)
bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name, bronze_lakehouse_name)
silver_abfss_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "canv_assignments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Helper paths for Bronze, Silver (source) and Gold (target) Canvas tables

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
    """Latest non-deleted Bronze snapshot for a given Canvas table."""
    df = spark.read.format("delta").load(bronze_path(table))
    w = Window.partitionBy("key_id").orderBy(F.col("ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    if "is_deleted" in df.columns:
        df = df.filter(~F.col("is_deleted"))
    return df


def with_unknown(df, key_col: str, defaults: dict):
    """Append the -1 Unknown member with sensible defaults per column."""
    row = {c: defaults.get(c) for c in df.columns}
    row[key_col] = -1
    unk = spark.createDataFrame([row], schema=df.schema)
    return df.unionByName(unk)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Helper paths for Bronze, Silver (source) and Gold (target) Canvas tables

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


def workday_silver_path(table: str) -> str:
    return abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        workday_gold_schema_name,
        table,
    )



def bronze(table: str):
    """Latest non-deleted Bronze snapshot for a given Canvas table."""
    df = spark.read.format("delta").load(bronze_path(table))
    w = Window.partitionBy("key_id").orderBy(F.col("ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    if "is_deleted" in df.columns:
        df = df.filter(~F.col("is_deleted"))
    return df


def with_unknown(df, key_col: str, defaults: dict):
    """Append the -1 Unknown member with sensible defaults per column."""
    row = {c: defaults.get(c) for c in df.columns}
    row[key_col] = -1
    unk = spark.createDataFrame([row], schema=df.schema)
    return df.unionByName(unk)



# ---------------------------------------------------------------- CONFIG
# TODO/CONFIRM (Uday): the Workday Student ID column on dim_students.
WD_STUDENT_ID_COL = "student_id"

# TODO/CONFIRM (Uday, CF-07): business-key column on dim_course_sections.
WD_SECTION_BK_COL = "course_section_key"

# O1 (OPEN): enrolment window X for the Workday-driven population. Until NWTC
# sets it, the full dim_students population is used (no window applied).
ENROLLMENT_WINDOW_START = None      # e.g. "2019-07-01"


# =============================================================================
# 4.1  STUDENT XREF  (STM Step 2; anchors A1/A2; CF-06)
# =============================================================================
users = bronze("users").select(
    F.col("key_id").cast("bigint").alias("canvas_user_id"),
    F.col("merged_into_user_id").cast("bigint").alias("merged_into_user_id"),
    F.trim(F.col("name")).alias("canvas_display_name"),
    F.col("workflow_state").alias("user_workflow_state"),
)

# Merge resolution: follow merged_into_user_id to the surviving user.
merged = users.filter(F.col("merged_into_user_id").isNotNull())
chain = (
    merged.alias("a")
    .join(
        merged.alias("b"),
        F.col("a.merged_into_user_id") == F.col("b.canvas_user_id"),
        "inner",
    )
    .count()
)
assert chain == 0, (
    f"{chain} chained user merges found - single-hop remap is insufficient, loop required"
)

user_resolved = users.select(
    "canvas_user_id",
    F.coalesce("merged_into_user_id", "canvas_user_id").alias("resolved_user_id"),
    (F.col("merged_into_user_id").isNotNull()).alias("was_merge_resolved"),
    "canvas_display_name",
)

pseud = bronze("pseudonyms").select(
    F.col("user_id").cast("bigint").alias("canvas_user_id"),
    F.trim(F.col("sis_user_id")).alias("sis_user_id"),
    F.col("sis_batch_id").cast("bigint").alias("sis_batch_id"),
    F.col("is_inst_id").cast("boolean").alias("is_inst_id"),
    F.col("position").cast("int").alias("position"),
    F.col("workflow_state"),
    F.col("deleted_at").cast("timestamp").alias("deleted_ts"),
)

# CF-06 dedup: one pseudonym per user.
w_p = Window.partitionBy("canvas_user_id").orderBy(
    F.col("sis_user_id").isNull().asc(),
    F.col("is_inst_id").desc_nulls_last(),
    F.col("position").asc_nulls_last(),
    F.col("deleted_ts").isNotNull().asc(),
)

pseud1 = pseud.withColumn("_rn", F.row_number().over(w_p)).filter("_rn=1").drop("_rn")

# NOTE: Workday dim_students comes from workday_cs schema in Gold lakehouse
wd_students = (
    spark.read.format("delta").load(workday_gold_path("dim_students"))
    .filter(F.col("is_current") == True)
    .select(
        F.col("student_key"),
        F.trim(F.col(WD_STUDENT_ID_COL)).alias("workday_student_id"),
    )
)

xref = (
    user_resolved.join(
        pseud1.withColumnRenamed("canvas_user_id", "resolved_user_id"),
        "resolved_user_id",
        "left",
    )
    .join(
        wd_students,
        F.col("sis_user_id") == F.col("workday_student_id"),
        "left",
    )
    .select(
        F.col("canvas_user_id"),
        "resolved_user_id",
        "was_merge_resolved",
        "sis_user_id",
        "sis_batch_id",
        F.coalesce("student_key", F.lit(-1)).alias("student_key"),
        (F.col("student_key").isNotNull()).alias("is_student_matched"),
        (F.col("sis_batch_id").isNotNull()).alias("is_sis_provisioned"),
        "canvas_display_name",
    )
)

(
    xref.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
    .save(silver_path("slv_canvas_student_xref"))
)

# Attribution metric (O3): publish the match rate rather than asserting it.
attr_stats = xref.selectExpr(
    "current_timestamp() as measured_at",
    "count(1) as canvas_users",
    "sum(case when is_sis_provisioned then 1 else 0 end) as sis_provisioned_users",
    "sum(case when is_student_matched then 1 else 0 end) as matched_users",
).withColumn(
    "match_rate_all",
    F.col("matched_users") / F.col("canvas_users"),
).withColumn(
    "match_rate_sis_provisioned",
    # Avoid use of F.nullif (not available in this Spark build); implement NULLIF semantics
    F.when(
        F.col("sis_provisioned_users") != 0,
        F.col("matched_users") / F.col("sis_provisioned_users"),
    ).otherwise(F.lit(None).cast("double")),
)

(
    attr_stats.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
    .save(silver_path("slv_canvas_attribution_stats"))
)


# =============================================================================
# 4.2  COURSE / SECTION CROSSWALK  (STM Step 4.1; CF-07)
#      Using Workday course_section_code logic to derive course_section_key
# =============================================================================

# Canvas courses
c_course = bronze("courses").select(
    F.col("key_id").cast("bigint").alias("canvas_course_id"),
    F.trim(F.col("sis_source_id")).alias("course_sis_id"),
    F.trim(F.col("course_code")).alias("canvas_course_code"),
    F.trim(F.col("name")).alias("canvas_course_name"),
    F.col("enrollment_term_id").cast("bigint").alias("canvas_term_id"),
    F.col("start_at").cast("timestamp").alias("course_start_at"),
    F.col("conclude_at").cast("timestamp").alias("course_conclude_at"),
)

# Canvas sections
c_sect = bronze("course_sections").select(
    F.col("key_id").cast("bigint").alias("canvas_section_id"),
    F.col("course_id").cast("bigint").alias("canvas_course_id"),
    F.trim(F.col("sis_source_id")).alias("section_sis_id"),
)

# Workday section listing + course section dimension to build course_section_code
wd_sect_dim = spark.read.format("delta").load(workday_gold_path("dim_course_sections")).select(
    F.col("course_section_key"),
    F.trim(F.col("course_code")).alias("wd_course_code"),
    F.trim(F.col("academic_period")).alias("wd_academic_period"),
)

wd_section_listing = spark.read.format("delta").load(workday_silver_path("section_listing_data")).select(
    F.col("course_section_key"),
    F.col("section_number"),
)

# Build Workday course_section_code: course_code + '_' + academic_year + '_' + session + '_' + section_number
wd_course_section_code = (
    wd_section_listing.alias("ls")
    .join(wd_sect_dim.alias("cs"), "course_section_key", "inner")
    .select(
        F.col("course_section_key"),
        F.concat_ws(
            "_",
            F.trim(F.col("wd_course_code")),
            F.trim(F.regexp_replace(F.col("wd_academic_period"), " ", "_")),
            F.trim(F.col("section_number").cast("string")),
        ).alias("course_section_code"),
    )
)

# Join Workday course_section_code to Canvas sections via Canvas sis_source_id
sect_match = (
    wd_course_section_code.join(
        c_sect,
        F.col("course_section_code") == F.col("section_sis_id"),
        "inner",
    )
    .select(
        "canvas_section_id",
        "canvas_course_id",
        "section_sis_id",
        F.col("course_section_key").alias("course_section_key"),
        F.lit("workday_course_section_code").alias("match_method"),
    )
)

# --------------------------------------------------------------------------
# PeopleSoft-era Canvas section IDs (old SIS IDs) via student-based voting
# --------------------------------------------------------------------------

# Old (PeopleSoft-era) Canvas section ID: catalog_termcode_classnbr_campus_section
#   e.g. 10806179_2255_51369_B_1, 10806179_2232_23222_P_8W1
OLD_RX = r"^(\d{8})_(\d{4})_(\d+)_([A-Za-z0-9]+)_([A-Za-z0-9]+)$"

# Term code = 2 + YY + season digit  ->  Workday period name "20YY Season"
SEASON = {"2": "Spring", "5": "Summer", "8": "Fall"}
season_map = F.create_map(*[x for k, v in SEASON.items() for x in (F.lit(k), F.lit(v))])

# Old-format ID: take the section's own SIS ID if it has one, else the course's
c_sect_ids = (
    c_sect.join(c_course.select("canvas_course_id", "course_sis_id"), "canvas_course_id", "left")
    .withColumn(
        "old_sis_id",
        F.when(F.col("section_sis_id").rlike(OLD_RX), F.col("section_sis_id"))
         .when(F.col("course_sis_id").rlike(OLD_RX), F.col("course_sis_id")),
    )
)

old = (
    c_sect_ids
    .filter(F.col("old_sis_id").isNotNull())
    .join(sect_match.select("canvas_section_id"), "canvas_section_id", "left_anti")  # skip sections already matched
    .select(
        "canvas_section_id",
        "canvas_course_id",
        "section_sis_id",
        F.regexp_extract("old_sis_id", OLD_RX, 1).alias("ps_catalog"),
        F.regexp_extract("old_sis_id", OLD_RX, 2).alias("ps_term"),
        F.regexp_extract("old_sis_id", OLD_RX, 5).alias("ps_section"),
    )
    .withColumn(
        "wd_term",
        F.concat_ws(
            " ",
            (F.lit(2000) + F.substring("ps_term", 2, 2).cast("int")).cast("string"),
            season_map[F.substring("ps_term", 4, 1)],
        ),
    )
)

# Students enrolled in each Canvas section
# (use the same column-prefix handling as nb_canvas_silver if enrollments carries value_* columns)
enr = (
    bronze("enrollments")
    .filter(
        (F.col("type") == "StudentEnrollment")
        & (F.col("workflow_state") != "deleted")
    )
    .select(
        F.col("course_section_id").cast("bigint").alias("canvas_section_id"),
        F.col("user_id").cast("bigint").alias("canvas_user_id"),
    )
    .distinct()
)

stu = (
    spark.read.format("delta")
    .load(silver_path("slv_canvas_student_xref"))
    .filter("is_student_matched")
    .select("canvas_user_id", "student_key")
)

reg = (
    spark.read.format("delta")
    .load(workday_gold_path("fact_registration"))
    .select("student_key", "course_section_key")
    .distinct()
)

# Workday sections those students are registered in: same catalog, same term (any session)
votes = (
    old.select(
        "canvas_section_id",
        "canvas_course_id",
        "section_sis_id",
        "ps_catalog",
        "wd_term",
    )
    .join(enr, "canvas_section_id")
    .join(stu, "canvas_user_id")
    .join(reg, "student_key")
    .join(
        wd_sect_dim.select(
            "course_section_key", "wd_course_code", "wd_academic_period"
        ),
        "course_section_key",
    )
    .filter(
        (F.col("wd_course_code") == F.col("ps_catalog"))
        & F.col("wd_academic_period").startswith(F.col("wd_term"))
    )
    .groupBy(
        "canvas_section_id",
        "canvas_course_id",
        "section_sis_id",
        "course_section_key",
    )
    .agg(F.countDistinct("student_key").alias("students"))
)

# Keep the Workday section that at least half the Canvas students are in
w = Window.partitionBy("canvas_section_id").orderBy(
    F.col("students").desc(), F.col("course_section_key")
)

tot = Window.partitionBy("canvas_section_id")

old_match = (
    votes.withColumn("_rk", F.row_number().over(w))
    .withColumn("_share", F.col("students") / F.sum("students").over(tot))
    .filter("_rk = 1 AND _share >= 0.5")
    .select(
        "canvas_section_id",
        "canvas_course_id",
        "section_sis_id",
        "course_section_key",
        F.lit("student_registration").alias("match_method"),
    )
)

# everything not matched, including CPL / non-class shells, goes to the unmatched list
old_unmatched = (
    c_sect.join(sect_match.select("canvas_section_id"), "canvas_section_id", "left_anti")
    .join(old_match.select("canvas_section_id"), "canvas_section_id", "left_anti")
)

(
    old_unmatched.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
    .save(silver_path("slv_canvas_course_xwalk_unmatched"))
)

sect_match = sect_match.unionByName(old_match)

# Direct xwalk at section level: bring in course attributes and load
xwalk = sect_match.join(c_course, "canvas_course_id", "left").select(
    "canvas_section_id",
    "canvas_course_id",
    "canvas_course_code",
    "canvas_course_name",
    "canvas_term_id",
    "course_sis_id",
    "section_sis_id",
    "course_section_key",
    "match_method",
)

(
    xwalk.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
    .save(silver_path("slv_canvas_course_xwalk"))
)

# --------------------------------------------------------------------------
# Term from old-format section ID for shells without a Workday section match
# --------------------------------------------------------------------------

ap_major = (
    spark.read.format("delta")
    .load(workday_gold_path("dim_academic_period"))
    .filter("is_major_term = 1")
    .select(F.col("academic_period_name").alias("wd_term"), "academic_period_key")
)

(
    old.select("canvas_section_id", "wd_term")
    .join(ap_major, "wd_term", "left")
    .write.mode("overwrite")
    .format("delta")
    .option("overwriteSchema", "true")
    .save(silver_path("slv_canvas_section_term"))
)


# =============================================================================
# 4.3  PERIOD MAP  (STM Step 4.2)
#       + Canvas course date coverage per academic period
# =============================================================================

# Base period map from Workday dim_academic_period
ap = spark.read.format("delta").load(workday_gold_path("dim_academic_period")).select(
    "academic_period_key",
    "academic_period_name",
    "academic_year",
    "session",
    F.col("period_start_date").cast("date").alias("period_start_date"),
)

w_lead = Window.orderBy("period_start_date")
period_map_base = ap.withColumn(
    "period_end_date_derived",
    F.coalesce(
        F.date_sub(F.lead("period_start_date").over(w_lead), 1),
        F.lit("2099-12-31").cast("date"),
    ),
)

# Derive Canvas course date coverage per academic period using course_start_at/conclude_at
course_anchor = c_course.withColumn(
    "course_anchor_dt",
    F.coalesce(F.col("course_start_at"), F.col("course_conclude_at")),
)

course_with_period = course_anchor.join(
    period_map_base,
    (F.to_date(F.col("course_anchor_dt")) >= period_map_base.period_start_date)
    & (F.to_date(F.col("course_anchor_dt")) <= period_map_base.period_end_date_derived),
    "left",
)

course_agg = (
    course_with_period
    .groupBy("academic_period_key")
    .agg(
        F.min(F.to_date("course_start_at")).alias("course_min_start_date"),
        F.max(F.to_date("course_conclude_at")).alias("course_max_conclude_date"),
    )
)

period_map = (
    period_map_base.alias("pm")
    .join(course_agg.alias("ca"), "academic_period_key", "left")
)

(
    period_map.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
    .save(silver_path("slv_canvas_period_map"))
)


# =============================================================================
# 4.4  PRIMARY PROGRAM STAMP  (STM Step 4.3)
# =============================================================================

snap = (
    spark.read.format("delta").load(workday_gold_path("fact_enrollment_snapshot"))
    .filter(F.col("is_prd_snapshot") == 1)
    .select("student_key", "academic_period_key", "program_key", "snapshotdate")
)

w_s = Window.partitionBy("student_key", "academic_period_key").orderBy(
    F.col("snapshotdate").desc(), F.col("program_key").desc()
)

snap1 = snap.withColumn("_rn", F.row_number().over(w_s)).filter("_rn=1").select(
    "student_key",
    "academic_period_key",
    F.col("program_key").alias("prd_program_key"),
)

reg = spark.read.format("delta").load(workday_gold_path("fact_registration")).select(
    "student_key",
    "academic_period_key",
    F.col("primary_program_of_study_key").alias("reg_program_key"),
    F.col("credits").cast("double").alias("credits"),
)

w_r = Window.partitionBy("student_key", "academic_period_key").orderBy(
    F.col("credits").desc_nulls_last(), F.col("reg_program_key").desc()
)

reg1 = reg.withColumn("_rn", F.row_number().over(w_r)).filter("_rn=1").select(
    "student_key", "academic_period_key", "reg_program_key"
)

program_stamp = snap1.join(
    reg1, ["student_key", "academic_period_key"], "full"
).select(
    "student_key",
    "academic_period_key",
    F.coalesce("prd_program_key", "reg_program_key", F.lit(-1)).alias(
        "primary_program_key"
    ),
    F.when(F.col("prd_program_key").isNotNull(), "prd_snapshot")
    .when(F.col("reg_program_key").isNotNull(), "registration_fallback")
    .otherwise("none")
    .alias("program_source"),
)

(
    program_stamp.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
    .save(silver_path("slv_canvas_program_stamp"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# 4.5  LOR → ACADEMIC PERIOD USING COURSE DATES
#       (use Canvas course start/conclude dates instead of LOR timestamps)
# =============================================================================

# Canvas learning outcome results (bronze)
lor = bronze("learning_outcome_results").select(
    F.col("key_id").cast("bigint").alias("lor_bk"),
    F.col("user_id").cast("bigint").alias("canvas_user_id"),
    # learning_outcome_results does not have course_id; use context_id instead
    # context_type should indicate the type (e.g. 'Course'), but we only need
    # the numeric ID here to join back to Canvas courses.
    F.col("context_id").cast("bigint").alias("canvas_course_id"),
)

# Canvas courses with date fields
c_course_dates = bronze("courses").select(
    F.col("key_id").cast("bigint").alias("canvas_course_id"),
    F.col("start_at").cast("timestamp").alias("course_start_at"),
    F.col("conclude_at").cast("timestamp").alias("course_conclude_at"),
)

# Anchor date for period mapping = course start/conclude date
lor_course = (
    lor.join(c_course_dates, "canvas_course_id", "left")
       .withColumn(
           "course_anchor_dt",
           F.coalesce(F.col("course_start_at"), F.col("course_conclude_at")),
       )
)

# Join to period_map using course-based anchor date
j = lor_course.join(
    period_map,
    (F.to_date(F.col("course_anchor_dt")) >= period_map.period_start_date)
    & (F.to_date(F.col("course_anchor_dt")) <= period_map.period_end_date_derived),
    "left",
)

w_pm = Window.partitionBy("lor_bk").orderBy(F.col("period_start_date").desc())

lor_with_period = (
    j.withColumn("_rn", F.row_number().over(w_pm))
     .filter("_rn = 1")
     .withColumn(
         "academic_period_key",
         F.coalesce(F.col("academic_period_key"), F.lit(-1)),
     )
     .drop("_rn", "period_start_date", "period_end_date_derived")
)

# Optionally, bring in student_key via the student xref if you need student + period pairs
student_xref = spark.read.format("delta").load(silver_path("slv_canvas_student_xref")).select(
    "canvas_user_id",
    "student_key",
    "is_student_matched",
)

lor_student_period = (
    lor_with_period.join(student_xref, "canvas_user_id", "left")
    .select(
        "lor_bk",
        "canvas_user_id",
        "student_key",
        "academic_period_key",
        "is_student_matched",
    )
)

# Example: write out a silver helper table if desired
# (
#     lor_student_period.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
#     .save(silver_path("slv_canvas_lor_student_period"))
# )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
