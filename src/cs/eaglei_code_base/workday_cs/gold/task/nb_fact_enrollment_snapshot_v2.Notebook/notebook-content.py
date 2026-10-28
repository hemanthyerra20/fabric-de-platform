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

# MARKDOWN ********************

# # nb_fact_enrollment_snapshot (v2)
# 
# One function, `build_snapshot(as_of)`, answers "who was enrolled on date X" from
# registration event dates. The same function serves history and the current term:
# 
# | Row set | as_of | is_prd_snapshot | Written |
# |---|---|---|---|
# | Census | each period's own PRD | 1 | Once per period, the day after its PRD. Never touched again. |
# | Daily | today (NWTC local date) | 0 | Each run, for periods open today. Rerun-safe. |
# 
# **RUN_MODE**
# * `"daily"` – normal scheduled run.
# * `"rebuild"` – one-time fix. Rebuilds every period's census as of its PRD and today's
#   daily rows, and overwrites the table. Old daily rows are dropped (they were overwritten
#   leftovers, not real history).
# 
# Writes only `fact_enrollment_snapshot_v2`. Students missing from dim_students are counted and
# printed (and the first 20 shown) instead of being silently dropped.

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
from pyspark.sql.window import Window
from datetime import datetime
from zoneinfo import ZoneInfo

# ---- Parameters -------------------------------------------------------------
RUN_MODE = "daily"            # "daily" | "rebuild"
NWTC_TZ = "America/Chicago"   # snapshot dates are NWTC local dates, not UTC

# Silver course_registration column names for the end-of-enrollment events.
# Gold fact_registration exposes them as drop_date / withdrawal_date - confirm Silver matches.
DROP_COL = "drop_date"
WITHDRAW_COL = "withdrawal_date"

RCC_DUAL = ["1A", "1B", "1E", "8A", "8B", "9B", "9C", "9D", "9G", "9H", "9K"]

TODAY = datetime.now(ZoneInfo(NWTC_TZ)).date()

# ---- Monitoring parameters (unchanged) -------------------------------------
MASTER_RUN_ID = RUN_ID = PIPELINE_NAME = NOTEBOOK_NAME = DOMAIN = SOURCE_NAME = ""
ACTIVITY_NAME = LEVEL = LAYER = ENVIRONMENT = WORKSPACE_NAME = WORKSPACE_ID = ""
JOB_NAME = "nb_gold_layer_ingestion"

# ---- Environment -----------------------------------------------------------
var_cs_lib_name = "var_cs_process"
silver_schema_name = gold_schema_name = "workday_cs"
store_workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, "lh_cs_silver")
gold_lakehouse_id = get_lakehouse_id(var_cs_lib_name, "lh_cs_gold")

source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_enrollment_snapshot_v2"
start_time = datetime.now()


def silver(t):
    return abfss_path_builder(store_workspace_id, silver_lakehouse_id, silver_schema_name, t)


def gold(t):
    return abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, t)


def rd(path):
    return spark.read.format("delta").load(path)


def log_run(status, rows=0, err=""):
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME), status, _safe_str(LEVEL),
        _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), rows, rows, 0,
        start_time, datetime.now(), err, err, _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME), rerun_count=0,
    )


print(f"RUN_MODE={RUN_MODE}  TODAY={TODAY}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Transformation logic (pure functions: DataFrames in, DataFrames out)
# =============================================================================

def build_events(cr, sections, delivery_modes, section_units, section_dates,
                 taxonomy, transfer_credit, periods):
    """One row per course registration with the dates it was enrolled.

    start_dt : registered date. If Workday moved it to an impossible value (after the
               period ended, or after the row's own drop/withdrawal), fall back to the
               section start date.
    end_dt   : earliest of drop / withdrawal date (null = still enrolled).
    """
    campus_list = F.transform(F.split(F.col("campus_location"), ";"), lambda x: F.trim(x))
    keyword = F.regexp_extract(F.col("location_code"), "-(.*)", 1)
    matches = F.filter(campus_list, lambda x: x.contains(keyword))
    campus_clean = (
        F.when(F.coalesce(F.col("campus_location"), F.lit("")) == "", F.col("campus_location"))
         .when(F.size(matches) > 0, matches.getItem(0))
         .otherwise(campus_list.getItem(0))
    )

    reg = (
        cr.filter(F.col("registration_status") != "Waitlisted")   # never enrolled
          .select(
              "student_id", "primary_program_of_study", "academic_period", "course_section_id",
              "course_listing", "location_code", "credits",
              campus_clean.alias("campus_location_clean"),
              F.to_date("registered_date").alias("reg_dt"),
              F.to_date(DROP_COL).alias("drop_dt"),
              F.to_date(WITHDRAW_COL).alias("withdrawal_dt"),
          )
    )

    modality = (
        sections.select("course_section_id", "course_section_key", "delivery_mode_id")
                .dropDuplicates(["course_section_id"])
                .join(delivery_modes.select("delivery_mode_id",
                                            F.col("description").alias("course_modality")),
                      "delivery_mode_id", "left")
                .drop("delivery_mode_id")
    )
    # One academic unit per section. A section with two unit rows used to duplicate the
    # registration and double its credits.
    unit = section_units.groupBy("course_section_key").agg(F.min("academic_unit_id").alias("academic_unit_id"))
    sec_start = section_dates.groupBy("course_section_id").agg(F.min("section_start_date").alias("section_start_date"))
    tax = taxonomy.groupBy("course_section_id").agg(F.concat_ws(",", F.collect_set("code")).alias("tax_code_list"))
    tc = (transfer_credit.groupBy(F.col("course").alias("course_listing"), "student_id")
                         .agg(F.sum("credits").alias("transfer_credits")))
    ap = periods.select(
        F.col("period_name").alias("academic_period"),
        "academic_period_key",
        F.col("academic_period_name").alias("major_term_name"),
        "period_start_date", "period_end_date", "pell_recalculation_date",
        F.to_date("registration_open_date").alias("registration_open_date"),
    )

    no_loc = ((F.coalesce(F.col("location_code"), F.lit("")) == "")
              | (F.coalesce(F.col("campus_location_clean"), F.lit("")) == ""))
    location = (
        F.when(no_loc & F.col("course_modality").isin("Online Live", "Online"), F.lit("Online_Online"))
         .when(no_loc, F.lit("Unknown_Unknown"))
         .otherwise(F.concat(F.col("location_code"), F.lit("_"), F.col("campus_location_clean")))
    )

    end_dt = F.least("drop_dt", "withdrawal_dt")
    impossible = (F.col("reg_dt") > F.col("period_end_date")) | (F.col("reg_dt") > F.col("end_dt"))

    return (
        reg.join(modality, "course_section_id", "inner")
           .join(unit, "course_section_key", "left")
           .join(sec_start, "course_section_id", "left")
           .join(tax, "course_section_id", "left")
           .join(tc, ["student_id", "course_listing"], "left")
           .join(ap, "academic_period", "inner")
           .withColumn("end_dt", end_dt)
           .withColumn("start_dt", F.when(impossible, F.coalesce("section_start_date", "reg_dt"))
                                    .otherwise(F.col("reg_dt")))
           .filter(F.col("start_dt").isNotNull())
           .withColumn("location", location)
    )


SNAPSHOT_GRAIN = ["student_id", "primary_program_of_study", "academic_period", "academic_period_key",
                  "major_term_name", "pell_recalculation_date", "course_modality", "snapshotdate"]


def build_snapshot(events, as_of, is_prd, students, programs, colleges, campuses, ps_dual_credit):
    """Students enrolled on `as_of`, at grain student x period x program x modality x date.

    as_of is a Column: F.col("pell_recalculation_date") for census, F.lit(date) for daily.
    Returns (snapshot_df, rejects_df).
    """
    active = (
        events.withColumn("snapshotdate", as_of)
              .filter((F.col("start_dt") <= F.col("snapshotdate"))
                      & (F.col("end_dt").isNull() | (F.col("end_dt") > F.col("snapshotdate"))))
    )

    snap = (
        active.groupBy(*SNAPSHOT_GRAIN).agg(
            F.sum(F.col("credits").cast("decimal(18,5)")).alias("credits"),
            F.sum("transfer_credits").alias("transfer_credits"),
            F.concat_ws(",", F.collect_set("tax_code_list")).alias("tax_code_list"),
            # first non-null value by earliest enrolment, as before
            F.min(F.struct(F.col("academic_unit_id").isNull().alias("n"), "start_dt", "academic_unit_id")).alias("au"),
            F.min(F.struct(F.lit(0).alias("n"), "start_dt", "location")).alias("loc"),
        )
        .withColumn("academic_unit_id", F.col("au.academic_unit_id"))
        .withColumn("campus_location", F.col("loc.location"))
        .drop("au", "loc")
    )

    # Load status per student per MAJOR term on that date (all sessions + modalities together)
    term_credits = F.sum("credits").over(Window.partitionBy("student_id", "major_term_name", "snapshotdate"))
    full = F.when(F.col("major_term_name").contains("Summer"), F.lit(6)).otherwise(F.lit(12))
    snap = (
        snap.withColumn("term_credits", term_credits)
            .withColumn("load_status",
                        F.when(F.col("term_credits") >= full, "Full-Time")
                         .when(F.col("term_credits") >= full / 2, "Half-Time")
                         .otherwise("Part-Time"))
            .drop("term_credits")
            .withColumn("is_prd_snapshot", F.lit(is_prd))
    )

    # Students: unmatched go to rejects, never silently dropped
    snap = snap.join(students, "student_id", "left")
    rejects = (snap.filter(F.col("student_key").isNull())
                   .select("student_id", "academic_period", "snapshotdate", "is_prd_snapshot",
                           F.lit("student_id not in dim_students (is_current = 1)").alias("reject_reason")))
    snap = snap.filter(F.col("student_key").isNotNull())

    # Program: exactly one program_key per program name valid on the snapshot date
    pos = programs.select("program_name", "program_key", "effective_date", "expiration_date")
    snap = (
        snap.join(pos, (F.col("primary_program_of_study") == F.col("program_name"))
                       & (F.col("snapshotdate") >= F.to_date("effective_date"))
                       & (F.col("snapshotdate") <= F.to_date("expiration_date")), "left")
            .withColumn("_rn", F.row_number().over(
                Window.partitionBy(*SNAPSHOT_GRAIN)
                      .orderBy(F.col("expiration_date").desc_nulls_last(),
                               F.col("effective_date").desc_nulls_last())))
            .filter("_rn = 1")
            .drop("_rn", "program_name", "effective_date", "expiration_date")
    )

    rcc = F.array(*[F.lit(c) for c in RCC_DUAL])
    tax_codes = F.transform(F.split(F.coalesce(F.col("tax_code_list"), F.lit("")), ","),
                            lambda c: F.upper(F.trim(c)))

    snap = (
        snap.join(colleges.select("college_key", F.col("id").alias("academic_unit_id")), "academic_unit_id", "left")
            .join(campuses.select("campus_location_key",
                                  F.concat(F.col("name"), F.lit("_"), F.col("primary_campus_location")).alias("campus_location")),
                  "campus_location", "left")
            .join(ps_dual_credit.select("student_key", "academic_period_key",
                                        F.lit("1").alias("is_dualcredit_ps")).dropDuplicates(),
                  ["student_key", "academic_period_key"], "left")
            .withColumn("is_dual_credit_flag",
                        F.when(F.arrays_overlap(tax_codes, rcc) | (F.col("is_dualcredit_ps") == "1"), 1).otherwise(0))
    )

    out = snap.select(
        "student_key", "primary_program_of_study", "program_key", "academic_period", "academic_period_key",
        "course_modality", "credits",
        F.col("transfer_credits").cast("double").alias("transfer_credits"),
        F.when(F.col("transfer_credits") > 0, "Yes").otherwise("No").alias("transfer_credits_flag"),
        "pell_recalculation_date", "snapshotdate", "is_prd_snapshot", "load_status", "academic_unit_id",
        "college_key", "campus_location", "campus_location_key", "tax_code_list", "is_dual_credit_flag",
        F.sha2(F.concat_ws("||", "student_key", "academic_period_key", F.coalesce("primary_program_of_study", F.lit("")),
                           F.coalesce("course_modality", F.lit("")), F.col("snapshotdate").cast("string"),
                           F.col("is_prd_snapshot").cast("string")), 256).alias("enrollment_snapshot_key"),
        "is_dualcredit_ps",
    )
    return out, rejects


def assert_one_row_per_grain(df, name):
    dup = df.groupBy("enrollment_snapshot_key").count().filter("count > 1").limit(5).collect()
    if dup:
        raise Exception(f"{name}: {len(dup)}+ duplicate rows at grain, e.g. {dup[0]['enrollment_snapshot_key']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =============================================================================
# Run
# =============================================================================
try:
    periods = rd(gold("dim_academic_period"))
    target = gold("fact_enrollment_snapshot_v2")
    table_exists = notebookutils.fs.exists(target)
    rebuild = RUN_MODE == "rebuild" or not table_exists

    events = build_events(
        cr=rd(silver("course_registration")),
        sections=rd(silver("course_sections")),
        delivery_modes=rd(silver("delivery_mode_references")),
        section_units=rd(silver("course_section_academic_unit_data")),
        section_dates=rd(gold("dim_course_sections")),
        taxonomy=rd(gold("dim_course_section_taxonomy")),
        transfer_credit=rd(silver("student_transfer_credit")),
        periods=periods,
    ).cache()

    lookups = dict(
        students=rd(gold("dim_students")).filter("is_current = 1").select("student_id", "student_key").dropDuplicates(["student_id"]),
        programs=rd(gold("dim_program")),
        colleges=rd(gold("dim_college")),
        campuses=rd(gold("dim_campus_location")),
        ps_dual_credit=rd(gold("ref_peoplesoft_dual_credit")),
    )

    # Census is written the day AFTER the PRD, so the whole PRD day is captured.
    census_due = periods.filter(F.col("pell_recalculation_date").isNotNull()
                                & (F.col("pell_recalculation_date") < F.lit(TODAY)))
    if not rebuild:   # write-once: skip periods whose census already exists
        done = rd(target).filter("is_prd_snapshot = 1").select("academic_period_key").distinct()
        census_due = census_due.join(done, "academic_period_key", "left_anti")
    census_keys = [r[0] for r in census_due.select("academic_period_key").distinct().collect()]

    # Daily rows for every period (term + its sessions) of a major term that is open today:
    # from its earliest registration open date to its latest period end date. Whole term,
    # so load status still sums Session 1 credits after Session 1 has ended.
    open_terms = (
        periods.groupBy("academic_period_name")
               .agg(F.min(F.coalesce(F.to_date("registration_open_date"), F.col("period_start_date"))).alias("opens"),
                    F.max("period_end_date").alias("closes"))
               .filter((F.lit(TODAY) >= F.col("opens")) & (F.lit(TODAY) <= F.col("closes")))
    )
    open_keys = [r[0] for r in periods.join(open_terms, "academic_period_name")
                                     .select("academic_period_key").distinct().collect()]
    print(f"census periods to write: {len(census_keys)} | open periods for daily: {len(open_keys)}")

    census_df, census_rej = build_snapshot(
        events.filter(F.col("academic_period_key").isin(census_keys)),
        F.col("pell_recalculation_date"), 1, **lookups)
    daily_df, daily_rej = build_snapshot(
        events.filter(F.col("academic_period_key").isin(open_keys)),
        F.lit(TODAY), 0, **lookups)

    census_df = add_audit_column(census_df, JOB_NAME)
    daily_df = add_audit_column(daily_df, JOB_NAME)
    assert_one_row_per_grain(census_df, "census")
    assert_one_row_per_grain(daily_df, "daily")

    if rebuild:
        out = census_df.unionByName(daily_df)
        (out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(target))
    else:
        if census_keys:
            keys_sql = ",".join(f"'{k}'" for k in census_keys)
            (census_df.write.format("delta").mode("overwrite")
                      .option("replaceWhere", f"is_prd_snapshot = 1 AND academic_period_key IN ({keys_sql})")
                      .save(target))
        if open_keys:
            (daily_df.write.format("delta").mode("overwrite")
                     .option("replaceWhere", f"is_prd_snapshot = 0 AND snapshotdate = '{TODAY}'")
                     .save(target))

    # Students not in dim_students: reported, not written anywhere
    rejects = census_rej.unionByName(daily_rej)
    n_rej = rejects.select("student_id").distinct().count()
    print(f"students not in dim_students (excluded): {n_rej}")
    if n_rej:
        rejects.show(20, False)
    events.unpersist()

    rows_written = rd(target).count()
    print(f"fact_enrollment_snapshot_v2 rows: {rows_written}")
except Exception as e:
    log_run("failed", 0, str(e))
    raise Exception(f"Failed to load fact_enrollment_snapshot_v2: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

log_run("success", rows_written)
print("SUCCESS logged")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
