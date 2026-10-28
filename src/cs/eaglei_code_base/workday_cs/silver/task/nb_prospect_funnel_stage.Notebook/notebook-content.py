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

# # nb_prospect_funnel — Workday stage-progress funnel report → silver
# 
# Builds the silver funnel tables from the new Bronze funnel report (config key `get_student_stage_progress_funnel`).
# The old hash-keyed `prospect` table is retired; this notebook creates `prospect` fresh, keyed on `prospect_wid`.
# 
# | Silver table | Key | Relationships |
# |---|---|---|
# | `prospect` | `prospect_wid` | → `student_recruitment` (explicit FK), → `students` |
# | `student_recruitment` | `recruitment_wid` | → `students` (student_id) |
# | `student_stage_progression` | `history_item_wid` | → `prospect` / `student_recruitment` (inferred, with `link_method` / `link_confidence`) |
# | `prospect_360` | `prospect_wid` | flattened prospect; `latest_prior_academic_record_wid` → `academic_record_for_student_data` |
# | `prospect_dq_issues` | append | relationship + quality exceptions per run |
# 
# Read-only: `students`, `academic_record_for_student_data`.
# **Before the first run:** drop the old `prospect` table. The write cell stops with an error if it still has the old schema.


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

from datetime import datetime
from delta.tables import DeltaTable
from pyspark.sql import functions as F, types as T, Window as W

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
OPERATION_NAME = "Get_Student_Stage_Progress_Funnel"
SOURCE_NAME = "workday"
VAR_CS_LIB_NAME = "var_cs_process"
AUDIT_LAKEHOUSE_NAME = "lh_platform_monitoring"
CONFIG_SCHEMA_NAME = "config"
CONFIG_TABLE_NAME = "config_param"

# ---- Transformation settings ----
WID_PREFIX_LEN        = 21     # leading WID chars shared by a stage-history item and its prospect
DUPLICATE_WINDOW_DAYS = 1      # recruitments for same student + level within N days = duplicate candidates
BRONZE_JSON_COLUMN    = None   # set if bronze stores the raw payload as a JSON string column
# Use a deterministic load-order column so the "latest snapshot per student" is stable.
# Bronze currently has a load timestamp in `created_on`; if a dedicated `modified_on` exists,
# switch this to "modified_on".
BRONZE_ORDER_COLUMN   = "created_on"

# ---- Silver tables owned by this notebook (old hash-keyed `prospect` table is retired) ----
TARGET_TABLES = {
    "recruitment":  "student_recruitment",
    "prospect":     "prospect",
    "stage":        "student_stage_progression",
    "prospect_360": "prospect_360",
    "dq":           "prospect_dq_issues",
}

# ---- Existing silver tables reused for relationships (read-only) ----
EXISTING_STUDENTS_TABLE        = "students"
EXISTING_STUDENTS_KEY          = "student_id"
EXISTING_ACAD_RECORD_TABLE     = "academic_record_for_student_data"
EXISTING_ACAD_RECORD_KEY       = "academic_record_wid"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

source_table, target_table = "", ""

def log_failure(e, src="", tgt=""):
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(src), _safe_str(tgt), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("FAILED logged")

try:
    start_time = datetime.now()
    config_workspace_id = get_variable(VAR_CS_LIB_NAME, "platform_monitoring_workspace_id")
    config_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, AUDIT_LAKEHOUSE_NAME)
    config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, CONFIG_SCHEMA_NAME, CONFIG_TABLE_NAME)

    ## Fetching Config object values
    config_df = spark.read.format("delta").load(config_abfss_path)

    bronze_schema_name = get_param_value(config_df, SOURCE_NAME, "bronze_schema_name")
    bronze_lh_name     = get_param_value(config_df, SOURCE_NAME, "bronze_lakehouse_name")
    silver_schema_name = get_param_value(config_df, SOURCE_NAME, "silver_schema_name")
    silver_lh_name     = get_param_value(config_df, SOURCE_NAME, "silver_lakehouse_name")
    parent_object      = get_param_value(config_df, SOURCE_NAME, SERVICE_NAME.lower())
    op_config          = parent_object[OPERATION_NAME.lower()]
    bronze_table_name  = op_config["bronze_table_name"]
    TARGET_TABLES.update(op_config.get("silver_tables", {}))      # optional override from config

    workspace_id        = get_workspace_id(VAR_CS_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, silver_lh_name)

    def silver_path(name):
        return abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, name)

    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = ", ".join(f"{silver_lh_name}.{silver_schema_name}.{t}" for t in TARGET_TABLES.values())

except Exception as e:
    log_failure(e)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Helpers, schema, stage order ----------------
from pyspark.sql import functions as F, types as T, Window as W


def clean(c):
    """Trim strings and convert empty strings to null."""
    c = F.trim(c)
    return F.when(c == "", None).otherwise(c)


# Funnel order used for regression / duplicate-survivor logic; align to Recruiting Stages
# Always normalise keys to lowercase so they match stage_rank(), which lowercases the input.
_raw_stage_rank = {"Lead": 1, "Inquirer": 2, "Pre-Applicant": 3, "Applicant": 4, "Pre-Matriculant": 5, "Student": 6}
STAGE_RANK = {k.lower(): v for k, v in _raw_stage_rank.items()}
_stage_map = F.create_map(*[F.lit(x) for kv in STAGE_RANK.items() for x in kv])


def stage_rank(c):
    return _stage_map[F.lower(F.trim(c))]


# Explicit Report_Entry schema (all strings), aligned to the actual bronze column names (lower_snake_case)
# Recruitment_WID is declared alongside Workday's misspelled Recuritment_WID so either alias works.
s = lambda *names: [T.StructField(n, T.StringType()) for n in names]
stage_prog_t = T.StructType(s("Action", "Category", "Date", "Details", "History_Item_WID", "Student_WID_from_history_Item"))
prospect_t = T.StructType(
    s("Academic_Level1", "Entry_Stage", "Prospect_WID", "Recuritment_WID", "Recruitment_WID",
      "Student_Prospect", "Student_Prospect_Stage","Primary_Program_of_Study2")
)
recruit_t = T.StructType(s("Academic_Level2", "Created_On", "Recruitment_WID", "Student_Recruitment1"))
acad_t = T.StructType(
    s("Academic_Level", "Academic_Record", "Academic_Record_Status", "Academic_Record_WID",
      "Primary_Program_of_Study1", "Programs_of_Study")
)

# Top-level fields aligned with bronze schema: student, student_id, primary_academic_level, and groups
entry_t = T.StructType(
    s("student", "student_id", "primary_academic_level", "student_history_items") + [
        T.StructField("stage_progression_group", T.ArrayType(stage_prog_t)),
        T.StructField("student_prospect_records_group", T.ArrayType(prospect_t)),
        T.StructField("student_recruitment_group", T.ArrayType(recruit_t)),
        T.StructField("academic_records_group", T.ArrayType(acad_t)),
    ]
)
root_t = T.StructType([T.StructField("Report_Entry", T.ArrayType(entry_t))])


def to_report_entries(bronze_df):
    """Normalise the bronze table to one row per Report_Entry with the expected schema.

    Handles:
    - JSON string column (BRONZE_JSON_COLUMN)
    - Nested Report_Entry array column
    - Or a table that's already one row per entry with columns matching the entry_t layout
    """
    order = F.col(BRONZE_ORDER_COLUMN) if BRONZE_ORDER_COLUMN else F.lit(None).cast("timestamp")
    cols = {c.lower(): c for c in bronze_df.columns}

    if BRONZE_JSON_COLUMN:
        js = F.col(BRONZE_JSON_COLUMN)
        df = (
            bronze_df
            .select(
                F.from_json(js, root_t).alias("_root"),
                F.from_json(js, entry_t).alias("_one"),
                order.alias("_order"),
            )
            .select(
                F.explode(
                    F.when(F.col("_root.Report_Entry").isNotNull(), F.col("_root.Report_Entry"))
                    .otherwise(F.array("_one"))
                ).alias("e"),
                "_order",
            )
        )
        return df.select("e.*", "_order")

    if "report_entry" in cols:
        # Bronze has a nested Report_Entry array; normalise via explode
        df = (
            bronze_df
            .select(F.explode(cols["report_entry"]).alias("e"), order.alias("_order"))
            .select(F.from_json(F.to_json("e"), entry_t).alias("e"), "_order")
        )
        return df.select("e.*", "_order")

    # Fallback: bronze rows already match entry_t layout; just attach _order
    return bronze_df.withColumn("_order", order)


def with_soft_deletes(df, path, keys):
    """Each report entry is a full snapshot of one student, so rows already in the target for a
    student in this batch, but missing from the source, are re-emitted with is_deleted = true."""
    df = df.dropDuplicates(keys).withColumn("is_deleted", F.lit(False))
    if not DeltaTable.isDeltaTable(spark, path):
        return df
    tgt = spark.read.format("delta").load(path)
    if "is_deleted" not in tgt.columns:
        tgt = tgt.withColumn("is_deleted", F.lit(False))
    shared = [c for c in df.columns if c in tgt.columns]
    gone = (
        tgt.filter(~F.col("is_deleted"))
        .join(df.select("student_id").distinct(), "student_id", "left_semi")
        .join(df.select(*keys), keys, "left_anti")
        .select(*shared)
        .withColumn("is_deleted", F.lit(True))
    )
    return df.unionByName(gone, allowMissingColumns=True)


def read_existing_silver(name):
    path = silver_path(name)
    return spark.read.format("delta").load(path) if DeltaTable.isDeltaTable(spark, path) else None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)
bronze_df = spark.read.format("delta").load(bronze_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
 
pros = bronze_df.select("student_id", F.explode("student_prospect_records_group").alias("p"))
 
prog = F.col("p.Primary_Program_of_Study2")
blank = prog.isNull() | (F.trim(prog) == "")
 
display(
    pros.select(
        F.count("*").alias("prospect_records"),
        F.sum(F.when(blank, 1).otherwise(0)).alias("no_program_in_workday"),
        F.countDistinct(F.when(blank, F.col("student_id"))).alias("students_with_blank_program")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Read bronze ----------------
try:
    bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)
    bronze_df = spark.read.format("delta").load(bronze_abfss_path)

    # --- DEBUG: Inspect a sample row to understand structure and student_id availability ---
    sample_df = bronze_df.limit(5)
    # print("Bronze schema:")
    # sample_df.printSchema()
    # print("Sample rows:")
    # display(sample_df)

    # Normalise to one row per entry and derive student_id from the already-present column
    entries = (
        to_report_entries(bronze_df)
        .withColumn("student_id", clean(F.col("student_id")))
        .filter(F.col("student_id").isNotNull())
    )

    # Latest snapshot per student wins when the same student appears more than once
    w_latest = W.partitionBy("student_id").orderBy(F.col("_order").desc_nulls_last())
    entries = (
        entries.withColumn("_rn", F.row_number().over(w_latest))
        .filter("_rn = 1")
        .drop("_rn", "_order")
        .cache()
    )

    rows_read = entries.count()

    # Relax the hard failure: if Bronze has rows but no student_id could be parsed,
    # log and continue with zero rows instead of raising an exception.
    if rows_read == 0 and bronze_df.limit(1).count() > 0:
        warning_msg = (
            "Bronze has rows but no student_id could be parsed; "
            "check field names/casing against entry_t or set BRONZE_JSON_COLUMN. "
            "Continuing with 0 student snapshots."
        )
        print(warning_msg)
        # Optionally log as a failure/issue but do not raise to avoid pipeline hard stop
        # log_failure(ValueError(warning_msg), source_table, target_table)

    print(f"student snapshots: {rows_read}")

    def explode_group(group_col, alias):
        return entries.select("student_id", F.explode(group_col).alias(alias))

except Exception as e:
    log_failure(e, source_table, target_table)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Recruitment, prospect, academic records (in-memory), stage progression ----------------
try:
    recruitment = (
        explode_group("student_recruitment_group", "r")
        .select(
            clean(F.col("r.Recruitment_WID")).alias("recruitment_wid"),
            "student_id",
            clean(F.col("r.Academic_Level2")).alias("academic_level"),
            # Preserve the actual recruitment date; the audit layer will add its own timestamp column.
            F.to_date(clean(F.col("r.Created_On"))).alias("recruitment_created_on"),
        )
        .filter(F.col("recruitment_wid").isNotNull())
    )

    prospect = (
        explode_group("student_prospect_records_group", "p")
        .select(
            clean(F.col("p.Prospect_WID")).alias("prospect_wid"),
            # Bronze schema only has the misspelled Recuritment_WID field; use that as the recruitment key.
            clean(F.col("p.Recuritment_WID")).alias("recruitment_wid"),
            "student_id",
            clean(F.col("p.Academic_Level1")).alias("academic_level"),
            clean(F.col("p.Entry_Stage")).alias("entry_stage"),
            clean(F.col("p.Student_Prospect_Stage")).alias("current_stage"),
            clean(F.col("p.Primary_Program_of_Study2")).alias("primary_program_of_study"),
            clean(F.col("p.Student_Prospect")).alias("prospect_display"),
        )
        .filter(F.col("prospect_wid").isNotNull())
        .withColumn("entry_stage_rank", stage_rank(F.col("entry_stage")))
        .withColumn("current_stage_rank", stage_rank(F.col("current_stage")))
    )

    # Academic records are NOT written (silver already has academic_record_for_student_data);
    # they are used here only to enrich prospect_360, keyed by academic_record_wid.
    ACAD_RX = r"\(\d+\) - ([^/]+)/(.+?) \(([^)]*)\) - (\d{2}/\d{2}/\d{4}) - (.+)$"
    academic_record = (
        explode_group("academic_records_group", "a")
        .select(
            clean(F.col("a.Academic_Record_WID")).alias("academic_record_wid"),
            "student_id",
            clean(F.col("a.Academic_Level")).alias("academic_level"),
            clean(F.col("a.Academic_Record_Status")).alias("academic_record_status"),
            clean(F.col("a.Primary_Program_of_Study1")).alias("primary_program_of_study"),
            clean(F.regexp_extract("a.Academic_Record", ACAD_RX, 3)).alias("credential_type"),
            F.to_date(
                clean(F.regexp_extract("a.Academic_Record", ACAD_RX, 4)),
                "MM/dd/yyyy",
            ).alias("record_start_date"),
        )
        .filter(F.col("academic_record_wid").isNotNull())
    )

    stage_base = (
        explode_group("stage_progression_group", "h")
        .select(
            clean(F.col("h.History_Item_WID")).alias("history_item_wid"),
            "student_id",
            clean(F.col("h.Student_WID_from_history_Item")).alias("student_wid"),
            clean(F.col("h.Category")).alias("category"),
            clean(F.col("h.Action")).alias("action"),
            clean(F.col("h.Details")).alias("stage"),
            F.to_timestamp(clean(F.col("h.Date"))).alias("event_ts_utc"),
            clean(F.col("h.Date")).alias("event_ts_source"),
        )
        .filter(F.col("history_item_wid").isNotNull())
        .withColumn("stage_rank", stage_rank(F.col("stage")))
    )

    # Link each stage event to one prospect (history items carry no prospect key).
    # Tiers: 1 WID_PREFIX+STAGE, 2 WID_PREFIX, 3 STAGE_MATCH, 4 SINGLE_PROSPECT.
    # Linked only when exactly one prospect qualifies at the best tier; otherwise AMBIGUOUS.
    cand = (
        stage_base.alias("h")
        .join(prospect.alias("p"), "student_id")
        .select(
            F.col("h.history_item_wid"),
            F.col("h.stage"),
            F.col("p.prospect_wid"),
            F.col("p.recruitment_wid"),
            F.col("p.entry_stage"),
            (
                F.substring("h.history_item_wid", 1, WID_PREFIX_LEN)
                == F.substring("p.prospect_wid", 1, WID_PREFIX_LEN)
            ).alias("prefix_match"),
            (
                (F.lower("h.stage") == F.lower("p.entry_stage"))
                | (F.lower("h.stage") == F.lower("p.current_stage"))
            ).alias("stage_match"),
        )
    )
    w_ev = W.partitionBy("history_item_wid")
    cand = (
        cand.withColumn("n_prospects", F.count(F.lit(1)).over(w_ev))
        .withColumn(
            "link_tier",
            F.when(F.col("prefix_match") & F.col("stage_match"), 1)
            .when(F.col("prefix_match"), 2)
            .when(F.col("stage_match"), 3)
            .when(F.col("n_prospects") == 1, 4),
        )
        .filter(F.col("link_tier").isNotNull())
        .withColumn("best_tier", F.min("link_tier").over(w_ev))
        .filter(F.col("link_tier") == F.col("best_tier"))
        .withColumn("n_at_best", F.count(F.lit(1)).over(w_ev))
    )
    resolved = cand.filter("n_at_best = 1").select(
        "history_item_wid",
        "prospect_wid",
        "recruitment_wid",
        "link_tier",
        F.col("entry_stage").alias("_entry_stage"),
    )
    ambiguous = (
        cand.filter("n_at_best > 1")
        .select("history_item_wid")
        .distinct()
        .withColumn("_ambiguous", F.lit(True))
    )

    w_pros = W.partitionBy("prospect_wid").orderBy("event_ts_utc", "history_item_wid")
    stage_progression = (
        stage_base.join(resolved, "history_item_wid", "left")
        .join(ambiguous, "history_item_wid", "left")
        .withColumn(
            "link_method",
            F.element_at(
                F.array(
                    *[F.lit(x) for x in [
                        "WID_PREFIX+STAGE",
                        "WID_PREFIX",
                        "STAGE_MATCH",
                        "SINGLE_PROSPECT",
                    ]]
                ),
                F.col("link_tier"),
            ),
        )
        .withColumn(
            "link_confidence",
            F.when(F.col("link_tier") == 1, "HIGH")
            .when(F.col("link_tier") == 2, "MEDIUM")
            .when(F.col("link_tier").isin(3, 4), "LOW"),
        )
        .withColumn(
            "link_status",
            F.when(F.col("prospect_wid").isNotNull(), "LINKED")
            .when(F.col("_ambiguous"), "AMBIGUOUS")
            .otherwise("UNLINKED"),
        )
        .withColumn(
            "_seq",
            F.when(
                F.col("prospect_wid").isNotNull(),
                F.row_number().over(w_pros),
            ),
        )
        .withColumn(
            "previous_stage",
            F.when(
                F.col("prospect_wid").isNotNull(),
                F.lag("stage").over(w_pros),
            ),
        )
        .withColumn(
            "is_entry_event",
            F.coalesce(
                (F.col("_seq") == 1)
                & (F.lower("stage") == F.lower("_entry_stage")),
                F.lit(False),
            ),
        )
        .drop("link_tier", "_ambiguous", "_seq", "_entry_stage")
    )

except Exception as e:
    log_failure(e, source_table, target_table)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Prospect 360 (grain: prospect_wid) ----------------
try:
    student_attrs = (
        entries.select(
            "student_id",
            clean(
                F.regexp_extract("student", r"^(.*) \(\d+\)\s*$", 1)
            ).alias("student_name"),
            clean(F.col("primary_academic_level")).alias("primary_academic_level"),
        )
        .join(
            stage_base.groupBy("student_id").agg(
                F.first("student_wid", ignorenulls=True).alias("student_wid")
            ),
            "student_id",
            "left",
        )
    )

    stage_agg = (
        stage_progression.filter(F.col("link_status") == "LINKED")
        .groupBy("prospect_wid")
        .agg(
            F.count(F.lit(1)).alias("stage_event_count"),
            # First ever linked stage event for this prospect (regardless of stage)
            F.min("event_ts_utc").alias("first_stage_event_ts"),
            # NOTE: we are no longer persisting entry_stage_event_ts on prospect_360.
            # If needed for analysis, derive it ad hoc from stage_progression.
            # Last linked stage event for this prospect (acts as current-stage date)
            F.max("event_ts_utc").alias("last_stage_event_ts"),
            F.max_by("stage", "event_ts_utc").alias("latest_history_stage"),
            F.min_by("history_item_wid", "event_ts_utc").alias(
                "entry_history_item_wid"
            ),
            F.array_sort(
                F.collect_list(F.struct("event_ts_utc", "stage"))
            ).alias("_path"),
        )
        .withColumn(
            "stage_path",
            F.concat_ws(" > ", F.transform("_path", lambda x: x["stage"])),
        )
        .drop("_path")
    )

    base = (
        prospect
        .join(
            recruitment.select(
                "recruitment_wid",
                F.col("academic_level").alias("recruitment_academic_level"),
                "recruitment_created_on",
            ),
            "recruitment_wid",
            "left",
        )
        .join(student_attrs, "student_id", "left")
        .join(stage_agg, "prospect_wid", "left")
        .withColumn("stage_event_count", F.coalesce("stage_event_count", F.lit(0)))
    )

    acad_prior = (
        base.select("prospect_wid", "student_id", "recruitment_created_on")
        .join(academic_record, "student_id")
        .filter(
            F.col("recruitment_created_on").isNull()
            | F.col("record_start_date").isNull()
            | (F.col("record_start_date") <= F.col("recruitment_created_on"))
        )
        .groupBy("prospect_wid")
        .agg(
            F.count(F.lit(1)).alias("prior_academic_record_count"),
            F.sum(
                F.when(
                    F.lower("academic_record_status") == "active", 1
                ).otherwise(0)
            ).alias("prior_active_record_count"),
            F.max_by("academic_record_wid", "record_start_date").alias(
                "latest_prior_academic_record_wid"
            ),
            F.max_by("primary_program_of_study", "record_start_date").alias(
                "latest_prior_program"
            ),
            F.max_by("academic_level", "record_start_date").alias(
                "latest_prior_academic_level"
            ),
            F.max_by("credential_type", "record_start_date").alias(
                "latest_prior_credential_type"
            ),
            F.max_by("academic_record_status", "record_start_date").alias(
                "latest_prior_record_status"
            ),
            F.max("record_start_date").alias(
                "latest_prior_record_start_date"
            ),
        )
    )

    # Latest academic record of ANY date (incl. future-term records of new applicants).
    # Used only as the fallback for program_of_study; latest_prior_program is unchanged.
    acad_latest = (
        academic_record
        .filter(F.col("primary_program_of_study").isNotNull())
        .groupBy("student_id")
        .agg(
            F.max_by(
                "primary_program_of_study",
                F.struct(
                    F.coalesce(F.col("record_start_date"), F.lit("1900-01-01").cast("date")),
                    F.col("academic_record_wid"),
                ),
            ).alias("latest_academic_program")
        )
    )

    w_student = W.partitionBy("student_id").orderBy(
        F.col("recruitment_created_on").asc_nulls_last(),
        F.col("first_stage_event_ts").asc_nulls_last(),
        "prospect_wid",
    )
    p360 = (
        base.join(acad_prior, "prospect_wid", "left")
        .join(acad_latest, "student_id", "left")
        .withColumn(
            "program_of_study",
            F.coalesce(F.col("primary_program_of_study"), F.col("latest_academic_program")),
        )
        .withColumn(
            "program_of_study_source",
            F.when(F.col("primary_program_of_study").isNotNull(), F.lit("prospect_record"))
            .when(F.col("latest_academic_program").isNotNull(), F.lit("academic_record")),
        )
        .withColumn(
            "prior_academic_record_count",
            F.coalesce("prior_academic_record_count", F.lit(0)),
        )
        .withColumn(
            "prior_active_record_count",
            F.coalesce("prior_active_record_count", F.lit(0)),
        )
        .withColumn(
            "is_returning_student",
            F.col("prior_academic_record_count") > 0,
        )
        .withColumn(
            "recruitment_seq_for_student",
            F.row_number().over(w_student),
        )
        .withColumn(
            "_prior_max_rank",
            F.max("current_stage_rank").over(
                w_student.rowsBetween(W.unboundedPreceding, -1)
            ),
        )
        .withColumn(
            "entered_below_existing_stage",
            F.coalesce(
                F.col("entry_stage_rank") < F.col("_prior_max_rank"),
                F.lit(False),
            ),
        )
        .withColumn(
            "academic_level_mismatch",
            F.coalesce(
                F.col("primary_academic_level") != F.col("academic_level"),
                F.lit(False),
            ),
        )
        .drop("_prior_max_rank")
    )

    pa = p360.select(
        "prospect_wid",
        "student_id",
        "academic_level",
        "recruitment_created_on",
        "current_stage_rank",
        "recruitment_seq_for_student",
    )
    a, b = pa.alias("a"), pa.alias("b")
    rank_a, rank_b = (
        F.coalesce("a.current_stage_rank", F.lit(0)),
        F.coalesce("b.current_stage_rank", F.lit(0)),
    )
    dup = (
        a.join(
            b,
            (F.col("a.student_id") == F.col("b.student_id"))
            & F.col("a.academic_level").eqNullSafe(
                F.col("b.academic_level")
            )
            & (F.col("a.prospect_wid") != F.col("b.prospect_wid"))
            & (
                F.abs(F.datediff("a.recruitment_created_on", "b.recruitment_created_on"))
                <= DUPLICATE_WINDOW_DAYS
            ),
            "left",
        )
        .groupBy(F.col("a.prospect_wid").alias("prospect_wid"))
        .agg(
            F.count("b.prospect_wid").alias("duplicate_peer_count"),
            F.sum(
                F.when(
                    (rank_b > rank_a)
                    | (
                        (rank_b == rank_a)
                        & (
                            F.col("b.recruitment_seq_for_student")
                            < F.col(
                                "a.recruitment_seq_for_student"
                            )
                        )
                    ),
                    1,
                ).otherwise(0)
            ).alias("_better_peers"),
        )
    )

    prospect_360 = (
        p360.join(dup, "prospect_wid", "left")
        .withColumn(
            "has_duplicate_candidates",
            F.col("duplicate_peer_count") > 0,
        )
        .withColumn("is_preferred_record", F.col("_better_peers") == 0)
        .select(
            "prospect_wid",
            "recruitment_wid",
            "student_id",
            "student_wid",
            "student_name",
            "academic_level",
            "recruitment_academic_level",
            "primary_academic_level",
            "academic_level_mismatch",
            "recruitment_created_on",
            "recruitment_seq_for_student",
            "entry_stage",
            "entry_stage_rank",
            "current_stage",
            "current_stage_rank",
            "entered_below_existing_stage",
            "stage_event_count",
            "entry_history_item_wid",
            "first_stage_event_ts",
            "last_stage_event_ts",
            "latest_history_stage",
            "stage_path",
            "is_returning_student",
            "prior_academic_record_count",
            "prior_active_record_count",
            "latest_prior_academic_record_wid",
            "latest_prior_program",
            "primary_program_of_study",
            "program_of_study",
            "program_of_study_source",
            "latest_prior_academic_level",
            "latest_prior_credential_type",
            "latest_prior_record_status",
            "latest_prior_record_start_date",
            "duplicate_peer_count",
            "has_duplicate_candidates",
            "is_preferred_record",
        )
    )

except Exception as e:
    log_failure(e, source_table, target_table)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Data-quality and cross-table relationship checks ----------------
try:
    def issue(df, check, entity, key_col):
        return df.select(F.lit(check).alias("check_name"), F.lit(entity).alias("entity"),
                         F.col(key_col).alias("record_key"), "student_id")

    checks = [
        issue(prospect.join(recruitment.select("recruitment_wid"), "recruitment_wid", "left_anti"),
              "PROSPECT_WITHOUT_RECRUITMENT", "prospect", "prospect_wid"),
        issue(recruitment.join(prospect.select("recruitment_wid"), "recruitment_wid", "left_anti"),
              "RECRUITMENT_WITHOUT_PROSPECT", "recruitment", "recruitment_wid"),
        issue(stage_progression.filter("link_status = 'UNLINKED'"), "STAGE_EVENT_UNLINKED", "stage_event", "history_item_wid"),
        issue(stage_progression.filter("link_status = 'AMBIGUOUS'"), "STAGE_EVENT_AMBIGUOUS", "stage_event", "history_item_wid"),
        issue(stage_progression.filter("link_confidence = 'LOW'"), "STAGE_EVENT_LOW_CONFIDENCE_LINK", "stage_event", "history_item_wid"),
        issue(prospect_360.filter("has_duplicate_candidates"), "PROSPECT_DUPLICATE_CANDIDATE", "prospect", "prospect_wid"),
        issue(prospect_360.filter("entered_below_existing_stage"), "PROSPECT_ENTERED_BELOW_EXISTING_STAGE", "prospect", "prospect_wid"),
        issue(prospect_360.filter("academic_level_mismatch"), "PRIMARY_LEVEL_MISMATCH", "prospect", "prospect_wid"),
        issue(prospect.filter("current_stage_rank IS NULL OR entry_stage_rank IS NULL"), "UNMAPPED_STAGE", "prospect", "prospect_wid"),
    ]

    # Relationships to tables that already exist in silver (skipped if table/column is absent)
    students_df = read_existing_silver(EXISTING_STUDENTS_TABLE)
    if students_df is not None and EXISTING_STUDENTS_KEY in students_df.columns:
        known = students_df.select(F.col(EXISTING_STUDENTS_KEY).cast("string").alias("student_id")).distinct()
        checks.append(issue(prospect.join(known, "student_id", "left_anti"),
                            "STUDENT_NOT_IN_SILVER_STUDENTS", "prospect", "prospect_wid"))
    else:
        print(f"skip: {EXISTING_STUDENTS_TABLE}.{EXISTING_STUDENTS_KEY} not found")

    acad_df = read_existing_silver(EXISTING_ACAD_RECORD_TABLE)
    if acad_df is not None and EXISTING_ACAD_RECORD_KEY in acad_df.columns:
        known = acad_df.select(F.col(EXISTING_ACAD_RECORD_KEY).alias("academic_record_wid")).distinct()
        checks.append(issue(academic_record.join(known, "academic_record_wid", "left_anti"),
                            "ACADEMIC_RECORD_NOT_IN_SILVER", "academic_record", "academic_record_wid"))
    else:
        print(f"skip: {EXISTING_ACAD_RECORD_TABLE}.{EXISTING_ACAD_RECORD_KEY} not found")

    dq_issues = checks[0]
    for c in checks[1:]:
        dq_issues = dq_issues.unionByName(c)
    dq_issues = (dq_issues.withColumn("run_id", F.lit(_safe_str(RUN_ID)))
                          .withColumn("detected_at", F.current_timestamp())
                          .cache())
    dq_issue_count = dq_issues.count()
    display(dq_issues.groupBy("check_name").count().orderBy("check_name"))

except Exception as e:
    log_failure(e, source_table, target_table)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ---------------- Write to silver ----------------
outputs = {
    "recruitment":  (recruitment,       ["recruitment_wid"]),
    "prospect":     (prospect,          ["prospect_wid"]),
    "stage":        (stage_progression, ["history_item_wid"]),
    "prospect_360": (prospect_360,      ["prospect_wid"]),
}
rows_written = {}
current_target = ""
try:
    # Guard: refuse to merge into the retired hash-keyed `prospect` table if it still exists
    old_path = silver_path(TARGET_TABLES["prospect"])
    if DeltaTable.isDeltaTable(spark, old_path) and \
       "prospect_wid" not in spark.read.format("delta").load(old_path).columns:
        raise ValueError(f"{TARGET_TABLES['prospect']} still has the old schema (no prospect_wid). "
                         "Drop the old prospect table before running this notebook.")

    for name, (df, keys) in outputs.items():
        tname = TARGET_TABLES[name]
        current_target = f"{silver_lh_name}.{silver_schema_name}.{tname}"
        path = silver_path(tname)

        # Start from the business dataframe with soft deletes applied
        out = with_soft_deletes(df, path, keys)

        if tname == TARGET_TABLES["recruitment"]:
            # --- Schema guard for historic table versions ---
            # Older versions of the student_recruitment table only had a generic created_on column.
            # The generic write_delta_tbl/upsert_data helper expects the TARGET table to expose
            # recruitment_created_on, so add it if missing to avoid MERGE analysis errors.
            if DeltaTable.isDeltaTable(spark, path):
                existing = spark.read.format("delta").load(path)
                if "recruitment_created_on" not in existing.columns:
                    spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS (recruitment_created_on DATE)")

            # Also ensure the *source* dataframe has the business date column under the expected name
            # before we standardise / audit; keep any audit created_on separate.
            if "recruitment_created_on" not in out.columns and "created_on" in out.columns:
                out = out.withColumnRenamed("created_on", "recruitment_created_on")

        # Standardise column names + add audit metadata
        out = add_audit_column(to_lowercase_columns(out), PIPELINE_NAME).cache()
        # --- Schema guard for all targets ---
        # The merge helper needs every source column to exist in the target, so add any new
        # columns (e.g. primary_program_of_study, program_of_study) before writing.
        if DeltaTable.isDeltaTable(spark, path):
            existing_cols = {c.lower() for c in spark.read.format("delta").load(path).columns}
            missing = [f for f in out.schema.fields if f.name.lower() not in existing_cols]
            if missing:
                cols_ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in missing)
                spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({cols_ddl})")
                print(f"added columns to {tname}: {[f.name for f in missing]}")
        rows_written[tname] = out.count()
        write_delta_tbl(source_df=out, target_table_path=path, keys=keys)
        out.unpersist()
        print(f"written {current_target}: {rows_written[tname]}")

    current_target = f"{silver_lh_name}.{silver_schema_name}.{TARGET_TABLES['dq']}"
    dq_issues.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path(TARGET_TABLES["dq"]))
    dq_issues.unpersist()
    entries.unpersist()

except Exception as e:
    log_failure(e, source_table, current_target)
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# One monitoring record per silver table written
end_time = datetime.now()
for tname, n in rows_written.items():
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(f"{silver_lh_name}.{silver_schema_name}.{tname}"), rows_read, n, 0,
        start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
print(f"SUCCESS logged ({len(rows_written)} tables, {dq_issue_count} DQ issues)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
