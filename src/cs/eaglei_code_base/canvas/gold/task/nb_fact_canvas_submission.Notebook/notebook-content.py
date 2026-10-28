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

xwalk = (
    spark.read.format("delta").load(silver_path("slv_canvas_course_xwalk"))
    .select("canvas_course_id", "course_section_key", "match_method")
)

pstamp = spark.read.format("delta").load(silver_path("slv_canvas_program_stamp"))
pm = spark.read.format("delta").load(silver_path("slv_canvas_period_map"))

def stamp_common(df, ts_for_period: str):
    """Add student_key, course_section_key, academic_period_key, primary_program_key.
       Expects canvas_user_id and canvas_course_id columns.
    """
    df = df.withColumn("_row_id", F.monotonically_increasing_id())

    # student_key + is_student_matched
    df = (
        df.join(xref, "canvas_user_id", "left")
        .withColumn("student_key", F.coalesce(F.col("student_key"), F.lit(-1)))
        .withColumn(
            "is_student_matched",
            F.coalesce(F.col("is_student_matched"), F.lit(False)),
        )
    )

    # course_section_key
    df = (
        df.join(
            xwalk.select(
                "canvas_course_id",
                F.col("course_section_key").alias("_cs_key"),
            ),
            "canvas_course_id",
            "left",
        )
        .withColumn("course_section_key", F.coalesce(F.col("_cs_key"), F.lit(-1)))
        .drop("_cs_key")
    )

    # academic_period_key via period map
    j = df.join(
        pm,
        (F.to_date(F.col(ts_for_period)) >= pm.period_start_date)
        & (F.to_date(F.col(ts_for_period)) <= pm.period_end_date_derived),
        "left",
    )
    w = Window.partitionBy("_row_id").orderBy(F.col("period_start_date").desc())
    df = (
        j.withColumn("_prn", F.row_number().over(w))
        .filter("_prn = 1")
        .withColumn(
            "academic_period_key", F.coalesce(F.col("academic_period_key"), F.lit(-1))
        )
        .drop(
            "_prn",
            "period_start_date",
            "period_end_date_derived",
            "academic_period_name",
            "session",
        )
    )

    # primary_program_key from program_stamp
    df = (
        df.join(
            pstamp.select("student_key", "academic_period_key", "primary_program_key"),
            ["student_key", "academic_period_key"],
            "left",
        )
        .withColumn(
            "primary_program_key",
            F.coalesce(F.col("primary_program_key"), F.lit(-1)),
        )
    )

    return df.drop("_row_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


KEEP_UNMATCHED_IN_FACT = False

try:
    # =============================================================================
    # 5.2  fact_canvas_submission (path-based)
    # =============================================================================

    sub = bronze("submissions").select(
        F.col("key_id").cast("bigint").alias("sub_bk"),
        F.col("user_id").cast("bigint").alias("canvas_user_id"),
        F.col("assignment_id").cast("bigint").alias("assignment_key_src"),
        F.col("course_id").cast("bigint").alias("canvas_course_id"),
        F.col("attempt").cast("int").alias("attempt"),
        F.col("score").cast("double").alias("score"),
        F.col("published_score").cast("double").alias("published_score"),
        F.col("grade").alias("assignment_grade"),
        F.col("excused").cast("boolean").alias("is_excused"),
        F.col("late_policy_status"),
        F.col("submission_type"),
        F.col("submitted_at").cast("timestamp").alias("submitted_ts"),
        F.col("graded_at").cast("timestamp").alias("graded_ts"),
        F.col("posted_at").cast("timestamp").alias("posted_ts"),
        F.col("cached_due_date").cast("timestamp").alias("due_ts"),
        F.col("workflow_state"),
    )

    # Anchor for period: submitted_at, else due date, else graded_at
    sub = sub.withColumn(
        "anchor_ts",
        F.coalesce(F.col("submitted_ts"), F.col("due_ts"), F.col("graded_ts")),
    )

    sub = stamp_common(sub, "anchor_ts")

    sub = (
        sub.withColumn(
            "is_late",
            (F.col("late_policy_status") == "late")
            | (
                F.col("submitted_ts").isNotNull()
                & F.col("due_ts").isNotNull()
                & (F.col("submitted_ts") > F.col("due_ts"))
            ),
        )
        .withColumn(
            "days_late",
            F.when(
                F.col("submitted_ts").isNotNull()
                & F.col("due_ts").isNotNull()
                & (F.col("submitted_ts") > F.col("due_ts")),
                F.datediff(F.to_date("submitted_ts"), F.to_date("due_ts")),
            ).otherwise(F.lit(0)),
        )
        .withColumn("is_submitted", F.col("submitted_ts").isNotNull())
    )

    sub_reject = sub.filter(~F.col("is_student_matched")).withColumn(
        "reject_reason", F.lit("student_unmatched")
    )

    # write_delta_tbl(
    #     sub_reject,
    #     silver_path("dq_rejects_submission"),
    #     ["sub_bk"],
    # )

    fact_sub = (
        sub
        if KEEP_UNMATCHED_IN_FACT
        else sub.filter(F.col("is_student_matched"))
    )

    fact_sub = fact_sub.select(
        "sub_bk",
        "student_key",
        F.coalesce(F.col("assignment_key_src"), F.lit(-1)).alias("assignment_key"),
        "course_section_key",
        "canvas_course_id",
        "academic_period_key",
        "primary_program_key",
        dk("submitted_ts").alias("submitted_date_key"),
        dk("graded_ts").alias("graded_date_key"),
        dk("due_ts").alias("due_date_key"),
        "attempt",
        "score",
        "published_score",
        "assignment_grade",
        "submission_type",
        "is_submitted",
        "is_excused",
        "is_late",
        "days_late",
        "late_policy_status",
        "workflow_state",
    )

    # Audit columns
    fact_sub = (
        fact_sub.withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit("test"))
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit("test"))
    )

    # Drop duplicate business keys
    fact_sub = fact_sub.dropDuplicates(["sub_bk"])

    write_delta_tbl(
        fact_sub,
        gold_path("fact_canvas_submission"),
        ["sub_bk"],
    )

    print("fact_canvas_submission build completed successfully")

except Exception as e:
    print("Error while building fact_canvas_submission:", str(e))
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
