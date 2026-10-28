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


KEEP_UNMATCHED_IN_FACT = False       # keep only matched in fact
BUILD_RUBRIC_CRITERION = True       # include fact_rubric_criterion
SCALE_TOLERANCE = 0.001             # kept for consistency if needed later

try:
    # =============================================================================
    # 5.3 / 5.4  fact_rubric_assessment (+ optional fact_rubric_criterion)
    # =============================================================================

    P = ""  # rubric_associations is a CF-14 doubled table; unprefixed set per Gate 0.2

    ra = bronze("rubric_assessments").select(
        F.col("key_id").cast("bigint").alias("ra_bk"),
        F.col("user_id").cast("bigint").alias("canvas_user_id"),
        F.col("rubric_id").cast("bigint").alias("rubric_key_src"),
        F.col("rubric_association_id").cast("bigint").alias("rubric_association_id"),
        F.col("assessment_type"),
        F.col("assessor_id").cast("bigint").alias("assessor_canvas_user_id"),
        F.col("artifact_id").cast("bigint").alias("artifact_id"),
        F.col("artifact_type"),
        F.col("artifact_attempt").cast("int").alias("attempt"),
        F.col("score").cast("double").alias("assessment_score"),
        F.col("hide_points").cast("boolean").alias("hide_points"),
        F.col("created_at").cast("timestamp").alias("assessed_ts"),
        F.col("data").alias("criterion_scores_json"),
    )

    assoc = bronze("rubric_associations").select(
        F.col("key_id").cast("bigint").alias("rubric_association_id"),
        F.col(f"{P}association_id").cast("bigint").alias("assoc_target_id_assoc"),
        F.col(f"{P}association_type").alias("assoc_target_type"),
        F.col(f"{P}purpose").alias("purpose"),
    )

    assign_course = (
        spark.read.format("delta").load(silver_path("slv_canvas_assignment"))
        .select(
            F.col("assignment_id").alias("assoc_target_id_assign"),
            F.col("canvas_course_id"),
        )
    )

    ra = (
        ra.join(assoc, "rubric_association_id", "left")
        .withColumn(
            "assignment_key",
            F.when(
                F.col("assoc_target_type") == "Assignment",
                F.col("assoc_target_id_assoc"),
            ).otherwise(F.lit(-1)),
        )
        .join(
            assign_course,
            F.col("assignment_key") == F.col("assoc_target_id_assign"),
            "left",
        )
    )

    ra = stamp_common(ra, "assessed_ts")

    ra_reject = ra.filter(~F.col("is_student_matched")).withColumn(
        "reject_reason", F.lit("student_unmatched")
    )

    

    fact_ra = (
        ra
        if KEEP_UNMATCHED_IN_FACT
        else ra.filter(F.col("is_student_matched"))
    )

    fact_ra = fact_ra.select(
        "ra_bk",
        "student_key",
        F.coalesce(F.col("rubric_key_src"), F.lit(-1)).alias("rubric_key"),
        "assignment_key",
        "course_section_key",
        "canvas_course_id",
        "academic_period_key",
        "primary_program_key",
        "assessment_type",
        "purpose",
        dk("assessed_ts").alias("assessed_date_key"),
        "attempt",
        "assessment_score",
        "hide_points",
        "assessor_canvas_user_id",
    )

    # Audit columns
    fact_ra = (
        fact_ra.withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit("test"))
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit("test"))
    )

    write_delta_tbl(
        fact_ra,
        gold_path("fact_rubric_assessment"),
        ["ra_bk"],
    )

    print("fact_rubric_assessment build completed successfully")

    # -------------------------------------------------------------------------
    # Optional fact_rubric_criterion
    # -------------------------------------------------------------------------
    if BUILD_RUBRIC_CRITERION:
        from pyspark.sql.types import (
            ArrayType,
            StructType,
            StructField,
            StringType,
            DoubleType,
            LongType,
        )

        crit_schema = ArrayType(
            StructType(
                [
                    StructField("criterion_id", StringType()),
                    StructField("points", DoubleType()),
                    StructField("learning_outcome_id", LongType()),
                    StructField("description", StringType()),
                ]
            )
        )

        crit = (
            ra.withColumn("c", F.explode(F.from_json("criterion_scores_json", crit_schema)))
            .select(
                "ra_bk",
                "student_key",
                "course_section_key",
                "academic_period_key",
                F.col("c.criterion_id").alias("criterion_id"),
                F.col("c.learning_outcome_id").alias("outcome_key"),
                F.col("c.points").alias("criterion_points"),
                dk("assessed_ts").alias("assessed_date_key"),
            )
        )

        # Audit columns
        crit = (
            crit.withColumn("modified_on", F.current_timestamp())
            .withColumn("modified_by", F.lit("test"))
            .withColumn("created_on", F.current_timestamp())
            .withColumn("created_by", F.lit("test"))
        )

        write_delta_tbl(
            crit,
            gold_path("fact_rubric_criterion"),
            ["ra_bk", "criterion_id"],
        )

        print("fact_rubric_criterion build completed successfully")

except Exception as e:
    print("Error while building fact_rubric_assessment / fact_rubric_criterion:", str(e))
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
