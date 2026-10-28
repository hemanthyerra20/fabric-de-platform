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

JOB_NAME = "<set appropriate job name>"  # optional

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

def with_unknown(df, key_col: str, defaults: dict):
    row = {c: defaults.get(c) for c in df.columns}
    row[key_col] = -1
    unk = spark.createDataFrame([row], schema=df.schema)
    return df.unionByName(unk)

def dk(col):
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

try:
    align = spark.read.format("delta").load(silver_path("slv_canvas_outcome_alignment"))

    _out_thr = (
        spark.read.format("delta").load(silver_path("slv_canvas_outcome"))
        .select(
            F.col("outcome_id").alias("outcome_key"),
            F.col("outcome_mastery_points").alias("_outcome_mastery_points"),
        )
    )

    dim_alignment = (
        align.join(
            _out_thr,
            align.outcome_id == _out_thr.outcome_key,
            "left",
        )
        .select(
            F.col("content_tag_id").alias("alignment_key"),
            F.coalesce("outcome_id", F.lit(-1)).alias("outcome_key"),
            "alignment_kind",
            F.when(
                F.col("alignment_kind") == "group_membership", F.col("context_id")
            ).alias("outcome_group_key"),
            F.when(
                F.col("alignment_kind") == "artifact_alignment", F.col("content_id")
            ).alias("artifact_id"),
            F.when(
                F.col("alignment_kind") == "artifact_alignment", F.col("content_type")
            ).alias("artifact_type"),
            F.col("mastery_score_override"),
            F.coalesce("mastery_score_override", "_outcome_mastery_points").alias(
                "effective_mastery_points"
            ),
            F.when(F.col("mastery_score_override").isNotNull(), "alignment_override")
            .when(F.col("_outcome_mastery_points").isNotNull(), "outcome_default")
            .otherwise("scale_mastery_level")
            .alias("mastery_source"),
            "position",
            "title",
            "workflow_state",
            "is_active_alignment",
        )
    )

    
    dim_alignment = (
        dim_alignment.withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit("test"))
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit("test"))
    )

    write_delta_tbl(
        dim_alignment,
        gold_path("dim_outcome_alignment"),
        ["alignment_key"],
    )

    print("dim_outcome_alignment build completed successfully")

except Exception as e:
    print("Error while building dim_outcome_alignment:", str(e))
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
