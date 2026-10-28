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
    out = spark.read.format("delta").load(silver_path("slv_canvas_outcome"))
    edge = spark.read.format("delta").load(silver_path("slv_canvas_outcome_group_edge"))
    scale = spark.read.format("delta").load(silver_path("slv_canvas_outcome_scale"))

    adepth = (
        spark.read.format("delta").load(silver_path("slv_canvas_account"))
        .select(F.col("account_id"), F.col("account_depth"))
    )

    _grp = spark.read.format("delta").load(silver_path("slv_canvas_outcome_group"))
    root_names = _grp.select(
        F.col("group_id").alias("root_group_id"),
        F.col("group_name").alias("root_group_name"),
    )
    grp_with_root = _grp.join(root_names, "root_group_id", "left").select(
        F.col("group_id").alias("outcome_group_key"),
        F.col("root_group_name").alias("ilo_family_name"),
    )

    o = (
        out.join(
            edge.select(
                F.col("edge_outcome_id").alias("outcome_id"),
                F.col("group_id").alias("outcome_group_key"),
                F.col("position").alias("outcome_sequence"),
            ),
            "outcome_id",
            "left",
        )
        .join(scale, "outcome_id", "left")
        .join(adepth, out.outcome_context_id == adepth.account_id, "left")
    )

    o = o.withColumn(
        "outcome_level",
        F.when(F.col("outcome_context_type") == "Course", "CLO")
        .when(
            (F.col("outcome_context_type") == "Account")
            & (F.col("account_depth") == 0),
            "ILO",
        )
        .when(F.col("outcome_context_type") == "Account", "PLO")
        .otherwise("UNKNOWN"),
    )

    _dim_group_lkp = grp_with_root

    dim_outcome = (
        o.join(_dim_group_lkp, "outcome_group_key", "left")
        .select(
            F.col("outcome_id").alias("outcome_key"),
            F.coalesce("outcome_name", F.lit("Unknown")).alias("outcome_name"),
            F.coalesce("display_name", "outcome_name").alias("display_name"),
            F.regexp_replace(
                F.coalesce("description_html", F.lit("")),
                "<[^>]*>",
                "",
            ).alias("description"),
            "outcome_level",
            F.coalesce("outcome_group_key", F.lit(-1)).alias("outcome_group_key"),
            F.coalesce("ilo_family_name", F.lit("Unassigned")).alias("ilo_family_name"),
            F.col("outcome_context_id").alias("owning_account_key"),
            "calc_method",
            "calc_int",
            "vendor_guid",
            F.col("outcome_mastery_points").alias("mastery_points"),
            F.col("outcome_points_possible").alias("points_possible"),
            F.coalesce("outcome_sequence", F.lit(9999)).alias("outcome_sequence"),
            "scale_key",
            "scale_source",
            "scale_inheritance",
            F.col("resolving_account_id").alias("scale_defining_account_key"),
            "workflow_state",
            (F.col("workflow_state") == "active").alias("is_current"),
        )
    )

    dim_outcome = (
        dim_outcome.withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit("test"))
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit("test"))
    )

    # Use keyed Delta writer helper (outcome_key as natural key)
    write_delta_tbl(
        dim_outcome,
        gold_path("dim_outcome"),
        ["outcome_key"],
    )

    print("dim_outcome build completed successfully")

except Exception as e:
    print("Error while building dim_outcome:", str(e))
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
