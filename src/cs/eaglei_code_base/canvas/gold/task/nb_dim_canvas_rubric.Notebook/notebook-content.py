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
    rub = spark.read.format("delta").load(silver_path("slv_canvas_rubric"))

    dim_rubric = rub.select(
        F.col("rubric_id").alias("rubric_key"),
        F.coalesce("rubric_title", F.lit("Unknown")).alias("rubric_title"),
        "points_possible",
        "association_count",
        "workflow_state",
        (F.col("workflow_state") != "deleted").alias("is_current"),
    )

    dim_rubric = (
        dim_rubric.withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit("test"))
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit("test"))
    )

    write_delta_tbl(
        dim_rubric,
        gold_path("dim_rubric"),
        ["rubric_key"],
    )

    print("dim_rubric build completed successfully")

except Exception as e:
    print("Error while building dim_rubric:", str(e))
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
