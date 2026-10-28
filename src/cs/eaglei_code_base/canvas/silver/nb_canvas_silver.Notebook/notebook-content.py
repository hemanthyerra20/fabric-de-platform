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

layer = "cs"
service_name = "academic_period"
operation_name = "canv_pseudonyms"
source_name = "canvas"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"
config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# Controls which Silver table(s) this notebook builds.
# "ALL" = run all sections; otherwise set to a specific table name,
# e.g. "slv_canvas_outcome" or "slv_canvas_assignment".
target_table = "ALL"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_df = spark.read.format("delta").load(config_abfss_path)
display(config_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_schema_name = get_param_value(config_df,source_name,"bronze_schema_name")
# bronze_lh_name = get_param_value(config_df,source_name,"bronze_lakehouse_name")
bronze_lh_name= "lh_cs_bronze"
silver_table_name="canv_pseudonyms"
silver_schema_name = get_param_value(config_df,source_name,"silver_schema_name")
# silver_lh_name = get_param_value(config_df,source_name,"silver_lakehouse_name")
silver_lh_name="lh_cs_silver"
parent_object = get_param_value(config_df,source_name, service_name.lower())
bronze_table_name =operation_name
print(bronze_schema_name)
print(bronze_lh_name)
print(silver_schema_name)
print(silver_lh_name)
print(source_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_workspace_id(var_cs_lib_name, layer, "store")
bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name, bronze_lh_name)
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, silver_lh_name)
silver_abfss_path= abfss_path_builder(workspace_id,silver_lakehouse_id,silver_schema_name,silver_table_name)
bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, DoubleType

# ---------------------------------------------------------------- path helpers
# All IO is now done via explicit Delta paths using the workspace + lakehouse
# metadata / schema names resolved earlier in the notebook.

def bronze_path(table: str) -> str:
    """Delta path for a Bronze table (Canvas landing, prefixed with canv_)."""
    return abfss_path_builder(
        workspace_id,
        bronze_lakehouse_id,
        bronze_schema_name,
        f"canv_{table}"
    )


def silver_path(table: str) -> str:
    """Delta path for a Silver table in lh_cs_silver.canvas."""
    return abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        table
    )


# CF-14: doubled value_* columns in some Canvas tables; this flag picks which
# set Silver binds to. Once nb_01 re-lands them, only unprefixed columns exist
# and this becomes a no-op.
USE_UNPREFIXED = True
P = "" if USE_UNPREFIXED else "value_"

MAX_TREE_DEPTH = 8      # bound for both recursive walks (accounts, groups)


def bronze(table: str):
    """Latest state per key_id (CF-03), soft-deletes excluded from staging.

    This reads from the explicit Bronze Delta path instead of a catalog table
    name, so the Bronze lakehouse does not need to be the default.
    """
    path = bronze_path(table)
    df = spark.read.format("delta").load(path)
    w = Window.partitionBy("key_id").orderBy(F.col("ts").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn=1").drop("_rn")
    if "is_deleted" in df.columns:
        df = df.filter(~F.col("is_deleted"))
    return df


def walk_tree(df, id_col, parent_col):
    """Iterative ancestry closure (Spark SQL has no recursive CTE).

    Returns (ancestry, enriched) where:
      ancestry = one row per (node, ancestor, distance), distance 0 = self
      enriched = df + depth, path (names root->node), root_id
    """
    base = df.select(F.col(id_col).alias("node_id"),
                     F.col(parent_col).alias("parent_id"),
                     F.col("_name").alias("node_name"))
    anc = base.select("node_id",
                      F.col("node_id").alias("ancestor_id"),
                      F.lit(0).alias("distance"),
                      F.col("parent_id").alias("_next"))
    frontier = anc
    for d in range(1, MAX_TREE_DEPTH + 1):
        frontier = (frontier.filter(F.col("_next").isNotNull())
                    .select("node_id", F.col("_next").alias("ancestor_id"),
                            F.lit(d).alias("distance"))
                    .join(base.select(F.col("node_id").alias("ancestor_id"),
                                      F.col("parent_id").alias("_next")),
                          "ancestor_id", "left"))
        if frontier.rdd.isEmpty():
            break
        anc = anc.unionByName(frontier)
    anc = anc.drop("_next")

    depth = anc.groupBy("node_id").agg(F.max("distance").alias("depth"))
    root  = anc.join(depth, "node_id").filter(F.col("distance") == F.col("depth")) \
               .select("node_id", F.col("ancestor_id").alias("root_id"))
    path  = (anc.join(base.select(F.col("node_id").alias("ancestor_id"),
                                  F.col("node_name").alias("anc_name")), "ancestor_id")
                .groupBy("node_id")
                .agg(F.concat_ws(" > ", F.reverse(
                     F.transform(F.array_sort(F.collect_list(F.struct("distance","anc_name"))),
                                 lambda s: s["anc_name"]))).alias("path")))
    enriched = (df.join(depth, df[id_col] == depth.node_id).drop("node_id")
                  .join(root,  df[id_col] == root.node_id).drop("node_id")
                  .join(path,  df[id_col] == path.node_id).drop("node_id"))
    return anc, enriched

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2.7 RUBRICS
if target_table in ("ALL", "slv_canvas_rubric"):
    slv_canvas_rubric = (
        bronze("rubrics")
        .select(F.col("key_id").cast("bigint").alias("rubric_id"),
                F.trim(F.col(f"{P}title")).alias("rubric_title"),
                F.col(f"{P}points_possible").cast("double").alias("points_possible"),
                F.col(f"{P}association_count").cast("int").alias("association_count"),
                F.col(f"{P}workflow_state").alias("workflow_state"),
                F.col(f"{P}data").alias("criteria_json"),
                F.col("ts").alias("src_change_ts"))
    )

    write_delta_tbl(
        slv_canvas_rubric,
        silver_path("slv_canvas_rubric"),
        ["rubric_id"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2.6 ASSIGNMENTS
if target_table in ("ALL", "slv_canvas_assignment"):
    slv_canvas_assignment = (
        bronze("assignments")
        .select(F.col("key_id").cast("bigint").alias("assignment_id"),
                F.trim(F.col("title")).alias("assignment_title"),
                F.col("points_possible").cast("double").alias("points_possible"),
                F.col("grading_type"),
                F.col("type").alias("assignment_type"),
                F.col("context_id").cast("bigint").alias("canvas_course_id"),
                F.col("context_type"),
                F.col("assignment_group_id").cast("bigint").alias("assignment_group_id"),
                F.col("due_at").cast("timestamp").alias("due_ts"),
                F.col("unlock_at").cast("timestamp").alias("unlock_ts"),
                F.col("lock_at").cast("timestamp").alias("lock_ts"),
                F.col("omit_from_final_grade").cast("boolean").alias("omit_from_final_grade"),
                F.col("workflow_state"),
                F.col("ts").alias("src_change_ts"))
    )

    write_delta_tbl(
        slv_canvas_assignment,
        silver_path("slv_canvas_assignment"),
        ["assignment_id"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2.4 OUTCOMES
if target_table in ("ALL", "slv_canvas_outcome"):
    rubric_schema = StructType([
        StructField("rubric_criterion", StructType([
            StructField("mastery_points", DoubleType()),
            StructField("points_possible", DoubleType()),
            StructField("ratings", ArrayType(StructType([
                StructField("description", StringType()),
                StructField("points", DoubleType()),
            ]))),
        ])),
    ])

    out = (bronze("learning_outcomes")
        .select(F.col("key_id").cast("bigint").alias("outcome_id"),
                F.trim(F.col("short_description")).alias("outcome_name"),
                F.trim(F.col("display_name")).alias("display_name"),
                F.col("description").alias("description_html"),
                F.col("context_type").alias("outcome_context_type"),
                F.col("context_id").cast("bigint").alias("outcome_context_id"),
                F.col("calculation_method").alias("calc_method"),
                F.col("calculation_int").cast("int").alias("calc_int"),
                F.col("vendor_guid"),
                F.col("workflow_state"),
                F.from_json(F.col("data"), rubric_schema).alias("d"),
                F.col("ts").alias("src_change_ts"))
        .select("*",
                F.col("d.rubric_criterion.mastery_points").alias("outcome_mastery_points"),
                F.col("d.rubric_criterion.points_possible").alias("outcome_points_possible"),
                F.col("d.rubric_criterion.ratings").alias("outcome_ratings_json"))
        .drop("d"))

    write_delta_tbl(
        out,
        silver_path("slv_canvas_outcome"),
        ["outcome_id"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2.3 ALIGNMENTS + outcome group edges
if target_table in ("ALL", "slv_canvas_outcome_alignment", "slv_canvas_outcome_group_edge"):
    tags = (bronze("content_tags")
        .select(F.col("key_id").cast("bigint").alias("content_tag_id"),
                F.col(f"{P}tag_type").alias("tag_type"),
                F.col(f"{P}learning_outcome_id").cast("bigint").alias("outcome_id"),
                F.col(f"{P}content_id").cast("bigint").alias("content_id"),
                F.col(f"{P}content_type").alias("content_type"),
                F.col(f"{P}context_id").cast("bigint").alias("context_id"),
                F.col(f"{P}context_type").alias("context_type"),
                F.col(f"{P}mastery_score").cast("double").alias("mastery_score_override"),
                F.col(f"{P}position").cast("int").alias("position"),
                F.trim(F.col(f"{P}title")).alias("title"),
                F.col(f"{P}workflow_state").alias("workflow_state"),
                F.col("ts").alias("src_change_ts")))

    align = tags.withColumn(
        "alignment_kind",
        F.when((F.col("tag_type") == "learning_outcome_association") &
               (F.col("context_type") == "LearningOutcomeGroup"), "group_membership")
         .when(F.col("tag_type") == "learning_outcome", "artifact_alignment")
         .otherwise("other")
    )

    align = align.withColumn("is_active_alignment", F.col("workflow_state") != "deleted")

    write_delta_tbl(
        align,
        silver_path("slv_canvas_outcome_alignment"),
        ["content_tag_id"],
    )

    w_edge = Window.partitionBy("edge_outcome_id").orderBy(
        F.col("position").asc_nulls_last(),
        F.col("group_id").asc()
    )

    edge = (align.filter((F.col("alignment_kind") == "group_membership") &
                         F.col("is_active_alignment") &
                         (F.col("content_type") == "LearningOutcome"))
            .select(F.col("content_id").alias("edge_outcome_id"),
                    F.col("context_id").alias("group_id"),
                    "position")
            .withColumn("_rn", F.row_number().over(w_edge))
            .filter("_rn = 1")
            .drop("_rn"))

    write_delta_tbl(
        edge,
        silver_path("slv_canvas_outcome_group_edge"),
        ["edge_outcome_id", "group_id"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2.2 OUTCOME GROUPS + hierarchy
if target_table in ("ALL", "slv_canvas_outcome_group", "slv_canvas_group_ancestry"):
    grp = (bronze("learning_outcome_groups")
        .select(F.col("key_id").cast("bigint").alias("group_id"),
                F.col("learning_outcome_group_id").cast("bigint").alias("parent_group_id"),
                F.trim(F.col("title")).alias("_name"),
                F.col("description"),
                F.col("context_type").alias("group_context_type"),
                F.col("context_id").cast("bigint").alias("group_context_id"),
                F.col("source_outcome_group_id").cast("bigint").alias("source_group_id"),
                F.col("workflow_state"),
                F.col("ts").alias("src_change_ts")))

    grp_ancestry, grp_e = walk_tree(grp, "group_id", "parent_group_id")

    slv_canvas_outcome_group = (
        grp_e.select("group_id", F.col("_name").alias("group_name"), "description",
                      "parent_group_id",
                      F.col("depth").alias("group_depth"),
                      F.col("path").alias("group_path"),
                      F.col("root_id").alias("root_group_id"),
                      "group_context_type", "group_context_id", "source_group_id",
                      "workflow_state", "src_change_ts")
    )

    write_delta_tbl(
        slv_canvas_outcome_group,
        silver_path("slv_canvas_outcome_group"),
        ["group_id"],
    )

    write_delta_tbl(
        grp_ancestry,
        silver_path("slv_canvas_group_ancestry"),
        ["node_id", "ancestor_id", "distance"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2.1 ACCOUNTS + ancestry
if target_table in ("ALL", "slv_canvas_account", "slv_canvas_account_ancestry"):
    acct = (bronze("accounts")
        .select(F.col("key_id").cast("bigint").alias("account_id"),
                F.col("parent_account_id").cast("bigint").alias("parent_account_id"),
                F.trim(F.col("name")).alias("_name"),
                F.trim(F.col("sis_source_id")).alias("sis_source_id"),
                F.col("current_sis_batch_id").cast("bigint").alias("current_sis_batch_id"),
                F.col("uuid").alias("account_uuid"),
                F.col("workflow_state"),
                F.col("deleted_at").cast("timestamp").alias("deleted_ts"),
                F.col("ts").alias("src_change_ts")))

    acct_ancestry, acct_e = walk_tree(acct, "account_id", "parent_account_id")

    slv_canvas_account = (
        acct_e.select("account_id", F.col("_name").alias("account_name"),
                       "parent_account_id",
                       F.col("depth").alias("account_depth"),
                       F.col("path").alias("account_path"),
                       F.col("root_id").alias("root_account_id"),
                       "sis_source_id",
                       (F.col("current_sis_batch_id").isNotNull()).alias("is_sis_provisioned"),
                       "account_uuid", "workflow_state", "deleted_ts", "src_change_ts")
    )

    write_delta_tbl(
        slv_canvas_account,
        silver_path("slv_canvas_account"),
        ["account_id"],
    )

    write_delta_tbl(
        acct_ancestry,
        silver_path("slv_canvas_account_ancestry"),
        ["node_id", "ancestor_id", "distance"],
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2.5 RATING SCALES (ratings + outcome_scale)
if target_table in ("ALL", "slv_canvas_outcome_rating", "slv_canvas_outcome_scale"):
    # Ensure 'out' (outcomes DataFrame) is defined, even if the OUTCOMES section hasn't been executed yet
    if 'out' not in locals():
        # Recreate the 'out' DataFrame logic from the OUTCOMES section (2.4)
        rubric_schema = StructType([
            StructField("rubric_criterion", StructType([
                StructField("mastery_points", DoubleType()),
                StructField("points_possible", DoubleType()),
                StructField("ratings", ArrayType(StructType([
                    StructField("description", StringType()),
                    StructField("points", DoubleType()),
                ]))),
            ])),
        ])

        out = (bronze("learning_outcomes")
            .select(F.col("key_id").cast("bigint").alias("outcome_id"),
                    F.trim(F.col("short_description")).alias("outcome_name"),
                    F.trim(F.col("display_name")).alias("display_name"),
                    F.col("description").alias("description_html"),
                    F.col("context_type").alias("outcome_context_type"),
                    F.col("context_id").cast("bigint").alias("outcome_context_id"),
                    F.col("calculation_method").alias("calc_method"),
                    F.col("calculation_int").cast("int").alias("calc_int"),
                    F.col("vendor_guid"),
                    F.col("workflow_state"),
                    F.from_json(F.col("data"), rubric_schema).alias("d"),
                    F.col("ts").alias("src_change_ts"))
            .select("*",
                    F.col("d.rubric_criterion.mastery_points").alias("outcome_mastery_points"),
                    F.col("d.rubric_criterion.points_possible").alias("outcome_points_possible"),
                    F.col("d.rubric_criterion.ratings").alias("outcome_ratings_json"))
            .drop("d"))

    prof = (bronze("outcome_proficiencies")
        .select(F.col("key_id").cast("bigint").alias("proficiency_id"),
                F.col(f"{P}context_id").cast("bigint").alias("defining_account_id"),
                F.col(f"{P}context_type").alias("context_type"),
                F.col(f"{P}workflow_state").alias("workflow_state"))
        .filter((F.col("context_type") == "Account") & (F.col("workflow_state") != "deleted")))

    prof_r = (bronze("outcome_proficiency_ratings")
        .select(F.col(f"{P}outcome_proficiency_id").cast("bigint").alias("proficiency_id"),
                F.col(f"{P}points").cast("double").alias("rating_points"),
                F.trim(F.col(f"{P}description")).alias("rating_label"),
                F.col(f"{P}mastery").cast("boolean").alias("is_mastery_level"),
                F.trim(F.col(f"{P}color")).alias("rating_color"),
                F.col(f"{P}workflow_state").alias("workflow_state"))
        .filter(F.col("workflow_state") != "deleted"))

    acct_scale = (prof_r.join(prof, "proficiency_id")
        .select(F.concat(F.lit("ACCT:"), F.col("defining_account_id")).alias("scale_key"),
                F.col("defining_account_id").alias("defining_account_key"),
                "rating_points", "rating_label", "is_mastery_level", "rating_color",
                F.lit("account_proficiency").alias("rating_source")))

    out_scale = (out.filter(F.col("outcome_ratings_json").isNotNull() &
                            (F.size("outcome_ratings_json") > 0))
        .select("outcome_id", "outcome_mastery_points",
                F.explode("outcome_ratings_json").alias("r"))
        .select(F.concat(F.lit("OUT:"), F.col("outcome_id")).alias("scale_key"),
                F.lit(None).cast("bigint").alias("defining_account_key"),
                F.col("r.points").alias("rating_points"),
                F.trim(F.col("r.description")).alias("rating_label"),
                (F.col("r.points") >= F.col("outcome_mastery_points")).alias("is_mastery_level"),
                F.lit(None).cast("string").alias("rating_color"),
                F.lit("outcome_json").alias("rating_source")))

    w_seq = Window.partitionBy("scale_key").orderBy(F.col("rating_points").desc())

    slv_canvas_outcome_rating = (
        acct_scale.unionByName(out_scale)
        .withColumn("rating_sequence", F.row_number().over(w_seq))
        .withColumn("outcome_rating_key", F.abs(F.xxhash64("scale_key", F.col("rating_points").cast("string"))))
    )

    write_delta_tbl(
        slv_canvas_outcome_rating,
        silver_path("slv_canvas_outcome_rating"),
        ["outcome_rating_key"],
    )

    spark.read.format("delta").load(silver_path("slv_canvas_outcome")).createOrReplaceTempView("slv_canvas_outcome")
    spark.read.format("delta").load(silver_path("slv_canvas_account_ancestry")).createOrReplaceTempView("slv_canvas_account_ancestry")
    spark.read.format("delta").load(silver_path("slv_canvas_outcome_rating")).createOrReplaceTempView("slv_canvas_outcome_rating")

    outcome_scale_path = silver_path("slv_canvas_outcome_scale")

    # Scale precedence (fixed 14 Oct, RPT-028):
    #   1. the outcome's own rating criteria (OUT:<outcome_id>), when it has them
    #   2. else the nearest account proficiency scale (rn = 1 only)
    #   3. else OUT:<outcome_id> (no ratings -> rating_key -2 downstream)
    # Was: account scale first, so 3-point ILO/PLO outcomes were rated on the
    # 0-4 account scale and every score was stretched by 4/3.
    spark.sql(f"""
    CREATE OR REPLACE TABLE delta.`{outcome_scale_path}` AS
    WITH acct_scales AS (
      SELECT DISTINCT defining_account_key AS ancestor_id, scale_key
      FROM slv_canvas_outcome_rating WHERE rating_source = 'account_proficiency'),
    own_scales AS (
      SELECT DISTINCT scale_key
      FROM slv_canvas_outcome_rating WHERE rating_source = 'outcome_json'),
    nearest AS (
      SELECT o.outcome_id, s.scale_key, s.ancestor_id AS resolving_account_id, a.distance,
             ROW_NUMBER() OVER (PARTITION BY o.outcome_id ORDER BY a.distance) rn
      FROM slv_canvas_outcome o
      JOIN slv_canvas_account_ancestry a ON a.node_id = o.outcome_context_id
      JOIN acct_scales s ON s.ancestor_id = a.ancestor_id
      WHERE o.outcome_context_type = 'Account')
    SELECT o.outcome_id,
           CASE WHEN os.scale_key IS NOT NULL THEN os.scale_key
                WHEN n.scale_key  IS NOT NULL THEN n.scale_key
                ELSE CONCAT('OUT:', o.outcome_id) END                       AS scale_key,
           CASE WHEN os.scale_key IS NULL THEN n.resolving_account_id END   AS resolving_account_id,
           CASE WHEN os.scale_key IS NOT NULL THEN 'outcome_json'
                WHEN n.scale_key  IS NOT NULL THEN 'account_proficiency'
                ELSE 'outcome_json' END                                     AS scale_source,
           CASE WHEN os.scale_key IS NOT NULL OR n.distance IS NULL THEN NULL
                WHEN n.distance = 0 THEN 'own_account' ELSE 'inherited' END AS scale_inheritance
    FROM slv_canvas_outcome o
    LEFT JOIN own_scales os ON os.scale_key = CONCAT('OUT:', o.outcome_id)
    LEFT JOIN nearest n     ON n.outcome_id = o.outcome_id AND n.rn = 1
    """)

    # Guard: one scale per outcome
    _sc = spark.read.format("delta").load(outcome_scale_path)
    _n, _k = _sc.count(), _sc.select("outcome_id").distinct().count()
    assert _n == _k, f"slv_canvas_outcome_scale: {_n - _k} duplicate outcome rows"

    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
