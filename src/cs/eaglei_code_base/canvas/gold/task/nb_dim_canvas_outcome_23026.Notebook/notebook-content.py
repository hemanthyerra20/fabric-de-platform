# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
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

# =============================================================================
# OUTCOME NAMING CONVENTION - config
# Source: NWTC RDD V0.02, RPT-037 comment thread (Jenny Druckrey, DJ95R93).
# Outcome level is identified by the NAME CODE, not by where the outcome sits
# in the Canvas account tree ("All outcomes are the same in Canvas, we just
# name/group them differently" - DJ93).
#
#   ILO : ILO_#d - Description          e.g. ILO_1a - Communication - Audience, Purpose, and Format
#   PLO : P_######_##_M - Description   e.g. P_314132_01_M - Apply electrical theory
#         6-digit program number, outcome number, level designation (M = mastery)
#   CLO : C_########_## - Description   e.g. C_10806177_01 - Apply descriptive ... terminology
#         8-digit catalog number, outcome number
#   Archived: outcome number replaced by ZZ   e.g. C_10806177_ZZ - 2025 03 Characterize ...
#
# Archived outcomes are KEPT (Matt, MP42). They are flagged, never filtered.
# =============================================================================

# Patterns are applied to the trimmed name. (?i) = case-insensitive.
# The separator after the code may be a hyphen, en dash or em dash (Jenny's
# spec uses an en dash; her examples use a hyphen) - all are accepted.
SEP = r"\s*(?:[-–—:]\s*)?"

RX_ILO = r"(?i)^\s*ILO_(\d+|ZZ)([A-Za-z])?" + SEP + r"(.*)$"
RX_PLO = r"(?i)^\s*P_(\d{6})_(\d{1,3}|ZZ)(?:_([A-Za-z]))?" + SEP + r"(.*)$"
RX_CLO = r"(?i)^\s*C_(\d{8})_(\d{1,3}|ZZ)" + SEP + r"(.*)$"
# Legacy CLO pattern: 8–9 digit catalog, underscore, outcome number, then description
#   10801136_04 - ..., 101543103_1- ...
RX_CLO_LEGACY = r"^\s*(\d{8,9})_(\d{1,3})" + SEP + r"(.*)$"

# Legacy ADN PLO and legacy ILO text patterns (OLD 4a/4b/4c + retired institutional outcomes)
ADN_PROGRAM_NUMBER = "105431"   # confirmed
RX_PLO_ADN_LEGACY = r"(?i)^\s*(EPSLO\s*\d+\s*-\s*ADN|ADN\s*-\s*SLO\s*\d+|SLO\s*\d+\s*-)"
RX_ILO_LEGACY = (
    r"(?i)^\s*OLD\s*4[abc]\b"
    r"|demonstrate personal accountability"
    r"|work cooperatively and professionally"
    r"|communicate effectively"
    r"|demonstrate community and global accountability"
)


CODE_COLUMNS = ["display_name", "outcome_name"]


ILO_NUMBER_TO_FAMILY = {"1": "Communication", "2": "Critical Thinking", "3": "Collaboration"}
ILO_FAMILIES = ["Communication", "Collaboration", "Critical Thinking"]


FALLBACK_TO_ACCOUNT_RULE = False


WD_PROGRAM_TABLE = "dim_program"
WD_PROGRAM_KEY_COL = "program_key"
WD_PROGRAM_CODE_COL = "program_code"     # TODO: confirm physical name

CATALOG_CORRECTIONS = {"101543103": "10543103"}


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime as _dt

try:
    out = spark.read.format("delta").load(silver_path("slv_canvas_outcome"))
    edge = spark.read.format("delta").load(silver_path("slv_canvas_outcome_group_edge"))
    scale = spark.read.format("delta").load(silver_path("slv_canvas_outcome_scale"))

    adepth = (
        spark.read.format("delta").load(silver_path("slv_canvas_account"))
        .select(F.col("account_id"), F.col("account_depth"))
    )

    o = (
        out.join(
            edge.select(
                F.col("edge_outcome_id").alias("outcome_id"),
                F.col("group_id").alias("outcome_group_key"),
                F.col("position").alias("group_position"),
            ),
            "outcome_id",
            "left",
        )
        .join(scale, "outcome_id", "left")
        .join(adepth, out.outcome_context_id == adepth.account_id, "left")
    )

    # -------------------------------------------------------------------------
    # 1. Pick the column that carries the naming code (display_name first,
    #    outcome_name / short_description as fallback). parse_source records it.
    #    Now legacy-aware via RX_CLO_LEGACY.
    # -------------------------------------------------------------------------
    def _lvl(col_name: str):
        """Return trimmed text when it matches any official or legacy pattern."""
        c = F.trim(F.col(col_name))
        return F.when(
            c.rlike(RX_ILO)
            | c.rlike(RX_PLO)
            | c.rlike(RX_CLO)
            | c.rlike(RX_CLO_LEGACY)
            | c.rlike(RX_PLO_ADN_LEGACY)
            | c.rlike(RX_ILO_LEGACY),
            c,
        )

    o = (
        o.withColumn(
            "_code_text",
            F.when(_lvl("display_name").isNotNull(), F.trim(F.col("display_name")))
            .when(_lvl("outcome_name").isNotNull(), F.trim(F.col("outcome_name"))),
        )
        .withColumn(
            "parse_source",
            F.when(_lvl("display_name").isNotNull(), F.lit("display_name"))
            .when(_lvl("outcome_name").isNotNull(), F.lit("outcome_name"))
            .otherwise(F.lit("unparsed")),
        )
    )
    t = F.col("_code_text")

    # -------------------------------------------------------------------------
    # 2. Level from the naming convention
    # -------------------------------------------------------------------------
    o = o.withColumn(
        "_level_by_name",
        F.when(t.rlike(RX_ILO), "ILO")
        .when(t.rlike(RX_PLO), "PLO")
        .when(t.rlike(RX_CLO), "CLO")
        .when(t.rlike(RX_CLO_LEGACY), "CLO")
        .when(t.rlike(RX_PLO_ADN_LEGACY), "PLO")
        .when(t.rlike(RX_ILO_LEGACY), "ILO_LEGACY"),
    )

    o = o.withColumn(
        "outcome_level",
        F.coalesce(
            F.col("_level_by_name"),
            F.when(F.col("outcome_context_type") == "Course", F.lit("CLO")),
            F.lit("UNKNOWN"),
        ),
    )

    # Old account-tree rule - kept as a diagnostic column only.
    o = o.withColumn(
        "outcome_level_account_rule",
        F.when(F.col("outcome_context_type") == "Course", "CLO")
        .when((F.col("outcome_context_type") == "Account") & (F.col("account_depth") == 0), "ILO")
        .when(F.col("outcome_context_type") == "Account", "PLO")
        .otherwise("UNKNOWN"),
    )

    o = o.withColumn(
        "level_source",
        F.when(
            t.rlike(RX_CLO_LEGACY)
            | t.rlike(RX_PLO_ADN_LEGACY)
            | t.rlike(RX_ILO_LEGACY),
            F.lit("legacy_pattern"),
        )
        .when(F.col("_level_by_name").isNotNull(), F.lit("naming_convention"))
        .when(F.col("outcome_context_type") == "Course", F.lit("context_course"))
        .when(F.lit(FALLBACK_TO_ACCOUNT_RULE), F.lit("account_rule_fallback"))
        .otherwise(F.lit("unparsed")),
    )

    # -------------------------------------------------------------------------
    # 3. Parse the code parts per level (official + legacy CLO/PLO/ILO)
    # -------------------------------------------------------------------------
    is_ilo = F.col("outcome_level") == "ILO"
    is_plo = F.col("outcome_level") == "PLO"
    is_clo = F.col("outcome_level") == "CLO"

    def _x(rx, i):
        v = F.regexp_extract(t, rx, i)
        return F.when(v != "", v)  # '' (no match / empty group) -> NULL

    # Catalog correction map for legacy 9-digit catalogs
    cat_corr_map = F.create_map(
        *[x for k, v in CATALOG_CORRECTIONS.items() for x in (F.lit(k), F.lit(v))]
    )

    o = (
        o
        # outcome number ('01', '1', or 'ZZ' for archived)
        .withColumn(
            "outcome_number",
            F.when(is_ilo, _x(RX_ILO, 1))
            .when(is_plo, _x(RX_PLO, 2))
            .when(is_clo, F.coalesce(_x(RX_CLO, 2), _x(RX_CLO_LEGACY, 2))),
        )
        .withColumn("outcome_number", F.upper("outcome_number"))
        .withColumn(
            "is_archived", F.coalesce(F.col("outcome_number") == "ZZ", F.lit(False))
        )
        # description = text after the code
        .withColumn(
            "outcome_description",
            F.trim(
                F.when(is_ilo, _x(RX_ILO, 3))
                .when(is_plo, _x(RX_PLO, 4))
                .when(is_clo, F.coalesce(_x(RX_CLO, 3), _x(RX_CLO_LEGACY, 3)))
            ),
        )
        # ILO parts
        .withColumn("ilo_number", F.when(is_ilo, F.col("outcome_number")))
        .withColumn(
            "ilo_dimension_letter", F.when(is_ilo, F.lower(_x(RX_ILO, 2)))
        )
        .withColumn(
            "ilo_dimension_code",
            F.when(
                is_ilo & F.col("ilo_dimension_letter").isNotNull(),
                F.concat(F.col("ilo_number"), F.col("ilo_dimension_letter")),
            ),
        )
        # PLO parts: 6-digit program number (+ WTCS display format NN-NNN-N)
        # NOTE: `_raw` column does not exist in this DataFrame, so we cannot
        # special-case PLO_ADN here. All PLOs use the parsed program number or
        # ADN_PROGRAM_NUMBER for legacy ADN patterns.
        .withColumn(
            "program_number",
            F.when(
                is_plo,
                F.when(t.rlike(RX_PLO_ADN_LEGACY), F.lit(ADN_PROGRAM_NUMBER)).otherwise(
                    _x(RX_PLO, 1)
                ),
            ),
        )
        .withColumn(
            "program_number_wtcs",
            F.when(
                F.col("program_number").isNotNull(),
                F.concat_ws(
                    "-",
                    F.substring("program_number", 1, 2),
                    F.substring("program_number", 3, 3),
                    F.substring("program_number", 6, 1),
                ),
            ),
        )
        .withColumn(
            "level_designation", F.when(is_plo, F.upper(_x(RX_PLO, 3)))
        )
        # CLO parts: 8-digit catalog number (+ WTCS display format NN-NNN-NNN)
        .withColumn(
            "catalog_number",
            F.when(
                is_clo,
                F.coalesce(
                    cat_corr_map[F.coalesce(_x(RX_CLO, 1), _x(RX_CLO_LEGACY, 1))],
                    F.coalesce(_x(RX_CLO, 1), _x(RX_CLO_LEGACY, 1)),
                ),
            ),
        )
        .withColumn(
            "catalog_number_wtcs",
            F.when(
                F.col("catalog_number").isNotNull(),
                F.concat_ws(
                    "-",
                    F.substring("catalog_number", 1, 2),
                    F.substring("catalog_number", 3, 3),
                    F.substring("catalog_number", 6, 3),
                ),
            ),
        )
    )

    # -------------------------------------------------------------------------
    # 4. ILO family: name text first, outcome number as fallback
    #    The family word is the FIRST segment of the description
    #    ("Communication - Audience, Purpose, and Format"), so a dimension that
    #    merely mentions another family's word is not misread.
    # -------------------------------------------------------------------------
    first_seg = F.lower(
        F.trim(
            F.split(
                F.coalesce(F.col("outcome_description"), F.lit("")), r"\s+[-–—]\s+"
            ).getItem(0)
        )
    )
    fam_from_text = F.lit(None).cast("string")
    for fam in ILO_FAMILIES:
        fam_from_text = F.when(first_seg.contains(fam.lower()), F.lit(fam)).otherwise(
            fam_from_text
        )

    num_map = F.create_map(
        *[x for k, v in ILO_NUMBER_TO_FAMILY.items() for x in (F.lit(k), F.lit(v))]
    )
    o = (
        o.withColumn("_fam_text", F.when(is_ilo, fam_from_text))
        .withColumn("_fam_num", F.when(is_ilo, num_map[F.col("ilo_number")]))
        .withColumn(
            "ilo_family_name",
            F.when(is_ilo, F.coalesce("_fam_text", "_fam_num", F.lit("Unassigned"))).otherwise(
                F.lit("Not Applicable")
            ),
        )
        .withColumn(
            "ilo_family_source",
            F.when(~is_ilo, F.lit(None).cast("string"))
            .when(F.col("_fam_text").isNotNull(), "name_text")
            .when(F.col("_fam_num").isNotNull(), "number_map")
            .otherwise("unresolved"),
        )
        # diagnostic: text and number disagree -> evidence for the 2/3 numbering question
        .withColumn(
            "ilo_family_conflict",
            F.coalesce(
                is_ilo
                & F.col("_fam_text").isNotNull()
                & F.col("_fam_num").isNotNull()
                & (F.col("_fam_text") != F.col("_fam_num")),
                F.lit(False),
            ),
        )
        # Strict RPT-028 flag: official ILO_ dimensions only
        .withColumn(
            "is_rpt028_ilo",
            (F.col("outcome_level") == "ILO")
            & F.col("_code_text").rlike(r"(?i)^\s*ILO_\d+[a-z]")
            & F.col("ilo_family_name").isin(
                "Communication", "Collaboration", "Critical Thinking"
            ),
        )
    )

    # -------------------------------------------------------------------------
    # 5. Sort key: ILO 1a,1b..3f in order; PLO/CLO by outcome number; archived last
    # -------------------------------------------------------------------------
    num_int = F.when(
        F.col("outcome_number").rlike(r"^\d+$"), F.col("outcome_number").cast("int")
    )
    letter_ix = F.when(
        F.col("ilo_dimension_letter").isNotNull(),
        F.ascii("ilo_dimension_letter") - F.lit(96),
    ).otherwise(F.lit(0))
    o = o.withColumn(
        "outcome_sequence",
        F.when(
            F.col("is_archived"), F.lit(9000) + F.coalesce("group_position", F.lit(0))
        )
        .when(is_ilo & num_int.isNotNull(), num_int * 100 + letter_ix)
        .when(num_int.isNotNull(), num_int)
        .otherwise(F.coalesce("group_position", F.lit(9999))),
    )

    # Report label for ILO dimensions, e.g. "1a. Context" (friendly name from
    # short_description, which Jenny updated to Matt's MP84 list - DJ76R73).
    o = o.withColumn(
        "dimension_label",
        F.when(
            F.col("ilo_dimension_code").isNotNull(),
            F.concat(
                F.col("ilo_dimension_code"),
                F.lit(". "),
                F.coalesce("outcome_name", "outcome_description"),
            ),
        ),
    )

    # -------------------------------------------------------------------------
    # 6. PLO -> owning program (guarded; never fails the build)
    # -------------------------------------------------------------------------
    o = o.withColumn(
        "_prog_digits",
        F.regexp_replace(F.coalesce("program_number", F.lit("")), r"[^0-9]", ""),
    )
    prog_match_status = "not_attempted"
    try:
        prog = spark.read.format("delta").load(workday_gold_path(WD_PROGRAM_TABLE))
        if WD_PROGRAM_CODE_COL in prog.columns and WD_PROGRAM_KEY_COL in prog.columns:
            if "is_current" in prog.columns:
                prog = prog.filter(F.col("is_current") == True)
            prog_lkp = (
                prog.select(
                    F.regexp_replace(
                        F.col(WD_PROGRAM_CODE_COL).cast("string"), r"[^0-9]", ""
                    ).alias("_prog_digits"),
                    F.col(WD_PROGRAM_KEY_COL).alias("_prog_key"),
                )
                .filter(F.length("_prog_digits") > 0)
                .groupBy("_prog_digits")
                .agg(
                    F.countDistinct("_prog_key").alias("_n_keys"),
                    F.min("_prog_key").alias("_prog_key"),
                )
            )
            o = o.join(prog_lkp, "_prog_digits", "left")
            o = (
                o.withColumn(
                    "owning_program_key",
                    F.when(is_plo & (F.col("_n_keys") == 1), F.col("_prog_key")).otherwise(
                        F.lit(-1)
                    ),
                )
                .withColumn(
                    "program_match_status",
                    F.when(~is_plo, F.lit(None).cast("string"))
                    .when(F.col("_n_keys") == 1, "matched")
                    .when(F.col("_n_keys") > 1, "ambiguous")
                    .otherwise("unmatched"),
                )
            )
            prog_match_status = "attempted"
        else:
            print(
                f"WARN: {WD_PROGRAM_TABLE} lacks {WD_PROGRAM_CODE_COL}/{WD_PROGRAM_KEY_COL} - owning_program_key set to -1"
            )
    except Exception as pe:
        print("WARN: program lookup skipped -", str(pe))

    if prog_match_status != "attempted":
        o = o.withColumn("owning_program_key", F.lit(-1)).withColumn(
            "program_match_status", F.when(is_plo, F.lit("not_attempted"))
        )

    # -------------------------------------------------------------------------
    # 7. Final shape (existing columns kept; new columns appended)
    # -------------------------------------------------------------------------
    dim_outcome = o.select(
        F.col("outcome_id").alias("outcome_key"),
        F.coalesce("outcome_name", "outcome_description", F.lit("Unknown")).alias(
            "outcome_name"
        ),
        F.coalesce("display_name", "outcome_name").alias("display_name"),
        F.regexp_replace(
            F.coalesce("description_html", F.lit("")), "<[^>]*>", ""
        ).alias("description"),
        "outcome_level",
        F.coalesce("outcome_group_key", F.lit(-1)).alias("outcome_group_key"),
        "ilo_family_name",
        F.col("outcome_context_id").alias("owning_account_key"),
        "calc_method",
        "calc_int",
        "vendor_guid",
        F.col("outcome_mastery_points").alias("mastery_points"),
        F.col("outcome_points_possible").alias("points_possible"),
        "outcome_sequence",
        "scale_key",
        "scale_source",
        "scale_inheritance",
        F.col("resolving_account_id").alias("scale_defining_account_key"),
        "workflow_state",
        (F.col("workflow_state") == "active").alias("is_current"),
        # ---- new: naming-convention attributes ----
        F.regexp_extract(F.col("_code_text"), r"^\s*(\S+)", 1).alias("outcome_code"),
        "outcome_number",
        "outcome_description",
        "is_archived",
        "ilo_number",
        "ilo_dimension_code",
        "dimension_label",
        "ilo_family_source",
        "program_number",
        "program_number_wtcs",
        "owning_program_key",
        "program_match_status",
        "level_designation",
        "catalog_number",
        "catalog_number_wtcs",
        "is_rpt028_ilo",
        # ---- new: lineage / diagnostics (hide in the semantic model) ----
        "parse_source",
        "level_source",
        "outcome_level_account_rule",
        "ilo_family_conflict",
    )

    # Ensure that all fields in the unknown row default mapping exist in the schema
    # This prevents None values from being passed into createDataFrame via with_unknown
    for col_name, default_value in {
        "outcome_name": "Unknown",
        "display_name": "Unknown",
        "description": "",
        "outcome_level": "UNKNOWN",
        "outcome_group_key": -1,
        "ilo_family_name": "Unassigned",
        "outcome_sequence": 9999,
        "is_current": False,
        "is_archived": False,
        "owning_program_key": -1,
        "level_source": "unknown_member",
        "parse_source": "unknown_member",
        "ilo_family_conflict": False,
        "is_rpt028_ilo": False,
        "created_on": _dt.datetime.now(),
        "modified_on": _dt.datetime.now(),
        "created_by": notebook_name,
        "modified_by": notebook_name,
    }.items():
        if col_name not in dim_outcome.columns:
            dim_outcome = dim_outcome.withColumn(col_name, F.lit(default_value))

    dim_outcome = (
        dim_outcome.withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit(notebook_name))
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit(notebook_name))
    )

    # ---------------------------------------------------------------------
    # Local safe version of with_unknown
    # Ensures no non-nullable fields get a None value when creating the
    # synthetic "unknown" member row. Falls back to type-based defaults
    # if a non-nullable column has no explicit default provided.
    # ---------------------------------------------------------------------
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        BooleanType,
        IntegerType,
        LongType,
        ShortType,
        FloatType,
        DoubleType,
        DecimalType,
        TimestampType,
        DateType,
    )

    def _default_for_type(data_type):
        """Return a generic default value for a given (non-nullable) Spark type."""
        if isinstance(data_type, (IntegerType, LongType, ShortType)):
            return -1
        if isinstance(data_type, (FloatType, DoubleType, DecimalType)):
            return 0.0
        if isinstance(data_type, BooleanType):
            return False
        if isinstance(data_type, TimestampType):
            return _dt.datetime.now()
        if isinstance(data_type, DateType):
            return _dt.date.today()
        # Fallback for String / other complex types
        return ""

    def with_unknown(df, key_col: str, defaults: dict):
        """Append an "unknown" (-1) member row using df.schema.

        - Uses `defaults` when provided.
        - Guarantees that non-nullable columns never receive None; instead
          they get a sensible type-based default, preventing CANNOT_BE_NONE.
        """
        row = {}
        for field in df.schema:  # type: StructField
            name = field.name
            if name == key_col:
                val = -1
            elif name in defaults:
                val = defaults[name]
            else:
                val = None

            # If column is non-nullable, do not allow None to pass through
            if val is None and not field.nullable:
                val = _default_for_type(field.dataType)

            row[name] = val

        unk = spark.createDataFrame([row], schema=df.schema)
        return df.unionByName(unk)

    # -1 Unknown member: facts carry outcome_key = -1 when a result's outcome
    # cannot be resolved (nb_05 F2 orphan check expects this row to exist).
    dim_outcome = with_unknown(
        dim_outcome,
        "outcome_key",
        {
            "outcome_name": "Unknown",
            "display_name": "Unknown",
            "description": "",
            "outcome_level": "UNKNOWN",
            "outcome_group_key": -1,
            "ilo_family_name": "Unassigned",
            "outcome_sequence": 9999,
            "is_current": False,
            "is_archived": False,
            "owning_program_key": -1,
            "level_source": "unknown_member",
            "parse_source": "unknown_member",
            "ilo_family_conflict": False,
            "is_rpt028_ilo": False,
            "created_on": _dt.datetime.now(),
            "modified_on": _dt.datetime.now(),
            "created_by": notebook_name,
            "modified_by": notebook_name,
        },
    )

    dim_outcome.cache()

    # Use keyed Delta writer helper (outcome_key as natural key)
    write_delta_tbl(
        dim_outcome,
        gold_path("dim_outcome"),
        ["outcome_key"],
    )
    # display(dim_outcome)

    print("dim_outcome build completed successfully")

except Exception as e:
    print("Error while building dim_outcome:", str(e))
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
