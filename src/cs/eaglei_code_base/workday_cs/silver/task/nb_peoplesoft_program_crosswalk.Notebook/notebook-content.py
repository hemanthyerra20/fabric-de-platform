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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_NAME = ""
DOMAIN = "cs"
SOURCE_NAME = "workday"
VAR_CS_LIB_NAME = "var_cs_process"
AUDIT_LAKEHOUSE_NAME = ""
CONFIG_SCHEMA_NAME = ""
CONFIG_TABLE_NAME = ""
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = ""
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
SERVICE_NAME = "ras_report"
OPERATION_NAME = "get_student_course_registration"
SOURCE_NAME = "workday"
VAR_CS_LIB_NAME = "var_cs_process"
AUDIT_LAKEHOUSE_NAME = "lh_platform_monitoring"
CONFIG_SCHEMA_NAME = "config"
CONFIG_TABLE_NAME = "config_param"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    start_time = datetime.now()
    config_workspace_id = get_variable(VAR_CS_LIB_NAME, "platform_monitoring_workspace_id")
    config_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, AUDIT_LAKEHOUSE_NAME)
    config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, CONFIG_SCHEMA_NAME, CONFIG_TABLE_NAME)

    ## Fetching Config object values
    config_df = spark.read.format("delta").load(config_abfss_path)


    bronze_schema_name = get_param_value(config_df,SOURCE_NAME,"bronze_schema_name")
    bronze_lh_name = get_param_value(config_df,SOURCE_NAME,"bronze_lakehouse_name")

    silver_schema_name = get_param_value(config_df,SOURCE_NAME,"silver_schema_name")
    silver_lh_name = get_param_value(config_df,SOURCE_NAME,"silver_lakehouse_name")

    workspace_id = get_workspace_id(VAR_CS_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, silver_lh_name)

    source_table = f"{bronze_lh_name}.{bronze_schema_name}.bronze_tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.xwalk_program_peoplesoft_workday"

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(""), _safe_str(""), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("FAILED logged")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    pos_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "program_of_study"
    )

    academic_level_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "academic_level_references"
    )

    cip_code_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "cip_code_references"
    )

    educational_credential_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "educational_credential_references"
    )

    academic_unit_references_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "academic_unit_references"
    )

    # ---------------------------------------------------------------
    # Read source tables
    # ---------------------------------------------------------------

    pos_silver_df = spark.read.format("delta").load(pos_path)
    academic_level_df = spark.read.format("delta").load(academic_level_path)
    cip_code_df = spark.read.format("delta").load(cip_code_path)
    educational_credential_df = spark.read.format("delta").load(educational_credential_path)
    academic_unit_df = spark.read.format("delta").load(academic_unit_references_path)

    # ---------------------------------------------------------------
    # Calculate SCD end dates
    # ---------------------------------------------------------------

    updated_df = calculate_end_dates(
        incremental_df=pos_silver_df,
        id_columns=["program_of_study_id"],
        target_df=None,
        history_in_incremental=True
    )

    # ---------------------------------------------------------------
    # Select required columns
    # ---------------------------------------------------------------

    selected_df = (
        updated_df
        .selectExpr(
            "program_of_study_key AS program_key",
            "program_of_study_id AS program_id",
            "program_of_study_code AS program_code",
            "program_name",
            "program_area_id AS program_area",
            "program_minimum_coursework_credit",
            "program_duration_years",
            "program_duration_months",
            "program_duration_weeks",
            "program_of_study_type_id AS program_of_study_type",
            "academic_unit_id",
            "academic_level_id",
            "cip_code_id",
            "educational_credential_id",
            "to_date(effective_date) AS effective_date",
            "expiration_date",
            "CASE WHEN expiration_date >= current_date() or expiration_date = '9999-12-31' THEN 1 ELSE 0 END AS is_current"
        )
    )

    # ---------------------------------------------------------------
    # Join reference tables
    # ---------------------------------------------------------------

    joined_df = (
        selected_df.alias("s")
        .join(
            academic_level_df.alias("al"),
            F.col("s.academic_level_id") == F.col("al.academic_level_id"),
            "left"
        )
        .join(
            cip_code_df.alias("cip"),
            F.col("s.cip_code_id") == F.col("cip.cip_code_id"),
            "left"
        )
        .join(
            academic_unit_df.alias("au"),
            F.col("s.academic_unit_id") == F.col("au.academic_unit_id"),
            "left"
        )
        .join(
            educational_credential_df.alias("ec"),
            F.col("s.educational_credential_id") == F.col("ec.educational_credential_id"),
            "left"
        )
        .selectExpr(
            "s.*",
            "au.description AS academic_unit",
            "cip.description AS cip_code",
            "ec.description AS credential_type",
            "al.description AS academic_level"
        )
        .drop(
            "academic_level_id",
            "cip_code_id",
            "educational_credential_id"
        )
    )

    # =================================================================
    # nb_dim_program.ipynb  —  PS HISTORICAL PROGRAMMES   (v2)
    #
    # INSERT POINT: cell 5, line 125.
    #   line 124 is the closing  )  of joined_df
    #   line 126 is  # Add cohort-supporting columns
    #   paste into the blank line 125, at the same 4-space indent.
    #
    # Nothing above or below changes.
    #
    # WHY HERE: unioned into joined_df BEFORE the derived-column block, so
    # program_lineage_key, enrollment_stage, cip parsing, credential_level,
    # cohort_exclusion_reason and is_cohort_eligible are all computed by the
    # existing code. No duplicated logic.
    #
    # ---------------------------------------------------------------
    # CHANGES FROM v1 — all three found by the validation pack:
    #
    #   Section 2.  Two-tier prefix norm.
    #               v1 let 'Waitlist' win the mode for aid prefix 32, which
    #               set enrollment_stage = Waitlist and sent three real
    #               Technical Diplomas (Judicial Reporting, Broadcast
    #               Captioning, Machine Tool Operation) to 'No cohort'.
    #               v1's single strict filter also found nothing for prefix
    #               39, whose six Workday peers all carry excluded types, so
    #               credential_type came out NULL for three more.
    #
    #   Section 3.  DECLARE_DT bounded to the 13-year scope.
    #               v1 took MIN/MAX raw. The column holds the 1901-01-01
    #               null sentinel (3 rows) and dates up to 3201-08-20
    #               (114 rows, 12 plans) — a year-3201 expiration sets
    #               is_current = 1 and generates cohorts for every period in
    #               the dimension. Bounding to the scope window removes both
    #               and is simpler: anything before Fall 2013 would be
    #               overwritten by the cohort floor anyway.
    #
    #   Section 4.  Coalesce strict -> relaxed; both fallback dates equal.
    #               '2012-12-31' as an expiration fallback was
    #               indistinguishable from a real retirement date.
    # =================================================================

    # PS tables may not live in workday_cs. Verify before first run.
    ps_schema_name = "peoplesoft"                 # <-- VERIFY

    # Scope floor. Must match PS_COHORT_FLOOR_PERIOD in nb_dim_cohort.
    PS_SCOPE_START = "2013-08-01"                 # Fall 2013

    ps_map_path = abfss_path_builder(
        workspace_id, silver_lakehouse_id, ps_schema_name, "PS_PROGRAM_MAP"
    )
    ps_plan_path = abfss_path_builder(
        workspace_id, silver_lakehouse_id, ps_schema_name, "PS_ACAD_PLAN"
    )

    ps_map_df  = spark.read.format("delta").load(ps_map_path)
    ps_plan_df = spark.read.format("delta").load(ps_plan_path)

    # Mark the Workday rows before the union.
    joined_df = joined_df.withColumn("is_history", F.lit(0))

    # -----------------------------------------------------------------
    # 1. PS codes with no Workday programme row
    # -----------------------------------------------------------------
    ps_missing_df = (
        ps_map_df.alias("m")
        .join(
            joined_df.select("program_code").distinct().alias("d"),
            F.trim(F.col("m.PeopleSoft_Acad_Plan_Code")) == F.col("d.program_code"),
            "left_anti",
        )
        .filter(F.trim(F.col("m.PeopleSoft_Acad_Plan_Code")) != "000000")
        .select(
            F.trim(F.col("m.PeopleSoft_Acad_Plan_Code")).alias("program_code"),
            F.col("m.Workday_Program_Title").alias("program_name"),
            F.substring(F.trim(F.col("m.PeopleSoft_Acad_Plan_Code")), 1, 2).alias("aid_prefix"),
        )
        .distinct()
    )

    # -----------------------------------------------------------------
    # 2. Attribute norms per aid prefix, from programmes that DO exist.
    #    No invented WTCS conventions.
    #
    #    TIER 1 (strict): programmes that would themselves resolve to
    #    'Direct'. Note the Waitlist / Pre-Program and G / Z exclusions —
    #    without them a prefix's modal program_of_study_type can be
    #    'Waitlist', which sets enrollment_stage to Waitlist and sends the
    #    PS row straight to 'No cohort'.
    #
    #    TIER 2 (relaxed): any programme with that prefix carrying a
    #    credential. Catches prefixes where every peer happens to be an
    #    Emphasis or a reference-coded type.
    # -----------------------------------------------------------------
    _excluded_pos_types = [
        "Conversion (Undeclared)", "Undeclared_Post_Secondary",
        "Undeclared_Continuing_Education", "Emphasis",
        "Nursing_Tracks", "Conversion", "Waitlist", "Pre-Program",
    ]

    _norm_cols = ["program_code", "program_duration_years",
                  "credential_type", "program_of_study_type"]

    ps_norm_strict_df = (
        joined_df
        .filter(
            F.col("credential_type").isNotNull()
            & ~F.col("program_of_study_type").like("PROGRAM_OF_STUDY_TYPE-%")
            & ~F.col("program_of_study_type").isin(_excluded_pos_types)
            & ~F.col("program_code").endswith("G")
            & ~F.col("program_code").endswith("Z")
        )
        .select(*_norm_cols).distinct()
        .withColumn("aid_prefix", F.substring(F.col("program_code"), 1, 2))
    )

    ps_norm_relaxed_df = (
        joined_df
        .filter(
            F.col("credential_type").isNotNull()
            & ~F.col("program_of_study_type").isin("Waitlist", "Pre-Program")
            & ~F.col("program_code").endswith("G")
            & ~F.col("program_code").endswith("Z")
        )
        .select(*_norm_cols).distinct()
        .withColumn("aid_prefix", F.substring(F.col("program_code"), 1, 2))
    )

    def _modal_by_prefix(df, suffix):
        """Most common (duration, credential, type) per aid prefix.
        Ties broken by shortest duration, so the pick is deterministic."""
        w = (
            Window.partitionBy("aid_prefix")
            .orderBy(F.col("n").desc(), F.col("program_duration_years").asc())
        )
        return (
            df.groupBy("aid_prefix", "program_duration_years",
                       "credential_type", "program_of_study_type")
              .agg(F.count("*").alias("n"))
              .withColumn("rn", F.row_number().over(w))
              .filter(F.col("rn") == 1)
              .drop("n", "rn")
              .selectExpr(
                  "aid_prefix",
                  f"program_duration_years AS dur_{suffix}",
                  f"credential_type        AS cred_{suffix}",
                  f"program_of_study_type  AS pos_{suffix}",
              )
        )

    ps_norm_s = _modal_by_prefix(ps_norm_strict_df,  "s")
    ps_norm_r = _modal_by_prefix(ps_norm_relaxed_df, "r")

    # Last-resort program_of_study_type: the modal value across all
    # cohort-eligible Workday programmes. Derived, not invented. Used only
    # when a prefix has no peer in either tier.
    _pos_rows = (
        ps_norm_strict_df
        .groupBy("program_of_study_type").agg(F.count("*").alias("n"))
        .orderBy(F.col("n").desc()).limit(1).collect()
    )
    ps_pos_fallback = _pos_rows[0]["program_of_study_type"] if _pos_rows else None

    # -----------------------------------------------------------------
    # 3. Programme usage window, bounded to the 13-year scope.
    #
    #    Only declares from Fall 2013 onward matter: nb_dim_cohort floors
    #    PS programmes there, so a programme last declared in 2009
    #    correctly produces nothing. Bounding here also disposes of the
    #    corrupt dates without a separate sanity filter.
    #
    #    The cohort build joins dim_program date-bounded against
    #    declare_date, so this window must span real usage or the join
    #    misses and the rows were added for nothing.
    # -----------------------------------------------------------------
    ps_usage_df = (
        ps_plan_df
        .filter(F.col("ACAD_PLAN").isNotNull() & (F.trim(F.col("ACAD_PLAN")) != ""))
        .withColumn("declare_dt", F.to_date(F.col("DECLARE_DT")))
        .filter(
            F.col("declare_dt").between(
                F.lit(PS_SCOPE_START).cast("date"),
                F.current_date(),
            )
        )
        .groupBy(F.trim(F.col("ACAD_PLAN")).alias("program_code"))
        .agg(
            F.min("declare_dt").alias("first_used"),
            F.max("declare_dt").alias("last_used"),
        )
    )

    # -----------------------------------------------------------------
    # 4. Build the PS frame.
    #
    #    credential_type must NOT be null: the cohort_exclusion_reason
    #    CASE sends a null credential_type to 'No cohort', which filters
    #    the row straight back out. It is deliberately left null when
    #    neither tier has a peer, rather than guessed — see step 6.
    #
    #    Both date fallbacks are the SAME date, so a programme with no
    #    declares inside the window provably matches no period, and the
    #    value is visibly synthetic rather than looking like a real
    #    retirement date.
    # -----------------------------------------------------------------
    _no_usage = F.lit("1900-01-01").cast("date")

    ps_program_df = (
        ps_missing_df.alias("m")
        .join(ps_norm_s.alias("s"), "aid_prefix", "left")
        .join(ps_norm_r.alias("r"), "aid_prefix", "left")
        .join(ps_usage_df.alias("u"), "program_code", "left")
        .select(
            F.sha2(F.concat(F.lit("PS||"), F.col("m.program_code")), 256).alias("program_key"),
            F.col("m.program_code").alias("program_id"),
            F.col("m.program_code"),
            F.col("m.program_name"),
            F.lit(None).alias("program_area"),
            F.lit(None).alias("program_minimum_coursework_credit"),

            F.coalesce(F.col("dur_s"), F.col("dur_r")).alias("program_duration_years"),
            F.lit(None).alias("program_duration_months"),
            F.lit(None).alias("program_duration_weeks"),

            F.coalesce(F.col("pos_s"), F.col("pos_r"), F.lit(ps_pos_fallback))
             .alias("program_of_study_type"),
            F.lit(None).alias("academic_unit_id"),

            F.coalesce(F.col("u.first_used"), _no_usage).alias("effective_date"),
            F.coalesce(F.col("u.last_used"),  _no_usage).alias("expiration_date"),

            # same rule the Workday path applies in selected_df
            F.when(
                F.coalesce(F.col("u.last_used"), _no_usage) >= F.current_date(),
                F.lit(1)
            ).otherwise(F.lit(0)).alias("is_current"),

            F.lit(None).alias("academic_unit"),
            F.lit(None).alias("cip_code"),

            # must be populated, and in the existing "X - Y" format so
            # credential_level maps off it
            F.coalesce(F.col("cred_s"), F.col("cred_r")).alias("credential_type"),

            F.lit("Post Secondary").alias("academic_level"),
            F.lit(1).alias("is_history"),
        )
    )

    # -----------------------------------------------------------------
    # 5. Align types to joined_df, then union.
    #
    #    Do NOT skip this. silver passes expiration_date through uncast
    #    (selected_df compares it to the string '9999-12-31') while
    #    effective_date is to_date(). The numeric duration and credit
    #    columns carry whatever silver gives them. Hand-casting guesses
    #    one wrong and unionByName fails at runtime, or worse succeeds
    #    after an implicit widen.
    #
    #    Selecting by joined_df.columns also throws a clear error if a
    #    column is missing, rather than silently null-filling it via
    #    allowMissingColumns.
    # -----------------------------------------------------------------
    _target_types = dict(joined_df.dtypes)
    ps_program_df = ps_program_df.select(
        [F.col(c).cast(_target_types[c]).alias(c) for c in joined_df.columns]
    )

    # -----------------------------------------------------------------
    # 6. Report, loudly. Nothing here disappears quietly.
    # -----------------------------------------------------------------
    ps_program_df.cache()

    _n_ps        = ps_program_df.count()
    _n_nocred    = ps_program_df.filter(F.col("credential_type").isNull()).count()
    _n_nousage   = ps_program_df.filter(F.col("effective_date") == _no_usage).count()

    print(f"[dim_program] PS programmes to add              : {_n_ps}")
    print(f"[dim_program] program_of_study_type fallback    : {ps_pos_fallback}")
    print(f"[dim_program] no declares since {PS_SCOPE_START} : {_n_nousage}  (no cohorts - expected)")
    print(f"[dim_program] no credential_type -> 'No cohort' : {_n_nocred}")

    if _n_nocred:
        print("[dim_program] DECIDE THESE WITH RANDY BEFORE GO-LIVE:")
        (ps_program_df
         .filter(F.col("credential_type").isNull())
         .select("program_code", "program_name")
         .orderBy("program_code").show(60, truncate=False))

    joined_df = joined_df.unionByName(ps_program_df)
    ps_program_df.unpersist()

    # ---------------------------------------------------------------
    # Add cohort-supporting columns
    # ---------------------------------------------------------------

    dim_program_df = (
        joined_df
        # Program Lineage Key
        .withColumn(
            "program_lineage_key",
            F.coalesce(
                F.when(
                    F.regexp_extract(F.col("program_code"), r"^(\d+)", 1) != "",
                    F.regexp_extract(F.col("program_code"), r"^(\d+)", 1)
                ),
                F.col("program_code")
            )
        )
        # Enrollment Stage
        .withColumn(
            "enrollment_stage",
            F.when(
                (F.col("program_of_study_type") == "Waitlist") |
                (F.col("program_code").endswith("G")),
                "Waitlist"
            ).when(
                (F.col("program_of_study_type") == "Pre-Program") |
                (F.col("program_code").endswith("Z")),
                "Pre-program"
            ).otherwise("Enrolled")
        )
        # CIP parsing
        .withColumn(
            "cip_code_short",
            F.substring(F.col("cip_code"), 1, 7)
        )
        .withColumn(
            "cip_description",
            F.trim(F.substring(F.col("cip_code"), 11, 200))
        )
        # Credential Level
        .withColumn(
            "credential_level",
            F.when(
                F.col("credential_type") == "Certificate - Certificate",
                "Certificate"
            ).when(
                F.col("credential_type") == "Advanced Technical Certificate - Advanced Technical Certificate",
                "Advanced Technical Certificate"
            ).when(
                F.col("credential_type") == "Technical Diploma - Technical Diploma",
                "Technical Diploma"
            ).when(
                F.col("credential_type") == "Associate Degree - Associate Degree",
                "Associate Degree"
            ).when(
                F.col("credential_type") == "A.A. - Associate of Arts",
                "Associate of Arts"
            ).when(
                F.col("credential_type") == "A.S. - Associate of Science",
                "Associate of Science"
            )
        )
        # Cohort Assignment
        .withColumn(
            "cohort_exclusion_reason",
            F.when(
                F.col("program_of_study_type").like("PROGRAM_OF_STUDY_TYPE-%"),
                "Roll up to parent"
            ).when(
                F.col("enrollment_stage") != "Enrolled",
                "No cohort"
            ).when(
                F.col("program_of_study_type").isin(
                    "Conversion (Undeclared)",
                    "Undeclared_Post_Secondary",
                    "Undeclared_Continuing_Education"
                ),
                "No cohort"
            ).when(
                F.col("program_of_study_type").isin(
                    "Emphasis",
                    "Nursing_Tracks",
                    "Conversion"
                ),
                "Roll up to parent"
            ).when(
                F.col("credential_type").isNull(),
                "No cohort"
            ).otherwise("Direct")
        )
        # Is Cohort Eligible
        .withColumn(
            "is_cohort_eligible",
            F.when(
                F.col("cohort_exclusion_reason") == F.lit("Direct"),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    )

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create workday program data: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# xwalk_program_peoplesoft_workday
#
# Resolves a PeopleSoft ACAD_PLAN code to a Gold program_lineage_key.
#
# WHY THIS EXISTS
#   nb_fact_cohort resolved PeopleSoft declarations by joining
#   PS_PROGRAM_MAP.Workday_Program_Title to dim_program.program_name.
#   Map entries whose title does not exist in dim_program, or exists
#   only on a programme with no valid date range, were silently
#   dropped (~14,000 students, Nursing 105431 among them).
#
# RESOLUTION ORDER — the order is the safety mechanism.
#   1. title_live : the map title matches exactly ONE live programme.
#                   This is today's behaviour, so nothing that works
#                   today changes.
#   2. code_match : the PS code matches exactly ONE live program_code.
#                   Reached when the title matches nothing live OR
#                   matches several (e.g. three distinct programmes are
#                   all called 'Fire Science' — the code separates them).
#   3. ambiguous  : both title and code match more than one live
#                   programme. Never guessed.
#   4. unresolved : neither matches anything live. Expected for the
#                   ~121 PS programmes with no declarations since
#                   PS_SCOPE_START — they have nothing to resolve.
#
#   'live' = effective_date < expiration_date. A row whose dates are
#   equal cannot satisfy the declare-date containment join in
#   nb_fact_cohort and is unusable as a join target.
#
#   program_name is the name of the ROW THAT MATCHED, never a value
#   derived per lineage: a lineage rolls up several programme names
#   (an Associate Degree plus its Full/Part Time and Bridge tracks) and
#   picking among them arbitrarily puts the wrong name on the row.
#   nb_fact_cohort uses this name as program_of_study, which drives the
#   Workday precedence rule, so a wrong name double-counts students.
#
# OWNER: RAS. Rebuilt whenever PS_PROGRAM_MAP or dim_program changes.
# NWTC answers for unresolved codes go in _manual_overrides below.
# =====================================================================

try:
    ps_map_path = abfss_path_builder(
        workspace_id, silver_lakehouse_id, ps_schema_name, "PS_PROGRAM_MAP")
    crosswalk_path = abfss_path_builder(
        workspace_id, silver_lakehouse_id, silver_schema_name,
        "xwalk_program_peoplesoft_workday")

    # -----------------------------------------------------------------
    # NWTC answers go here. ps_acad_plan_code -> program_lineage_key.
    # Wins over automatic resolution. One line per code, with a comment
    # naming who confirmed it and when.
    # -----------------------------------------------------------------
    _manual_overrides = [
        # ("314221", "XXXXXX"),   # pending NWTC — map title is 'Unknown'
        # ("481220", "XXXXXX"),   # pending NWTC — map title is 'Unknown'
        # ("314210", "XXXXXX"),   # pending NWTC — map title is 'Unknown'
    ]

    ps_map_df = spark.read.format("delta").load(ps_map_path)

    live_df = (
        dim_program_df
        .filter(F.col("effective_date") < F.col("expiration_date"))
        .select(
            F.col("program_lineage_key"),
            F.trim(F.col("program_code")).alias("program_code"),
            F.col("program_name"),
            F.col("is_current"),
        )
    )

    # -----------------------------------------------------------------
    # Lookup by NAME. Carries the distinct lineages that name resolves
    # to; one means unambiguous. The matched name is the map title
    # itself, so no name column is needed here.
    # -----------------------------------------------------------------
    by_name_df = (
        live_df.select("program_name", "program_lineage_key").distinct()
        .groupBy("program_name")
        .agg(F.collect_set("program_lineage_key").alias("name_lineages"))
    )

    # -----------------------------------------------------------------
    # Lookup by CODE. Carries the lineages AND the name of that code's
    # own row — current row preferred — so a code_match resolution
    # labels itself correctly.
    # -----------------------------------------------------------------
    _w_code_name = (
        Window.partitionBy("program_code")
              .orderBy(F.col("is_current").desc(),
                       F.col("program_name").asc())
    )
    code_name_df = (
        live_df
        .withColumn("rn", F.row_number().over(_w_code_name))
        .filter(F.col("rn") == 1)
        .select(F.col("program_code"),
                F.col("program_name").alias("code_program_name"))
    )
    by_code_df = (
        live_df.select("program_code", "program_lineage_key").distinct()
        .groupBy("program_code")
        .agg(F.collect_set("program_lineage_key").alias("code_lineages"))
        .join(code_name_df, "program_code", "left")
    )

    # -----------------------------------------------------------------
    # One row per (code, title). A code may carry more than one title in
    # the map, so resolve each pair and reconcile afterwards.
    # -----------------------------------------------------------------
    map_pairs_df = (
        ps_map_df
        .select(
            F.trim(F.col("PeopleSoft_Acad_Plan_Code")).alias("ps_acad_plan_code"),
            F.trim(F.col("Workday_Program_Title")).alias("map_title"),
        )
        .filter(F.col("ps_acad_plan_code").isNotNull()
                & (F.col("ps_acad_plan_code") != ""))
        .distinct()
    )

    _n_name = F.size(F.coalesce(F.col("name_lineages"), F.array()))
    _n_code = F.size(F.coalesce(F.col("code_lineages"), F.array()))

    resolved_pairs_df = (
        map_pairs_df
        .join(by_name_df, F.col("map_title") == F.col("program_name"), "left")
        .join(by_code_df, F.col("ps_acad_plan_code") == F.col("program_code"), "left")
        # 1. unambiguous title   2. unambiguous code   3. nothing
        #    The code is tried whenever the title does not resolve to
        #    exactly one live programme — including when it resolves to
        #    several. That is the fix: an ambiguous title is not a dead
        #    end while the code still identifies the programme.
        .withColumn(
            "pair_lineage",
            F.when(_n_name == 1, F.col("name_lineages").getItem(0))
             .when(_n_code == 1, F.col("code_lineages").getItem(0))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "pair_name",
            F.when(_n_name == 1, F.col("map_title"))
             .when(_n_code == 1, F.col("code_program_name"))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "pair_source",
            F.when(_n_name == 1, F.lit("title_live"))
             .when(_n_code == 1, F.lit("code_match"))
             .when((_n_name > 1) | (_n_code > 1), F.lit("ambiguous"))
             .otherwise(F.lit("unresolved"))
        )
        .select("ps_acad_plan_code", "map_title",
                "pair_lineage", "pair_name", "pair_source")
    )

    # -----------------------------------------------------------------
    # Collapse to one row per code. If a code's titles disagree about
    # the lineage, that is a conflict — flagged, never resolved.
    # -----------------------------------------------------------------
    per_code_df = (
        resolved_pairs_df
        .groupBy("ps_acad_plan_code")
        .agg(
            F.collect_set("pair_lineage").alias("lineages"),
            F.collect_set("pair_name").alias("names"),
            F.collect_set("pair_source").alias("sources"),
            F.collect_set("map_title").alias("map_titles"),
        )
        .withColumn("_lineages", F.expr("filter(lineages, x -> x is not null)"))
        .withColumn("_names",    F.expr("filter(names,    x -> x is not null)"))
        .withColumn("n_lineage", F.size(F.col("_lineages")))
        .withColumn(
            "program_lineage_key",
            F.when(F.col("n_lineage") == 1, F.col("_lineages").getItem(0))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "program_name",
            F.when(F.col("n_lineage") == 1, F.col("_names").getItem(0))
             .otherwise(F.lit(None))
        )
        .withColumn(
            "resolution_source",
            F.when(F.col("n_lineage") > 1, F.lit("conflict_multiple_titles"))
             .when(F.col("n_lineage") == 0,
                   F.when(F.array_contains(F.col("sources"), "ambiguous"),
                          F.lit("ambiguous"))
                    .otherwise(F.lit("unresolved")))
             .when(F.array_contains(F.col("sources"), "title_live"),
                   F.lit("title_live"))
             .otherwise(F.lit("code_match"))
        )
        .drop("_lineages", "_names")
    )

    # -----------------------------------------------------------------
    # Manual overrides win. The name is taken from the override's own
    # live row so it stays consistent with the lineage.
    # -----------------------------------------------------------------
    if _manual_overrides:
        override_df = (
            spark.createDataFrame(_manual_overrides, ["ov_code", "ov_lineage"])
            .join(
                live_df.select("program_lineage_key", "program_name",
                               "is_current").distinct()
                       .withColumn(
                           "rn",
                           F.row_number().over(
                               Window.partitionBy("program_lineage_key")
                                     .orderBy(F.col("is_current").desc(),
                                              F.col("program_name").asc())))
                       .filter(F.col("rn") == 1)
                       .select(F.col("program_lineage_key").alias("ov_lin2"),
                               F.col("program_name").alias("ov_name")),
                F.col("ov_lineage") == F.col("ov_lin2"), "left")
            .select("ov_code", "ov_lineage", "ov_name")
        )
        per_code_df = (
            per_code_df
            .join(override_df,
                  F.col("ps_acad_plan_code") == F.col("ov_code"), "left")
            .withColumn("program_lineage_key",
                        F.coalesce(F.col("ov_lineage"),
                                   F.col("program_lineage_key")))
            .withColumn("program_name",
                        F.coalesce(F.col("ov_name"), F.col("program_name")))
            .withColumn("resolution_source",
                        F.when(F.col("ov_lineage").isNotNull(), F.lit("manual"))
                         .otherwise(F.col("resolution_source")))
            .drop("ov_code", "ov_lineage", "ov_name")
        )

    crosswalk_df = (
        per_code_df
        .select(
            F.col("ps_acad_plan_code"),
            F.col("program_lineage_key"),
            F.col("program_name"),
            F.col("resolution_source"),
            F.concat_ws(" | ", F.col("map_titles")).alias("map_titles"),
            F.when(F.col("program_lineage_key").isNotNull(), F.lit(1))
             .otherwise(F.lit(0)).alias("is_resolved"),
            F.current_timestamp().alias("built_at"),
        )
    )

    audit_df = add_audit_column(crosswalk_df, PIPELINE_NAME)
    audit_df.cache()

    _total      = audit_df.count()
    _resolved   = audit_df.filter(F.col("is_resolved") == 1).count()
    _dupe_codes = (audit_df.groupBy("ps_acad_plan_code").count()
                           .filter(F.col("count") > 1).count())

    print("=== xwalk_program_peoplesoft_workday ===")
    print("codes total     :", _total)
    print("resolved        :", _resolved)
    print("unresolved      :", _total - _resolved)
    print("duplicate codes :", _dupe_codes, " (must be 0)")
    (audit_df.groupBy("resolution_source").count()
             .orderBy(F.desc("count")).show(20, truncate=False))

    # nb_fact_cohort joins this table one-to-one on ps_acad_plan_code.
    # More than one row per code would fan out every PS declaration.
    if _dupe_codes:
        raise Exception(
            f"xwalk: {_dupe_codes} duplicate ps_acad_plan_code rows — "
            "nb_fact_cohort would fan out. Investigate before loading.")

    print("--- not resolved by title_live (review these) ---")
    (audit_df.filter(F.col("resolution_source") != "title_live")
             .select("ps_acad_plan_code", "map_titles",
                     "program_lineage_key", "program_name",
                     "resolution_source")
             .orderBy("resolution_source", "ps_acad_plan_code")
             .show(200, truncate=False))

    final_count  = _total
    rows_read    = final_count
    rows_written = final_count

    audit_df.write.format("delta").mode("overwrite") \
            .option("mergeSchema", True).save(crosswalk_path)
    print("crosswalk written")

    audit_df.unpersist()

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME),
        _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME),
        _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER),
        _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT),
        _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load xwalk_program_peoplesoft_workday table: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = datetime.now()
insert_pipeline_monitoring_record( 
    _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
    "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), rows_read, rows_written, 0,
    start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
    rerun_count=0
)
print("SUCCESS logged")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
