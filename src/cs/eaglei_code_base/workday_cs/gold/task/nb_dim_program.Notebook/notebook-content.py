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

# PARAMETERS CELL ********************

VAR_CS_LIB_NAME = "var_cs_process"
PIPELINE_NAME = "nb_gold_layer_ingestion"
MASTER_RUN_ID = ""
RUN_ID = ""
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
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

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_program"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Program

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

    from pyspark.sql.window import Window

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

    final_df = (
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

    # ---------------------------------------------------------------
    # Add audit columns and write to gold
    # ---------------------------------------------------------------

    final_df = add_audit_column(final_df, PIPELINE_NAME)

    final_df.cache()

    final_count = final_df.count()
    rows_read = final_count
    rows_written = final_count

    dim_program_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_program"
    )

    write_delta_tbl(
        final_df,
        dim_program_gold_path,
        ["program_key"]
    )

    final_df.unpersist()

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_program table: {e}")

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
