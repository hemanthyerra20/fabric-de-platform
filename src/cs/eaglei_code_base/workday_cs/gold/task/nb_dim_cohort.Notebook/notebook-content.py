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
# META       "environmentId": "7211437d-61a1-4e64-a477-3428da2a9829",
# META       "workspaceId": "503848ce-d37b-49ba-ab3e-db24b4671fa3"
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

VAR_CS_LIB_NAME = "var_cs_process"
PIPELINE_NAME = "nb_gold_layer_ingestion"
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = ""
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_workspace_id = get_variable(VAR_CS_LIB_NAME, "platform_monitoring_workspace_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Academic Period  —  UNCHANGED

# CELL ********************

try:
    maintained_academic_periods_sil_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "maintained_academic_periods"
    )
    academic_period_date_controls_sil_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "academic_period_date_controls"
    )
    academic_pell_recalc_silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "academic_pell_recalc"
    )
    # ----------------------------------------------------------
    # Read Silver Tables
    # ----------------------------------------------------------
    maintained_academic_periods_df = (
        spark.read
            .format("delta")
            .load(maintained_academic_periods_sil_path)
            .withColumn(
                "period_name",
                F.col("academic_period_name")
            )
            .withColumn(
                "session",
                F.when(
                    F.col("academic_period_name").endswith("Session 1"),
                    F.lit("Session 1")
                )
                .when(
                    F.col("academic_period_name").endswith("Session 2"),
                    F.lit("Session 2")
                )
                .when(
                    F.col("academic_period_name").contains("J Term"),
                    F.lit("J Term")
                )
            )
            .withColumn(
                "academic_period_name",
                F.when(
                    F.col("academic_period_name").endswith("Session 1"),
                    F.trim(F.regexp_replace("academic_period_name", "Session 1$", ""))
                )
                .when(
                    F.col("academic_period_name").endswith("Session 2"),
                    F.trim(F.regexp_replace("academic_period_name", "Session 2$", ""))
                )
                .when(
                    F.col("academic_period_name").contains("J Term"),
                    F.trim(F.regexp_replace("academic_period_name", "J Term", "Spring"))
                )
                .otherwise(F.col("academic_period_name"))
            )
            .withColumn(
                "current_flag",
                F.when(
                    (F.current_date() >= F.col("academic_period_start_date")) &
                    (F.current_date() <= F.col("academic_period_end_date")),
                    F.lit(1)
                ).otherwise(F.lit(0))
            )
    )
    academic_period_date_controls_df = (
        spark.read
            .format("delta")
            .load(academic_period_date_controls_sil_path)
            .select(
                "academic_period_id",
                "date_control_id",
                "control_date"
            )
    )
    academic_pell_recalc_df = (
        spark.read
            .format("delta")
            .load(academic_pell_recalc_silver_path)
            .select(
                F.trim(F.col("academicperiod")).alias("academic_period"),
                F.trim(
                    F.regexp_replace(
                        F.col("academic_year"),
                        "\\s*Academic\\s+Year$",
                        ""
                        )
                    ).alias("academic_year"),
                F.to_date(
                    F.col("pell_recalculation_date"),
                    "MM/dd/yyyy"
                ).alias("pell_recalculation_date")
            )
            .dropDuplicates(["academic_period"])
    )

    # ----------------------------------------------------------
    # Derive Registration Dates
    # ----------------------------------------------------------
    registration_dates_df = (
        academic_period_date_controls_df
            .groupBy("academic_period_id")
            .agg(
                F.max(
                    F.when(
                        F.col("date_control_id") == "ENROLLMENT_WINDOW_START",
                        F.col("control_date")
                    )
                ).alias("registration_open_date"),
                F.max(
                    F.when(
                        F.col("date_control_id") == "ENROLLMENT_WINDOW_END",
                        F.col("control_date")
                    )
                ).alias("registration_close_date")
            )
    )
    # ----------------------------------------------------------
    # Build Dimension
    # ----------------------------------------------------------
    maintained_academic_periods_silver_df = (
        maintained_academic_periods_df.alias("map")
        .join(
            registration_dates_df.alias("rd"),
            F.col("map.id") == F.col("rd.academic_period_id"),
            "inner"
        )
        .join(
            academic_pell_recalc_df.alias("pr"),
            F.trim(F.col("map.academic_period_name")) == F.col("pr.academic_period"),
            "left"
        )
        .select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("map.id").cast("string"), F.lit(""))
                ),
                256
            ).alias("academic_period_key"),
            F.col("map.id").alias("academic_period_id"),
            F.col("map.period_name").alias("period_name"),
            F.col("map.academic_period_name").alias("academic_period_name"),
            F.col("map.session").alias("session"),
            F.to_date("map.academic_period_start_date", "yyyy-MM-dd").alias("period_start_date"),
            F.to_date("map.academic_period_end_date", "yyyy-MM-dd").alias("period_end_date"),
            F.col("map.academic_period_weeks_of_instruction").alias("weeks_of_instruction"),
            F.col("map.academic_calendar_id").alias("academic_calendar_id"),
            F.col("pr.academic_year").alias("academic_year"),
            F.col("rd.registration_open_date"),
            F.col("rd.registration_close_date"),
            F.col("pr.pell_recalculation_date").alias("pell_recalculation_date"),
            F.col("map.current_flag").alias("current_flag")
        )
        .filter(F.col("academic_period_id").isNotNull())
    )

    # ----------------------------------------------------------
    # Add Audit Columns
    # ----------------------------------------------------------
    dim_academic_period_df = add_audit_column(
        maintained_academic_periods_silver_df,
        PIPELINE_NAME
    )

    dim_academic_period_df.cache()

    final_count = dim_academic_period_df.count()
    rows_read = final_count
    rows_written = final_count

    dim_academic_period_df.unpersist()
except Exception as e:
    end_time = datetime.now()
    raise Exception(f"Failed to create/load dim_academic_period table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Cohort  —  CHANGED
# 
# Two changes from the previous version:
# 
# 1. **`dim_program` is read from Gold** instead of being rebuilt from silver.
#    The inline rebuild was a second copy of `nb_dim_program.ipynb` that had
#    already drifted (it never joined `academic_unit_references`). Reading Gold
#    means the 138 PeopleSoft historical programmes — and any future change to
#    the cohort exclusion rules — arrive here automatically instead of having to
#    be applied in two places.
# 
# 2. **PS programmes are floored at Fall 2013.** Their effective dates come from
#    the earliest observed `DECLARE_DT`, which reaches back to the 1970s. Left
#    alone the cross-join would generate five decades of cohorts. Workday rows
#    are untouched, so existing 2011 and 2012 cohorts are unaffected.
# 
# **PRECONDITION:** `nb_dim_program` must run before this notebook in the pipeline.

# CELL ********************

try:
    # ---------------------------------------------------------------
    # Read dim_program from Gold
    #
    # CHANGED: this notebook previously rebuilt dim_program from the
    # silver program_of_study table - a second copy of the logic in
    # nb_dim_program.ipynb that had already drifted (no academic_unit
    # join here). Reading Gold means the PS historical programmes, and
    # every future change to the cohort exclusion rules, arrive
    # automatically instead of needing to be applied twice.
    #
    # PRECONDITION: nb_dim_program must run before nb_dim_cohort.
    # ---------------------------------------------------------------

    dim_program_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_program"
    )

    dim_program_df = spark.read.format("delta").load(dim_program_gold_path)

    # ---------------------------------------------------------------
    # Academic period dimension
    #
    # One row per academic_period_name, highest weeks_of_instruction
    # wins. This collapses Session 1 / Session 2 / J Term onto the
    # main term.
    # ---------------------------------------------------------------

    dim_academic_period_raw_df = dim_academic_period_df

    window_spec = (
        Window
        .partitionBy("academic_period_name")
        .orderBy(F.col("weeks_of_instruction").desc())
    )

    dim_academic_period_df = (
        dim_academic_period_raw_df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # ---------------------------------------------------------------
    # Floor PeopleSoft programmes at Fall 2013
    #
    # NEW. PS programme effective dates are the earliest observed
    # DECLARE_DT, which reaches back to the 1970s. Without a floor the
    # cross-join generates five decades of cohorts nobody asked for.
    # NWTC's scope is 13 years, so PS rows start at Fall 2013.
    #
    # Applied here and NOT in dim_program, so the dimension keeps
    # telling the truth about when each programme was really used.
    # Workday rows are untouched - their cohorts stay exactly as they
    # are today, including 2011 and 2012.
    # ---------------------------------------------------------------

    PS_COHORT_FLOOR_PERIOD = "2013 Fall"

    _floor_row = (
        dim_academic_period_df
        .filter(F.col("academic_period_name") == PS_COHORT_FLOOR_PERIOD)
        .select("period_start_date")
        .collect()
    )

    if not _floor_row:
        raise Exception(
            f"PS cohort floor period '{PS_COHORT_FLOOR_PERIOD}' not found in "
            f"dim_academic_period - cannot bound the PS cohort range."
        )

    ps_cohort_floor = _floor_row[0]["period_start_date"]
    print(f"[dim_cohort] PS cohorts floored at {PS_COHORT_FLOOR_PERIOD} ({ps_cohort_floor})")

    if "is_history" in dim_program_df.columns:
        ps_program_rows = dim_program_df.filter(F.col("is_history") == 1).count()
        print(f"[dim_cohort] PS programme rows found in dim_program: {ps_program_rows}")

        dim_program_df = dim_program_df.withColumn(
            "effective_date",
            F.when(
                F.col("is_history") == 1,
                F.greatest(F.col("effective_date"), F.lit(ps_cohort_floor))
            ).otherwise(F.col("effective_date"))
        )
    else:
        print(
            "[dim_cohort] WARNING: dim_program has no is_history column. "
            "PS programmes are not loaded yet, or the ALTER TABLE has not "
            "run. Building Workday-only cohorts."
        )

    # ---------------------------------------------------------------
    # Program date range (including eligibility flag)
    # ---------------------------------------------------------------

    program_date_range_df = (
        dim_program_df
        .groupBy("program_name", "program_lineage_key")
        .agg(
            F.min("effective_date").alias("min_effective_date"),
            F.max("expiration_date").alias("max_expiration_date"),
            F.max("is_cohort_eligible").alias("is_cohort_eligible"),
            F.max("program_key").alias("program_key")
        )
    )

    # ---------------------------------------------------------------
    # Program cohort records
    # ---------------------------------------------------------------

    program_cohort_df = (
        program_date_range_df.alias("p")
        .crossJoin(dim_academic_period_df.alias("ap"))

        # Cohort type
        .withColumn("cohort_type", F.lit("Program"))
        .withColumn(
            "year_code",
            F.regexp_extract(F.col("ap.academic_period_name"), r"^(\d{4})", 1)
        )

        # Term code
        .withColumn(
            "term_code",
            F.when(F.col("ap.academic_period_name").endswith("Fall"), "FA")
             .when(F.col("ap.academic_period_name").endswith("Spring"), "SP")
             .when(F.col("ap.academic_period_name").endswith("Summer"), "SU")
        )

        # Cohort code
        .withColumn(
            "cohort_code",
            F.concat(
                F.lit("PROG-"),
                F.col("p.program_lineage_key"),
                F.lit("-"),
                F.col("year_code"),
                F.col("term_code")
            )
        )

        # Cohort name
        .withColumn(
            "cohort_name",
            F.concat_ws(
                "-",
                F.col("p.program_name"),
                F.col("ap.academic_period_name")
            )
        )

        # Is within program dates
        .withColumn(
            "is_within_program_dates",
            F.when(
                (F.col("ap.period_start_date") >= F.col("p.min_effective_date")) &
                (F.col("ap.period_start_date") <= F.col("p.max_expiration_date")),
                F.lit(1)
            ).otherwise(F.lit(0))
        )

        .select(
            F.col("p.program_name"),
            F.col("p.program_lineage_key"),
            F.col("p.min_effective_date"),
            F.col("p.max_expiration_date"),
            F.col("ap.academic_period_name"),
            F.col("ap.period_start_date"),
            F.col("ap.period_end_date"),
            F.col("cohort_type"),
            F.col("cohort_code"),
            F.col("cohort_name"),
            F.col("is_within_program_dates"),
            F.col("p.is_cohort_eligible")
        )
    )

    # ---------------------------------------------------------------
    # Institutional cohort records (one row per academic period)
    # ---------------------------------------------------------------

    institutional_df = (
        dim_academic_period_df.alias("ap")
        .select(
            F.lit(None).cast("string").alias("program_name"),
            F.lit(None).cast("string").alias("program_lineage_key"),
            F.lit(None).cast("date").alias("min_effective_date"),
            F.lit(None).cast("date").alias("max_expiration_date"),
            F.col("ap.academic_period_name"),
            F.col("ap.period_start_date"),
            F.col("ap.period_end_date"),
            F.lit("Institutional").alias("cohort_type"),
            F.concat(
                F.lit("INST-"),
                F.col("ap.academic_period_id")
            ).alias("cohort_code"),
            F.col("ap.academic_period_name").alias("cohort_name"),
            F.lit(1).alias("is_within_program_dates"),
            F.lit(1).alias("is_cohort_eligible")
        )
    )

    # ---------------------------------------------------------------
    # Combine Program + Institutional cohorts
    # ---------------------------------------------------------------

    cohort_df = program_cohort_df.unionByName(institutional_df)

    # ---------------------------------------------------------------
    # Add surrogate key
    # ---------------------------------------------------------------

    final_df = cohort_df.withColumn(
        "cohort_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("program_lineage_key"), F.lit("INST")),
                F.col("academic_period_name"),
                F.col("cohort_type")
            ),
            256
        )
    )

    # Keep only valid rows
    cohort_df = (
        final_df
        .filter(F.col("is_within_program_dates") == 1)
        .drop("is_within_program_dates")
    )

    # ---------------------------------------------------------------
    # Add audit columns
    # ---------------------------------------------------------------

    cohort_df = add_audit_column(cohort_df, PIPELINE_NAME)

    # ---------------------------------------------------------------
    # Keep only cohort-eligible rows
    # ---------------------------------------------------------------

    eligible_df = cohort_df.filter(F.col("is_cohort_eligible") == 1).drop("is_cohort_eligible")

    # ---------------------------------------------------------------
    # Write to Gold
    # ---------------------------------------------------------------

    dim_cohort_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_cohort"
    )

    write_delta_tbl(
        eligible_df,
        dim_cohort_gold_path,
        ["cohort_key"]
    )
except Exception as e:
    raise Exception(f"Failed to create/load dim_cohort table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
