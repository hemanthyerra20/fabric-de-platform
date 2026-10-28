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
# META           "id": "08846ef1-977d-4698-b06f-84fdbd834bc6"
# META         },
# META         {
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "f45c2c8e-a6e5-4098-aaf6-567ab6b6ab98",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
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

# PARAMETERS CELL ********************

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
            .withColumn (
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
                #F.col("academic_year"),
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
            F.col("map.period_name").alias ("period_name"),
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

    # dim_academic_period_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    # write_delta_tbl(source_df = dim_academic_period_df,target_table_path = dim_academic_period_path
    #             ,keys = ["academic_period_id"])

    dim_academic_period_df.unpersist()
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    # insert_pipeline_monitoring_record( 
    #     _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
    #     "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
    #     start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
    #     rerun_count=0
    
    raise Exception(f"Failed to create/load dim_academic_period table: {e}")

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

    # dim_college_path = abfss_path_builder(
    #     workspace_id,
    #     gold_lakehouse_id,
    #     gold_schema_name,
    #     "dim_college"
    # )

    # ---------------------------------------------------------------
    # Read source tables
    # ---------------------------------------------------------------

    pos_silver_df = spark.read.format("delta").load(pos_path)
    academic_level_df = spark.read.format("delta").load(academic_level_path)
    cip_code_df = spark.read.format("delta").load(cip_code_path)
    educational_credential_df = spark.read.format("delta").load(educational_credential_path)
    # dim_college_df = spark.read.format("delta").load(dim_college_path)

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
            educational_credential_df.alias("ec"),
            F.col("s.educational_credential_id") == F.col("ec.educational_credential_id"),
            "left"
        )
        # .join(
        #     dim_college_df.alias("c"),
        #     F.col("s.academic_unit_id") == F.col("c.id"),
        #     "left"
        # )
        .selectExpr(
            "s.*",
            "cip.description AS cip_code",
            "ec.description AS credential_type",
            # "c.college_key",
            "al.description AS academic_level"
        )
        .drop(
            "academic_unit_id",
            "academic_level_id",
            "cip_code_id",
            "educational_credential_id"
        )
    )

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

        #  Cohort Assignment
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
    # Add audit columns
    # ---------------------------------------------------------------

    final_df = add_audit_column(final_df, PIPELINE_NAME)

   

    # Read raw academic period dimension
    dim_academic_period_raw_df = dim_academic_period_df

    # Window: one row per academic_period_name, highest weeks_of_instruction
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

    dim_program_df = final_df

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

    # Write only eligible rows to Gold

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
