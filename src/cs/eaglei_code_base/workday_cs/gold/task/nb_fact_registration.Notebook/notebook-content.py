# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d899371c-8729-4adc-a55d-da6e6520153a",
# META       "default_lakehouse_name": "lh_cs_gold",
# META       "default_lakehouse_workspace_id": "bd8e5690-a001-4896-8ed1-ae46d585f7b6",
# META       "known_lakehouses": [
# META         {
# META           "id": "d899371c-8729-4adc-a55d-da6e6520153a"
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

# MARKDOWN ********************

# ### abfss_path_builder usage by layer
# 
# #### Silver layer
# 
# - `std_course_reg_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_registration")`
# - `transfer_credit_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_transfer_credit")`
# 
# #### Gold layer
# 
# - `dim_program_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")`
# - `students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")`
# - `course_section_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_sections")`
# - `fact_reg_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_registration")`
# - `dim_student_grade_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_grade")`
# - `dim_aca_period = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")`
# - `dim_college_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")`
# - `dim_campus_loc_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_campus_location")`
# - `dim_cs_taxonomy_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_section_taxonomy")`
# - `pl_dual_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "ref_peoplesoft_dual_credit")`
# - `ref_std_grade_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "ref_student_grade")`


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
PIPELINE_NAME = "ManualRun"
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
bronze_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_bronze_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_program_enrollment"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Fact Registration

# CELL ********************

try:
    # ------------------------------------------------------------------
    # 1) Resolve Lakehouse paths
    # ------------------------------------------------------------------
    std_course_reg_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_registration")
    transfer_credit_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_transfer_credit")
    students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    course_section_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_sections")
    fact_reg_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_registration")
    dim_student_grade_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_grade")
    dim_aca_period = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_college_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")
    dim_campus_loc_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_campus_location")
    dim_cs_taxonomy_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_section_taxonomy")
    pl_dual_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "ref_peoplesoft_dual_credit")
    ref_std_grade_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "ref_student_grade")
    cs_acad_unit_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_section_academic_unit_data")
    dim_program_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")

    # ------------------------------------------------------------------
    # 2) Load dimension and reference data
    # ------------------------------------------------------------------
    program_df = (
        spark.read.format("delta").load(dim_program_path)
        .select(
            "program_key",
            "program_name",
            "effective_date",
            "expiration_date",
            "academic_unit",
        )
    )

    ref_std_df = (
        spark.read.format("delta").load(ref_std_grade_path)
        .withColumn("calc_term", F.regexp_extract("term", r"^([^ ]+)", 1))
        .withColumn("calc_term_year", F.regexp_extract("term", r"^[^ ]+ (.*)$", 1))
        .withColumn(
            "course_section_id",
            F.concat(
                F.col("catalog_#").cast("string"),
                F.lit("_"),
                F.col("class_#").cast("string"),
                F.lit("_"),
                F.col("calc_term_year"),
                F.lit("_"),
                F.col("calc_term"),
            ),
        )
        .select("student_id", "course_section_id", "original_grade", "current_grade")
    )

    cs_acad_unit_df = (
        spark.read.format("delta").load(cs_acad_unit_path)
        .select("course_section_key", "academic_unit_id", "au_offering_percent")
    )

    pl_result = (
        spark.read.format("delta").load(pl_dual_path)
        .select("student_key", "academic_period_key")
        .drop_duplicates()
    )

    window_spec = Window.partitionBy("course_section_key").orderBy(F.col("code"))

    dim_cs_taxonomy_df = (
        spark.read.format("delta").load(dim_cs_taxonomy_path)
        .filter(
            F.col("code").isin([
                "1A",
                "1B",
                "1E",
                "8A",
                "8B",
                "9B",
                "9C",
                "9D",
                "9G",
                "9H",
                "9K",
            ])
        )
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .select("course_section_key")
    )

    dim_acad_period_df = spark.read.format("delta").load(dim_aca_period)

    transfer_credit_df = (
        spark.read.format("delta").load(transfer_credit_path).select(
            "student_id", "course", "credits"
        )
    )

    grade_df = (
        spark.read.format("delta").load(dim_student_grade_path).withColumn(
            "grade", F.lower(F.col("grade"))
        )
    )

    dim_student_df = (
        spark.read.format("delta").load(students_path)
        .filter(F.col("is_current") == "1")
        .select("student_key", "student_id")
    )

    dim_clg_df = (
        spark.read.format("delta").load(dim_college_path).select(
            "college_key", "id", "name"
        )
    )

    dim_campus_loc_df = (
        spark.read.format("delta").load(dim_campus_loc_path).select(
            "campus_location_key", "name", "primary_campus_location"
        )
    )

    dim_course_df = (
        spark.read.format("delta").load(course_section_path).select(
            "course_section_key", "course_section_id"
        )
    )

    dim_course_section_df = dim_course_df.alias("CO").selectExpr(
        "CO.course_section_key", "CO.course_section_id"
    )

    course_reg_df = spark.read.format("delta").load(std_course_reg_path)

    # ------------------------------------------------------------------
    # 3) Clean campus_location and course_section_id
    # ------------------------------------------------------------------
    campus_list = F.transform(
        F.split(F.col("campus_location"), ";"), lambda x: F.trim(x)
    )
    keyword = F.regexp_extract(F.col("location_code"), "-(.*)", 1)
    matches = F.filter(campus_list, lambda x: x.contains(keyword))
    selected_campus = F.when(F.size(matches) > 0, matches.getItem(0)).otherwise(
        campus_list.getItem(0)
    )

    course_registration_base_df = (
        course_reg_df.withColumn(
            "calc_course_section_id",
            F.when(
                F.col("course_section_id").endswith("Session_1"),
                F.trim(F.regexp_replace("course_section_id", "_Session_1$", "")),
            )
            .when(
                F.col("course_section_id").endswith("Session_2"),
                F.trim(F.regexp_replace("course_section_id", "_Session_2$", "")),
            )
            .when(
                F.col("course_section_id").contains("J_Term"),
                F.trim(F.regexp_replace("course_section_id", "_J_Term", "Spring")),
            )
            .otherwise(F.col("course_section_id")),
        ).withColumn(
            "campus_location",
            F.when(
                F.col("campus_location").isNotNull()
                & (F.col("campus_location") != ""),
                selected_campus,
            ).otherwise(F.col("campus_location")),
        )
    )

    # ------------------------------------------------------------------
    # 4) Join to reference grades and normalize
    # ------------------------------------------------------------------
    std_course_reg_df = (
        course_registration_base_df.alias("SRC")
        .join(
            ref_std_df.alias("RST"),
            (
                F.upper(F.col("SRC.calc_course_section_id"))
                == F.upper(F.col("RST.course_section_id"))
            )
            & (
                F.upper(F.col("RST.current_grade"))
                == F.upper(F.col("SRC.grade"))
            )
            & ((F.col("RST.student_id")) == (F.col("SRC.student_id"))),
            "left",
        )
        .select(
            F.col("SRC.registration_key"),
            F.col("SRC.academic_period"),
            F.col("SRC.course_section_id"),
            F.col("SRC.academic_record_status"),
            F.col("SRC.student_id"),
            F.to_timestamp(
                F.col("SRC.graded_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
            ).alias("graded_date"),
            F.col("SRC.grading_scheme"),
            F.col("SRC.grading_basis"),
            F.to_timestamp(
                F.col("SRC.registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
            ).alias("registered_date"),
            F.to_timestamp(
                F.col("SRC.waitlist_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
            ).alias("waitlist_date"),
            F.to_timestamp(
                F.col("SRC.withdrawal_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
            ).alias("withdrawal_date"),
            F.to_timestamp(
                F.col("SRC.drop_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
            ).alias("drop_date"),
            F.col("SRC.registration_status"),
            F.col("SRC.credits"),
            F.col("SRC.earned_credits"),
            F.col("SRC.original_credits"),
            F.col("SRC.course_listing"),
            F.col("SRC.academic_unit"),
            F.when(
                (F.col("SRC.location_code") == "")
                | (F.col("SRC.campus_location") == ""),
                "Unknown",
            )
            .when(
                (F.col("SRC.location_code").isNull())
                | (F.col("SRC.campus_location").isNull()),
                "Unknown",
            )
            .when(
                (F.lower(F.col("SRC.location_code")).like("%online%"))
                | (F.lower(F.col("SRC.campus_location")).like("%online%")),
                "Online",
            )
            .otherwise(F.col("SRC.location_code"))
            .alias("location_code"),
            F.when(
                (F.col("SRC.location_code") == "")
                | (F.col("SRC.campus_location") == ""),
                "Unknown",
            )
            .when(
                (F.col("SRC.location_code").isNull())
                | (F.col("SRC.campus_location").isNull()),
                "Unknown",
            )
            .when(
                (F.lower(F.col("SRC.location_code")).like("%online%"))
                | (F.lower(F.col("SRC.campus_location")).like("%online%")),
                "Online",
            )
            .otherwise(F.col("SRC.campus_location"))
            .alias("campus_location"),
            F.lower(
                F.coalesce(F.col("RST.original_grade"), F.col("SRC.grade"))
            ).alias("grade"),
            F.col("SRC.primary_program_of_study"),
            F.col("SRC.program_of_study"),
            F.coalesce(
                F.to_date(
                    F.to_timestamp(
                        F.col("SRC.registered_date"),
                        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                    )
                ),
                F.to_date(
                    F.to_timestamp(
                        F.col("SRC.waitlist_date"),
                        "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                    )
                ),
            ).alias("converted_reg_date"),
        )
    )

    # ------------------------------------------------------------------
    # 5) Join to other dimensions and facts
    # ------------------------------------------------------------------
    joined_df = (
        std_course_reg_df.alias("CR")
        .join(
            transfer_credit_df.alias("TC"),
            (
                (F.col("CR.student_id") == F.col("TC.student_id"))
                & (F.col("CR.course_listing") == F.col("TC.course"))
            ),
            "left",
        )
        .join(
            dim_student_df.alias("STD"),
            (F.col("CR.student_id") == F.col("STD.student_id")),
            "left",
        )
        .join(
            dim_clg_df.alias("CLG"),
            (F.col("CLG.name") == F.col("CR.academic_unit")),
            "left",
        )
        .join(
            dim_course_section_df.alias("CS"),
            (F.col("CR.course_section_id") == F.col("CS.course_section_id")),
            "left",
        )
        .join(
            dim_cs_taxonomy_df.alias("CST"),
            (F.col("CST.course_section_key") == F.col("CS.course_section_key")),
            "left",
        )
        .join(
            dim_campus_loc_df.alias("LOC"),
            (
                (F.col("CR.location_code") == F.col("LOC.name"))
                & (F.col("CR.campus_location") == F.col("LOC.primary_campus_location"))
            ),
            "left",
        )
        .join(
            grade_df.alias("GD"),
            (
                (F.col("GD.grade") == F.col("CR.grade"))
                & (F.col("GD.scheme_id") == F.col("CR.grading_scheme"))
            ),
            "left",
        )
        .join(
            dim_acad_period_df.alias("AP"),
            F.col("CR.academic_period") == F.col("AP.period_name"),
            "left",
        )
        .join(
            pl_result.alias("PST"),
            (
                (F.col("PST.student_key") == F.col("STD.student_key"))
                & (F.col("PST.academic_period_key")
                   == F.col("AP.academic_period_key"))
            ),
            "left",
        )
        .selectExpr(
            "CR.registration_key",
            "CR.primary_program_of_study",
            "CR.converted_reg_date",
            "STD.student_key",
            "AP.academic_period_key",
            "GD.grade_key AS grade_key",
            "CS.course_section_key",
            "CLG.college_key",
            "LOC.campus_location_key AS campus_location_key",
            "CR.academic_record_status",
            "CR.graded_date",
            "CR.grading_basis",
            "CR.registered_date",
            "CR.waitlist_date",
            "CR.withdrawal_date",
            "CR.drop_date",
            "CR.registration_status",
            "CR.credits",
            "CR.earned_credits",
            "CR.original_credits",
            "CR.program_of_study",
            "TC.credits AS transfer_credits",
            "CASE WHEN CST.course_section_key IS NOT NULL OR PST.student_key IS NOT NULL THEN 1 ELSE 0 END AS is_dual_credit_flag",
            "CR.academic_unit",
        )
    )

    au_joined_df = (
        joined_df.alias("LH")
        .join(
            cs_acad_unit_df.alias("CAUD"),
            (F.col("CAUD.course_section_key") == F.col("LH.course_section_key")),
            "left",
        )
        .join(
            dim_clg_df.alias("CACL"),
            (F.col("CAUD.academic_unit_id") == F.col("CACL.id")),
            "left",
        )
        .selectExpr(
            "LH.*",
            "CACL.college_key AS academic_unit_offering_key",
            "CAUD.au_offering_percent",
        )
    )

    program_joined_df = (
        au_joined_df.alias("CR")
        .join(
            program_df.alias("PG"),
            (
                (F.col("PG.program_name") == F.col("CR.primary_program_of_study"))
                & (F.col("PG.effective_date") <= F.col("CR.converted_reg_date"))
                & (F.col("PG.expiration_date") >= F.col("CR.converted_reg_date"))
                # NOTE: academic_unit match should be equality, not ">="
                & (F.col("PG.academic_unit") == F.col("CR.academic_unit"))
            ),
            "left",
        )
        .selectExpr(
            "CR.registration_key",
            "PG.program_key AS primary_program_of_study_key",
            "CR.student_key",
            "CR.academic_period_key",
            "CR.grade_key AS grade_key",
            "CR.course_section_key",
            "CR.college_key",
            "CR.academic_unit_offering_key",
            "CR.campus_location_key",
            "CR.academic_record_status",
            "CR.graded_date",
            "CR.grading_basis",
            "CR.registered_date",
            "CR.waitlist_date",
            "CR.withdrawal_date",
            "CR.drop_date",
            "CR.registration_status",
            "CR.au_offering_percent",
            "CAST(CR.credits AS DECIMAL(18,5)) AS credits",
            "CAST(CR.earned_credits AS DECIMAL(18,5)) AS earned_credits",
            "CAST(CR.original_credits AS DECIMAL(18,5)) AS original_credits",
            "CAST(CR.transfer_credits AS DECIMAL(18,5)) AS transfer_credits",
            "CR.is_dual_credit_flag",
            "row_number() over(partition by CR.registration_key order by PG.expiration_date desc) AS RN",
        )
        .filter(F.col("RN") == 1)
        .drop("RN")
    )

    # ------------------------------------------------------------------
    # 6) PeopleSoft override logic (unchanged from original)
    # ------------------------------------------------------------------
    PS_CUTOFF_STRM = "2255"  # Summer 2025 = last PeopleSoft term (Trista)
    AP_START_COL = "period_start_date"  # TODO: confirm start-date column
    UNDECLARED = ["000000", "000001"]
    OVERRIDE_MODE = "undeclared_only"  # "undeclared_only" | "all"

    car_term_path = abfss_path_builder(
        workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_STDNT_CAR_TERM"
    )
    acad_plan_path = abfss_path_builder(
        workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_ACAD_PLAN"
    )
    xwalk_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "xwalk_program_peoplesoft_workday",
    )

    # 6.1 term start + PeopleSoft STRM per academic period
    ap_strm_df = (
        dim_acad_period_df.select(
            "academic_period_key",
            F.to_date(F.col(AP_START_COL)).alias("term_start"),
        )
        .withColumn(
            "season",
            F.when(F.month("term_start") <= 4, F.lit("2"))
            .when(F.month("term_start") <= 7, F.lit("5"))
            .otherwise(F.lit("8")),
        )
        .withColumn(
            "strm",
            F.concat(
                F.lit("2"),
                F.lpad((F.year("term_start") % 100).cast("string"), 2, "0"),
                F.col("season"),
            ),
        )
        .filter(F.col("strm") <= PS_CUTOFF_STRM)
        .select("academic_period_key", "term_start", "strm")
    )

    # 6.2 registrations in PeopleSoft-era terms
    dim_prog_full_df = (
        spark.read.format("delta").load(dim_program_path).select(
            "program_key", "program_lineage_key", "effective_date", "expiration_date"
        )
    )

    reg_ps_df = (
        program_joined_df.select(
            "registration_key", "student_key", "academic_period_key", "primary_program_of_study_key"
        )
        .join(ap_strm_df, "academic_period_key", "inner")
        .join(dim_student_df, "student_key", "inner")  # student_id = EMPLID
        .join(
            dim_prog_full_df.select(
                F.col("program_key").alias("primary_program_of_study_key"),
                F.col("program_lineage_key").alias("wd_lineage_key"),
            ),
            "primary_program_of_study_key",
            "left",
        )
        .dropDuplicates(["registration_key"])
    )

    if OVERRIDE_MODE == "undeclared_only":
        reg_ps_df = reg_ps_df.filter(
            F.col("wd_lineage_key").isNull() | F.col("wd_lineage_key").isin(UNDECLARED)
        )

    # 6.3 career number active for the student in that term
    car_term_df = (
        spark.read.format("delta").load(car_term_path)
        .filter(F.col("STRM") <= PS_CUTOFF_STRM)
        .select(
            F.col("EMPLID").alias("student_id"),
            F.col("STRM").alias("strm"),
            "ACAD_CAREER",
            "STDNT_CAR_NBR",
        )
        .dropDuplicates(["student_id", "strm"])
    )

    # 6.4 primary plan in effect at term start
    acad_plan_df = (
        spark.read.format("delta").load(acad_plan_path)
        .filter(F.trim(F.col("EMPLID")) != "")
        .select(
            F.col("EMPLID").alias("student_id"),
            "ACAD_CAREER",
            "STDNT_CAR_NBR",
            "EFFDT",
            "EFFSEQ",
            "ACAD_PLAN",
            "PLAN_SEQUENCE",
        )
    )

    w_plan = Window.partitionBy("registration_key").orderBy(
        F.col("EFFDT").desc(),
        F.col("EFFSEQ").desc(),
        F.col("PLAN_SEQUENCE").asc(),
    )

    ps_plan_df = (
        reg_ps_df.select("registration_key", "student_id", "strm", "term_start")
        .join(car_term_df, ["student_id", "strm"], "inner")
        .join(acad_plan_df, ["student_id", "ACAD_CAREER", "STDNT_CAR_NBR"], "inner")
        .filter(F.to_date("EFFDT") <= F.col("term_start"))
        .withColumn("rn", F.row_number().over(w_plan))
        .filter((F.col("rn") == 1) & (~F.col("ACAD_PLAN").isin(UNDECLARED)))
        .select("registration_key", "term_start", "ACAD_PLAN")
    )

    # 6.5 plan -> lineage -> program_key valid at term start
    xwalk_df = (
        spark.read.format("delta").load(xwalk_path)
        .filter(F.col("is_resolved") == 1)
        .select(
            F.col("ps_acad_plan_code").alias("ACAD_PLAN"),
            "program_lineage_key",
            F.col("program_name").alias("xwalk_program_name"),
        )
        .dropDuplicates(["ACAD_PLAN"])
    )

    dim_prog_named_df = (
        spark.read.format("delta").load(dim_program_path).select(
            "program_key",
            "program_lineage_key",
            "program_name",
            "effective_date",
            "expiration_date",
        )
    )

    name_match = F.lower(F.trim(F.col("program_name"))) == F.lower(
        F.trim(F.col("xwalk_program_name"))
    )

    w_ver = Window.partitionBy("registration_key").orderBy(
        F.when(name_match, 0).otherwise(1),
        F.col("effective_date").desc(),
        F.col("program_key"),
    )

    ps_program_df = (
        ps_plan_df.join(xwalk_df, "ACAD_PLAN", "inner")
        .join(dim_prog_named_df, "program_lineage_key", "inner")
        .filter(
            (F.col("effective_date") <= F.col("term_start"))
            & (F.col("expiration_date") >= F.col("term_start"))
        )
        .withColumn("is_name_match", F.when(name_match, 1).otherwise(0))
        .withColumn("rn", F.row_number().over(w_ver))
        .filter(F.col("rn") == 1)
    )

    print(
        "PS assignments without name match:",
        ps_program_df.filter(F.col("is_name_match") == 0).count(),
    )

    ps_program_df = ps_program_df.select(
        "registration_key", F.col("program_key").alias("ps_program_key")
    )

    # 6.6 apply override + audit columns
    final_reg_df = (
        program_joined_df.join(ps_program_df, "registration_key", "left")
        .withColumn(
            "workday_program_of_study_key", F.col("primary_program_of_study_key")
        )
        .withColumn(
            "is_ps_program",
            F.when(F.col("ps_program_key").isNotNull(), F.lit(1))
            .otherwise(F.lit(0))
            .cast("int"),
        )
        .withColumn(
            "primary_program_of_study_key",
            F.coalesce(F.col("ps_program_key"), F.col("primary_program_of_study_key")),
        )
        .drop("ps_program_key")
    )

    # grain guard: one row per registration
    dup_before = (
        program_joined_df.groupBy("registration_key").count().filter("count > 1").count()
    )
    dup_after = (
        final_reg_df.groupBy("registration_key").count().filter("count > 1").count()
    )
    print(f"Duplicate registration_keys - before: {dup_before}, after: {dup_after}")
    if dup_after > dup_before:
        raise Exception(
            "PeopleSoft program override added "
            f"{dup_after - dup_before} duplicate registration_keys"
        )

    # ------------------------------------------------------------------
    # 7) Add audit columns and write Delta
    # ------------------------------------------------------------------
    fact_reg_audit_df = add_audit_column(final_reg_df, PIPELINE_NAME)

    final_count = fact_reg_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    # NOTE: Target table *must* include workday_program_of_study_key & is_ps_program
    # for merge to succeed, because write_delta_tbl/upsert_data uses them
    write_delta_tbl(fact_reg_audit_df, fact_reg_path, ["registration_key"])

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID),
        _safe_str(RUN_ID),
        _safe_str(PIPELINE_NAME),
        _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN),
        _safe_str(SOURCE_NAME),
        _safe_str(ACTIVITY_NAME),
        "failed",
        _safe_str(LEVEL),
        _safe_str(LAYER),
        _safe_str(source_table),
        _safe_str(target_table),
        0,
        0,
        0,
        start_time,
        end_time,
        str(e),
        str(e),
        _safe_str(ENVIRONMENT),
        _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID),
        _safe_str(PIPELINE_NAME),
        rerun_count=0,
    )
    raise Exception(f"Failed to create/load fact_registration table: {e}")

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
