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
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data pipeline overview
# 
# This notebook builds the **fact_cohort_retention** model from silver and gold layer tables.
# 
# ## Table hierarchy
# 
# ### Silver layer (`lh_cs_silver.workday_cs`)
# - `course_registration` – raw/enhanced student course registrations
# - `student_transfer_credit` – external transfer-credit records
# - `program_of_study_all` – declared programs of study and completions
# 
# ### Gold layer (`lh_cs_gold.workday_cs`)
# **Source tables**
# **Core dimensions**
# - `dim_students` – student surrogate keys and attributes
# - `dim_program` – academic programs with effective/expiration dates and duration
# - `dim_course_sections` – course section keys/IDs
# - `dim_grade` – grading scheme and grade keys
# - `dim_academic_period` – terms/sessions and measurement dates
# - `dim_college` – academic units/colleges
# - `dim_campus_location` – campus locations and primary locations
# - `dim_course_section_taxonomy` – section-level taxonomy codes (dual‑credit flags)
# - `dim_date` – calendar date dimension
# - `dim_cohort` – program and institutional cohorts
# - `dim_ref_cohort_measurement_map` – maps entry terms to Y1/Y2/Y3/Y4/Term2 measurement periods
# 
# **Reference  tables**
# - `ref_peoplesoft_dual_credit` – Peoplesoft dual‑credit mappings
# - `ref_student_grade` – raw student grade feed used to clean registration grades
# 
# **Target fact tables**
# - `fact_cohort_retention` – cohort‑level retention/completion fact (program + institutional levels, written by this notebook)


# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
import json
from datetime import date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

JOB_NAME = "testing"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
silver_schema_name = 'workday_cs'
gold_schema_name = 'workday_cs'
silver_lakehouse_name = 'lh_cs_silver'
gold_lakehouse_name = 'lh_cs_gold'
notebook_name = notebookutils.runtime.context["currentNotebookName"]
config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
#config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, silver_lakehouse_name)
gold_lakehouse_id = get_lakehouse_id(var_cs_lib_name, gold_lakehouse_name)
PIPELINE_NAME ='goldlayeringestion'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build dim_program with de‑duplicated program date ranges
# Inputs: lh_cs_gold.workday_cs.dim_program
# Output: cached DataFrame `program_df` used across registration/completion/enrollment logic

dim_program_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")

program_df = spark.read.format("delta").load(dim_program_path)\
                .select("program_key", "program_name", "effective_date", "expiration_date", "program_lineage_key")


# Calculate duration for overlap pruning
df1 = (program_df
    .withColumn(
        "duration",
        F.datediff(F.col("expiration_date"), F.col("effective_date"))      
    )
)

a = df1.alias("a")
b = df1.alias("b")

# Find records that have an overlapping record with a longer duration
to_remove = (
    a.join(
        b,
        (F.col("a.program_name") == F.col("b.program_name")) &
        (F.col("a.program_key") != F.col("b.program_key")) &
        # Overlap condition
        (F.col("a.effective_date") <= F.col("b.expiration_date")) &
        (F.col("a.expiration_date") >= F.col("b.effective_date")) &
        # b has a longer duration
        (F.col("b.duration") > F.col("a.duration")),
        "inner"
    )
    .select(F.col("a.program_key"))
    .distinct()
)

# Keep all records except the shorter overlapping ones
program_df = df1.join(to_remove, "program_key", "left_anti").drop("duration")

program_df.cache()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# registration_silver

# CELL ********************

try:
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

        # Use regexp_replace to transform the string values in the column
        ref_std_df = (
                spark.read.format("delta").load(ref_std_grade_path)
                .withColumn("calc_term", F.regexp_extract("term", r"^([^ ]+)", 1))
                .withColumn("calc_term_year", F.regexp_extract("term", r"^[^ ]+ (.*)$", 1))
                .withColumn("course_section_id", F.concat(F.col("catalog_#").cast("string"), F.lit("_"), F.col("class_#").cast("string"), F.lit("_"), F.col("calc_term_year"), F.lit("_"), F.col("calc_term")))
                .select("student_id", "course_section_id", "original_grade", "current_grade")
        )

        pl_result = spark.read.format("delta").load(pl_dual_path)\
                                .select("student_key", "academic_period_key")\
                                .drop_duplicates()
                        
        window_spec = Window.partitionBy("course_section_key").orderBy(F.col("code"))

        dim_cs_taxonomy_df = (
        spark.read.format("delta").load(dim_cs_taxonomy_path)
                .filter(F.col("code").isin(["1A", "1B", "1E", "8A", "8B", "9B", "9C", "9D", "9G", "9H", "9K"]))
                .withColumn("rn", F.row_number().over(window_spec))
                .filter(F.col("rn") == 1)
                .select("course_section_key")
        )

        dim_acad_period_df = spark.read.format("delta").load(dim_aca_period)

        transfer_credit_df = spark.read.format("delta").load(transfer_credit_path)\
                                .select("student_id", "course", "credits")

        grade_df = spark.read.format("delta").load(dim_student_grade_path)\
                        .withColumn("grade", F.lower(F.col("grade")))
        dim_student_df = spark.read.format("delta").load(students_path)\
                        .filter(F.col("is_current") == "1")\
                        .select("student_key", "student_id")

        dim_clg_df = spark.read.format("delta").load(dim_college_path)\
                        .select("college_key", "name")

        dim_campus_loc_df = spark.read.format("delta").load(dim_campus_loc_path)\
                                .select("campus_location_key", "name", "primary_campus_location")

        dim_course_df = spark.read.format("delta").load(course_section_path)\
                        .select("course_section_key", "course_section_id")

        dim_course_section_df = dim_course_df.alias("CO")\
                                        .selectExpr("CO.course_section_key", "CO.course_section_id")

        course_reg_df = spark.read.format("delta").load(std_course_reg_path)

        # Derive cleaned campus_location from semicolon-separated list using location_code
        campus_list = F.transform(F.split(F.col("campus_location"), ";"), lambda x: F.trim(x))
        keyword = F.regexp_extract(F.col("location_code"), "-(.*)", 1)
        matches = F.filter(campus_list, lambda x: x.contains(keyword))
        selected_campus = F.when(F.size(matches) > 0, matches.getItem(0)).otherwise(campus_list.getItem(0))
        
        course_registration_base_df = course_reg_df.withColumn(
                "campus_location",
                F.when(
                F.col("campus_location").isNotNull() & (F.col("campus_location") != ""),
                selected_campus,
                ).otherwise(F.col("campus_location")),
        )

        std_course_reg_df = (
        course_registration_base_df.alias("SRC")
        .join(
            ref_std_df.alias("RST"),
            (
                F.upper(F.col("SRC.course_section_id")) == F.upper(F.col("RST.course_section_id"))
            )
            & (
                F.upper(F.col("RST.current_grade")) == F.upper(F.col("SRC.grade"))
            )
            & (
                F.upper(F.col("RST.student_id")) == F.upper(F.col("SRC.student_id"))
            ),
            "left",
        )
        .select(
                F.col("SRC.registration_key"),
                F.col("SRC.academic_period"),
                F.col("SRC.course_section_id"),
                F.col("SRC.academic_record_status"),
                F.col("SRC.student_id"),
                F.to_timestamp(F.col("SRC.graded_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("graded_date"),
                F.col("SRC.grading_scheme"),
                F.col("SRC.grading_basis"),
                F.to_timestamp(F.col("SRC.registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("registered_date"),
                F.to_timestamp(F.col("SRC.waitlist_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("waitlist_date"),
                F.to_timestamp(F.col("SRC.withdrawal_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("withdrawal_date"),
                F.to_timestamp(F.col("SRC.drop_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("drop_date"),
                F.col("SRC.registration_status"),
                F.col("SRC.credits"),
                F.col("SRC.earned_credits"),
                F.col("SRC.original_credits"),
                F.col("SRC.course_listing"),
                F.col("SRC.academic_unit"),
                F.when((F.col("SRC.location_code") == "") | (F.col("SRC.campus_location") == ""), "Unknown")
                        .when((F.col("SRC.location_code").isNull()) | (F.col("SRC.campus_location").isNull()), "Unknown")
                        .when((F.lower(F.col("SRC.location_code")).like("%online%")) | (F.lower(F.col("SRC.campus_location")).like("%online%")), "Online")
                        .otherwise(F.col("SRC.location_code")).alias("location_code"),
                F.when((F.col("SRC.location_code") == "") | (F.col("SRC.campus_location") == ""), "Unknown")
                        .when((F.col("SRC.location_code").isNull()) | (F.col("SRC.campus_location").isNull()), "Unknown")
                        .when((F.lower(F.col("SRC.location_code")).like("%online%")) | (F.lower(F.col("SRC.campus_location")).like("%online%")), "Online")
                        .otherwise(F.col("SRC.campus_location")).alias("campus_location"),
                F.lower(F.coalesce(F.col("RST.original_grade"),F.col("SRC.grade"))).alias("grade"),
                F.col("SRC.primary_program_of_study"),
                F.col("SRC.program_of_study"),
                F.coalesce(
                        F.to_date(F.to_timestamp(F.col("SRC.registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
                        F.to_date(F.to_timestamp(F.col("SRC.waitlist_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
                ).alias("converted_reg_date")
        ))


        joined_df = std_course_reg_df.alias("CR")\
                        .join(transfer_credit_df.alias("TC")
                                ,((F.col("CR.student_id") == F.col("TC.student_id"))
                                & (F.col("CR.course_listing") == F.col("TC.course")))
                                ,"left")\
                        .join(dim_student_df.alias("STD")
                                ,(F.col("CR.student_id") == F.col("STD.student_id"))
                                ,"left")\
                        .join(dim_clg_df.alias("CLG")
                                ,(F.col("CLG.name") == F.col("CR.academic_unit"))
                                ,"left")\
                        .join(dim_course_section_df.alias("CS")
                                ,(F.col("CR.course_section_id") == F.col("CS.course_section_id"))
                                ,"left")\
                        .join(dim_cs_taxonomy_df.alias("CST")
                                ,(F.col("CST.course_section_key") == F.col("CS.course_section_key"))
                                ,"left")\
                        .join(dim_campus_loc_df.alias("LOC")
                                ,((F.col("CR.location_code") == F.col("LOC.name"))
                                & (F.col("CR.campus_location") == F.col("LOC.primary_campus_location")))
                                ,"left")\
                        .join(grade_df.alias("GD")
                                ,((F.col("GD.grade") == F.col("CR.grade"))
                                & (F.col("GD.scheme_id") == F.col("CR.grading_scheme")))
                                ,"left")\
                        .join(dim_acad_period_df.alias("AP")
                                ,F.col("CR.academic_period") == F.col("AP.period_name")
                                ,"left")\
                        .join(pl_result.alias("PST")
                                ,((F.col("PST.student_key") == F.col("STD.student_key"))
                                & (F.col("PST.academic_period_key") == F.col("AP.academic_period_key")))
                                ,"left")\
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
                                "CASE WHEN CST.course_section_key IS NOT NULL OR PST.student_key IS NOT NULL THEN 1 ELSE 0 END AS is_dual_credit_flag"
                                )

        program_joined_df = joined_df.alias("CR")\
                        .join(program_df.alias("PG")
                                ,((F.col("PG.program_name") == F.col("CR.primary_program_of_study"))
                                        & (F.col("PG.effective_date") <= F.col("CR.converted_reg_date"))
                                        & (F.col("PG.expiration_date") >= F.col("CR.converted_reg_date")))
                                ,"left")\
                        .selectExpr(
                                "CR.registration_key",
                                "PG.program_key AS primary_program_of_study_key",
                                "CR.student_key",
                                "CR.academic_period_key",
                                "CR.grade_key AS grade_key",
                                "CR.course_section_key",
                                "CR.college_key",
                                "CR.campus_location_key",
                                "CR.academic_record_status",
                                "CR.graded_date",
                                "CR.grading_basis",
                                "CR.registered_date",
                                "CR.waitlist_date",
                                "CR.withdrawal_date",
                                "CR.drop_date",
                                "CR.registration_status",
                                "CAST(CR.credits AS INT) AS credits",
                                "CAST(CR.earned_credits AS INT) AS earned_credits",
                                "CAST(CR.original_credits AS INT) AS original_credits",
                                "CAST(CR.transfer_credits AS INT) AS transfer_credits",
                                "CR.is_dual_credit_flag"
                        ).drop_duplicates()

        fact_reg_audit_df = add_audit_column(program_joined_df, PIPELINE_NAME)

        # =================================================================
        # PS RETENTION EVIDENCE
        #
        # Appends PeopleSoft enrolments to the in-memory registration frame
        # so cells 14 and 15 can measure retention for PS-only students.
        # Gold fact_registration is NOT written by this notebook, so nothing
        # here reaches CSR, the enrolment snapshot or FTE.
        #
        # POPULATION: dim_students.is_history = 1 only (the ~8,624 students
        # who exist in PeopleSoft but were never migrated to Workday).
        # Everyone else already has complete Workday registrations -
        # fact_registration covers 1970-2027 at full volume - so including
        # them would double-count every term.
        #
        # FLOOR: STRM >= '2138' (Fall 2013), matching nb_dim_program,
        # nb_dim_cohort and the declarations block. Randy's
        # ADMIT_TERM >= '2132' (Spring 2013) applies to the dim_students
        # load only, deliberately two terms wider so a Spring-2013 admit who
        # first enrolled in Fall 2013 exists to be placed in that cohort.
        #
        # STRM: only the modern '2' + YY + term-digit format is decoded.
        # Every legacy code ('0011'-'0265') is pre-2000 and sorts below
        # '2138', so the string comparison excludes them with no decode and
        # no origin to calibrate.
        # =================================================================

        PS_RETENTION_ENABLED    = True        # False reverts to Workday-only
        PS_RETENTION_FLOOR_STRM = "2138"      # Fall 2013
        ps_schema_name          = "peoplesoft"   # VERIFY

        if PS_RETENTION_ENABLED:

            ps_enrl_df = spark.read.format("delta").load(
                abfss_path_builder(workspace_id, silver_lakehouse_id,
                                   ps_schema_name, "PS_STDNT_ENRL"))

            # --- main-term periods only. Session rows are rolled up to the
            #     main term in cell 14; emitting main-term keys directly
            #     makes that rollup a no-op for PS rows.
            ps_period_df = (
                spark.read.format("delta").load(dim_aca_period)
                .filter(F.col("session").isNull())
                .select(
                    F.col("academic_period_name").alias("dim_period_name"),
                    F.col("academic_period_key").alias("dim_period_key"),
                )
                .dropDuplicates(["dim_period_name"])
            )

            # --- PS-only students
            ps_student_df = (
                spark.read.format("delta").load(students_path)
                .filter(F.col("is_history") == 1)
                .select(
                    F.col("student_key").alias("ps_student_key"),
                    F.col("student_id").alias("ps_student_id"),
                )
                .dropDuplicates(["ps_student_id"])
            )

            # --- STRM -> 'YYYY Fall'  (year first, matching the dimension)
            _strm = F.trim(F.col("STRM"))
            _term_digit = F.substring(_strm, 4, 1)

            _ps_period_name = F.concat(
                (F.lit(2000) + F.substring(_strm, 2, 2).cast("int")).cast("string"),
                F.lit(" "),
                F.when(_term_digit == "2", F.lit("Spring"))
                 .when(_term_digit == "5", F.lit("Summer"))
                 .when(_term_digit == "8", F.lit("Fall")),
            )

            _grade = F.coalesce(F.trim(F.col("CRSE_GRADE_OFF")), F.lit(""))

            ps_filtered_df = (
                ps_enrl_df
                .filter(F.col("CRSE_CAREER") == "PS")       # excludes CNED (0.05 CEU rows)
                .filter(_strm >= F.lit(PS_RETENTION_FLOOR_STRM))
                .filter(F.col("STDNT_ENRL_STATUS") == "E")  # D = dropped, W = waitlisted
                # Withdrawal lives in the grade, not the status: 'W' appears
                # under ENRL, WDRW, DROP and EWAT reasons. NS = no show,
                # AU = audit (not credit-bearing toward progress).
                .filter(~_grade.isin("W", "NS", "AU"))
                .filter(F.col("UNT_TAKEN") >= 1)
                .withColumn("ps_period_name", _ps_period_name)
                .withColumn("ps_grade", _grade)
            )

            ps_reg_df = (
                ps_filtered_df.alias("e")
                .join(ps_period_df,
                      F.col("e.ps_period_name") == F.col("dim_period_name"),
                      "inner")
                .join(ps_student_df,
                      F.trim(F.col("e.EMPLID")) == F.col("ps_student_id"),
                      "inner")
                .select(
                    F.sha2(F.concat_ws("||", F.lit("PS"),
                                       F.trim(F.col("e.EMPLID")),
                                       F.trim(F.col("e.STRM")),
                                       F.col("e.CLASS_NBR").cast("string"),
                                       F.coalesce(F.col("e.SESSION_CODE").cast("string"),
                                                  F.lit(""))), 256).alias("registration_key"),
                    F.lit(None).cast("string").alias("primary_program_of_study_key"),
                    F.col("ps_student_key").alias("student_key"),
                    F.col("dim_period_key").alias("academic_period_key"),
                    F.lit(None).cast("string").alias("grade_key"),
                    F.lit(None).cast("string").alias("course_section_key"),

                    # No owning-unit or campus mapping exists for PeopleSoft.
                    F.lit(None).cast("string").alias("college_key"),
                    F.lit(None).cast("string").alias("campus_location_key"),
                    F.lit(None).cast("string").alias("academic_record_status"),

                    # Date columns are not read by the retention logic.
                    F.lit(None).cast("timestamp").alias("graded_date"),
                    F.lit(None).cast("string").alias("grading_basis"),
                    F.lit(None).cast("timestamp").alias("registered_date"),
                    F.lit(None).cast("timestamp").alias("waitlist_date"),
                    F.lit(None).cast("timestamp").alias("withdrawal_date"),
                    F.lit(None).cast("timestamp").alias("drop_date"),

                    # Graded = Completed, ungraded = Registered. Both satisfy
                    # the isin("Completed","Registered") test in cells 14/15.
                    F.when(F.col("e.ps_grade") != "", F.lit("Completed"))
                     .otherwise(F.lit("Registered")).alias("registration_status"),

                    # Workday credits is INTEGER; UNT_TAKEN is decimal. Round
                    # rather than truncate - a 2.5-credit course would
                    # otherwise become 2 and under-state program_credits.
                    F.round(F.col("e.UNT_TAKEN")).cast("int").alias("credits"),
                    F.round(F.coalesce(F.col("e.UNT_EARNED"), F.lit(0))).cast("int")
                     .alias("earned_credits"),
                    F.lit(None).cast("int").alias("original_credits"),
                    F.lit(None).cast("int").alias("transfer_credits"),

                    # NOT NULL in the target schema.
                    F.lit(0).cast("int").alias("is_dual_credit_flag"),
                )
            )

            ps_reg_df = add_audit_column(ps_reg_df, PIPELINE_NAME)

            # --- hard schema gate. Fail loudly rather than null-fill:
            #     is_dual_credit_flag and the four audit columns are NOT NULL,
            #     so allowMissingColumns would be unsafe here.
            _tgt = set(fact_reg_audit_df.columns)
            _src = set(ps_reg_df.columns)
            if _tgt != _src:
                raise Exception(
                    f"PS retention schema mismatch. "
                    f"missing={sorted(_tgt - _src)} extra={sorted(_src - _tgt)}"
                )

            _types = dict(fact_reg_audit_df.dtypes)
            ps_reg_df = ps_reg_df.select(
                [F.col(c).cast(_types[c]).alias(c) for c in fact_reg_audit_df.columns]
            )

            # --- diagnostics
            ps_reg_df.cache()
            _ps_rows     = ps_reg_df.count()
            _ps_students = ps_reg_df.select("student_key").distinct().count()
            _ps_terms    = ps_reg_df.select("academic_period_key").distinct().count()
            _ps_dupes    = (ps_reg_df.groupBy("registration_key").count()
                                     .filter(F.col("count") > 1).count())

            print(f"[ps_retention] rows               : {_ps_rows}")
            print(f"[ps_retention] students           : {_ps_students}   (expect <= 8,624)")
            print(f"[ps_retention] distinct terms     : {_ps_terms}")
            print(f"[ps_retention] duplicate reg keys : {_ps_dupes}   (expect 0)")

            if _ps_rows == 0:
                raise Exception(
                    "PS retention produced 0 rows. Check (a) is_history = 1 rows "
                    "exist in dim_students, (b) the STRM decode matches "
                    "dim_academic_period naming, (c) ps_schema_name."
                )

            if _ps_dupes:
                raise Exception(
                    "PS retention: duplicate registration_key - PS_STDNT_ENRL is "
                    "not unique on EMPLID + STRM + CLASS_NBR + SESSION_CODE."
                )

            fact_reg_audit_df = fact_reg_audit_df.unionByName(ps_reg_df)
            ps_reg_df.unpersist()

            print(f"[ps_retention] fact_reg_audit_df total: {fact_reg_audit_df.count()}")

        else:
            print("[ps_retention] DISABLED - Workday-only registrations")

        

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    
    
    raise Exception(f"Failed to create/load fact_registration table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

dim_course_section_tax_path = abfss_path_builder(
    workspace_id,
    gold_lakehouse_id,
    gold_schema_name,
    "dim_course_section_taxonomy",
)

dim_course_section_tax_df = (
    spark.read.format("delta")
    .load(dim_course_section_tax_path)
    .select("course_section_id", "code")
)


course_section_tax_df = (
    dim_course_section_tax_df
    .groupBy("course_section_id")
    .agg(
        F.concat_ws(",", F.collect_set("code")).alias("tax_code_list")
    )
)


reg_with_tax_df = (
    std_course_reg_df.alias("c")
    .join(
        course_section_tax_df.alias("a"),
        F.col("c.course_section_id") == F.col("a.course_section_id"),
        "left",
    )
)

# Final aggregation at student/program/term level
program_tax_df = (
    reg_with_tax_df
    .groupBy("student_id", "academic_period","primary_program_of_study")
    .agg(
        # Aggregate all section-level tax_code_list values into a single comma‑separated list
        F.concat_ws(",", F.collect_list("tax_code_list")).alias("tax_code_list")
    )
)

program_tax_df.cache()
print("program_tax_df built with", program_tax_df.count(), "rows")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# silver_complition

# CELL ********************

try:
    pos_all = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "program_of_study_all")
    dim_aca_period = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_date_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_date")
    students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    dim_college_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")
    # dim_program_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")

    # Read program of study data from the silver layer
    pos_df = spark.read.format("delta").load(pos_all)

    # -----------------------------------------------------------------
    # WORKDAY COMPLETION PERIOD -- added 20 Oct 2026 (RPT-13 / RPT-15).
    #
    #    NWTC decision, 19 Oct call: "Workday: use the conferral
    #    information, not the completion date. Conferral period: the
    #    academic period the credential is awarded for. Use the period
    #    to assign the term."
    #
    #    conferral_academic_period arrives ALREADY in the dimension's
    #    format ("2011 Summer"), so this is a direct name join -- no
    #    STRM decode, unlike the PeopleSoft path.
    #
    #    conferral_date is deliberately NOT used as the completion date.
    #    It can fall outside its own conferral period: silver carries
    #    rows such as conferral_date 1985-01-16 against period
    #    "1984 Fall". The date is when the credential was processed; the
    #    period is what it was awarded for. Only the period assigns the
    #    term. Taking the period's END date keeps the completion inside
    #    its own term by construction, exactly as on the PeopleSoft side.
    #
    #    Measured on silver before this change (1,330,865 rows):
    #      has completion_date .......................... 193,177
    #      has conferral period ......................... 192,940
    #      completion_date NULL + conferral period set ...       0
    #      program_status Complete, no conferral period ..     117
    #
    #    The zero is why there is NO scope floor here. No row gains a
    #    completion, so this can only MOVE dates. The 87,577 pre-2013
    #    conferrals already carry completion dates today and are
    #    unaffected; flooring them would REMOVE completions the model
    #    already has.
    #
    #    The 237 rows with a completion date but no conferral period
    #    keep today's date via the COALESCE.
    # -----------------------------------------------------------------
    _wd_period_end_df = (
        spark.read.format("delta").load(dim_aca_period)
        .filter(F.col("session").isNull())
        .select(
            F.col("academic_period_name").alias("wd_ct_period_name"),
            F.col("period_end_date").alias("wd_ct_period_end_date"),
        )
        .dropDuplicates(["wd_ct_period_name"])       # 1 row per period name
    )

    _wd_rows_before = pos_df.count()
    _wd_comp_before = pos_df.filter(F.col("completion_date").isNotNull()).count()

    pos_df = (
        pos_df
        .join(
            _wd_period_end_df,
            F.col("conferral_academic_period") == F.col("wd_ct_period_name"),
            "left",
        )
        .withColumn(
            "completion_date",
            F.coalesce(
                F.to_date(F.col("wd_ct_period_end_date")),
                F.to_date(F.col("completion_date")),
            ),
        )
        .drop("wd_ct_period_name", "wd_ct_period_end_date")
    )

    _wd_rows_after = pos_df.count()
    _wd_comp_after = pos_df.filter(F.col("completion_date").isNotNull()).count()

    print(f"[fact_cohort] WD rows        before/after : {_wd_rows_before} / {_wd_rows_after}")
    print(f"[fact_cohort] WD completions before/after : {_wd_comp_before} / {_wd_comp_after}")
    if _wd_rows_after != _wd_rows_before:
        raise Exception(
            f"Conferral period join fanned out: {_wd_rows_before} -> "
            f"{_wd_rows_after}. dim_academic_period has duplicate "
            f"academic_period_name rows at session IS NULL. Do not load."
        )
    if _wd_comp_after != _wd_comp_before:
        print("[fact_cohort] WARNING: the conferral swap changed the NUMBER of "
              "Workday completions. It must only change their dates - "
              "investigate before handing numbers to NWTC.")


    from pyspark.sql.window import Window

    ps_schema_name = "peoplesoft"        # VERIFY
    PS_SCOPE_START = "2013-08-01"        # Fall 2013 — matches nb_dim_program / nb_dim_cohort

    # Workday became system of record around April 2025.
    #   "2526 for fall, that should all be in Workday because that
    #    would have kicked off in April."              (Oct 9 call)
    # Workday declarations on or after this date are authoritative and
    # are never suppressed. CONFIRM WITH NWTC.
    WD_CUTOVER = "2025-04-01"

    ps_plan_df = spark.read.format("delta").load(
        abfss_path_builder(workspace_id, silver_lakehouse_id, ps_schema_name, "PS_ACAD_PLAN"))
    ps_map_df  = spark.read.format("delta").load(
        abfss_path_builder(workspace_id, silver_lakehouse_id, ps_schema_name, "PS_PROGRAM_MAP"))
    ps_prog_df = spark.read.format("delta").load(
        abfss_path_builder(workspace_id, silver_lakehouse_id, ps_schema_name, "PS_ACAD_PROG"))

    # -----------------------------------------------------------------
    # 1. One plan row per declaration.
    #
    #    PS_ACAD_PLAN is effective-dated, and DECLARE_DT is the stable
    #    declaration date carried across those rows. Keep the latest
    #    effective row per (student, career number, plan, declare date).
    #
    #    Excluded here:
    #      000000  - undeclared
    #      *Z      - pre-program
    #      *G      - waitlist
    #    dim_program excludes Z and G via enrollment_stage, but this
    #    frame joins on the mapped TITLE, so a 105041Z would otherwise
    #    resolve to the real programme and create a cohort entry.
    # -----------------------------------------------------------------
    w_plan = (
        Window
        .partitionBy("EMPLID", "STDNT_CAR_NBR", "ACAD_PLAN", "declare_dt")
        .orderBy(F.col("EFFDT").desc(), F.col("EFFSEQ").desc())
    )

    ps_plan_latest_df = (
        ps_plan_df
        .withColumn("ACAD_PLAN", F.trim(F.col("ACAD_PLAN")))
        .withColumn("EMPLID",    F.trim(F.col("EMPLID")))
        .withColumn("declare_dt", F.to_date(F.col("DECLARE_DT")))
        .filter(
            F.col("ACAD_PLAN").isNotNull()
            & (F.col("ACAD_PLAN") != "")
            & (F.col("ACAD_PLAN") != "000000")
            & ~F.col("ACAD_PLAN").endswith("Z")
            & ~F.col("ACAD_PLAN").endswith("G")
            # scope window; also disposes of the 1901-01-01 sentinel
            # and the 114 rows dated to 3201-08-20
            & F.col("declare_dt").between(
                F.lit(PS_SCOPE_START).cast("date"), F.current_date())
        )
        .withColumn("rn", F.row_number().over(w_plan))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # -----------------------------------------------------------------
    # 2. Completion, per career track.
    #
    #    PS_ACAD_PLAN hangs off PS_ACAD_PROG by
    #      EMPLID + ACAD_CAREER + STDNT_CAR_NBR (+ EFFDT + EFFSEQ)
    #    so STDNT_CAR_NBR identifies the programme track. Attributing
    #    completion at that grain means a student who finished one
    #    programme and abandoned another is not marked Complete on both.
    #
    #    PROG_ACTION = 'COMP' is the completion action (Randy's query
    #    used PROG_ACTION <> 'COMP' to find incomplete programmes).
    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
    # STRM -> academic_period_name.
    #
    #    Modern PeopleSoft term codes are '2' + YY + term digit, where
    #    2 = Spring, 5 = Summer, 8 = Fall. 2225 -> "2022 Summer".
    #    Legacy codes (0011-0265) and anything malformed return NULL.
    #    Mirrors the helper in cell 12 - keep the two in step.
    # -----------------------------------------------------------------
    def _ps_term_to_period_name(term_col):
        _t  = F.trim(term_col)
        _yr = (F.lit(2000) + F.substring(_t, 2, 2).cast("int")).cast("string")
        _d  = F.substring(_t, 4, 1)
        return F.when(
            (F.length(_t) == 4)
            & (F.substring(_t, 1, 1) == "2")
            & _d.isin("2", "5", "8"),
            F.concat(
                _yr,
                F.lit(" "),
                F.when(_d == "2", F.lit("Spring"))
                 .when(_d == "5", F.lit("Summer"))
                 .otherwise(F.lit("Fall")),
            ),
        )

    # -----------------------------------------------------------------
    # COMPLETION DATE SOURCE -- changed 20 Oct 2026 (EGLFIND-144 / RPT-13).
    #
    #    PROG_ACTION='COMP' rows carry EFFDT on the PeopleSoft term END
    #    date -- 12/31 or 5/22. Those dates land in the gaps BETWEEN the
    #    gold academic periods (Fall 2024 ends 12/22, Spring 2025 starts
    #    01/21), so the completion resolved to no period at all and the
    #    student counted as never having completed. 5,248 completions sat
    #    in the Fall 2024 gap alone; Nursing 2023 Fall showed 0 of 80.
    #
    #    COMPLETION_TERM on the SAME ROW states the term outright, so the
    #    date never has to be interpreted. Decode it to a period name and
    #    take that period's END date from the gold dimension, which is
    #    inside the period by construction.
    #
    #    NWTC decision, 19 Oct call: "PeopleSoft: use the completion term
    #    (grad term), not the effective date." PS_TERM_TBL is deliberately
    #    NOT used -- its TERM_END_DT carries the same 12/31 and 5/22 values
    #    that cause the problem.
    #
    #    Grain is UNCHANGED: EMPLID + ACAD_CAREER + STDNT_CAR_NBR, the
    #    latest COMP row per track. Moving to plan grain (COMPLETION_TERM
    #    also exists on PS_ACAD_PLAN) would stop marking un-awarded plans
    #    Complete, but that is a separate number movement and is NOT in
    #    this change.
    #
    #    Rows with a missing or undecodable COMPLETION_TERM, or a term with
    #    no gold period, keep the latest EFFDT -- today's behaviour exactly
    #    -- so no completion is lost by the swap.
    # -----------------------------------------------------------------
    _ps_period_end_df = (
        spark.read.format("delta").load(dim_aca_period)
        .filter(F.col("session").isNull())
        .select(
            F.col("academic_period_name").alias("ct_period_name"),
            F.col("period_end_date").alias("ct_period_end_date"),
        )
        .dropDuplicates(["ct_period_name"])
    )

    # Latest COMP row per track. Equivalent to the previous MAX(EFFDT) for
    # the fallback date, and it also gives us that row's COMPLETION_TERM.
    w_comp = (
        Window
        .partitionBy("EMPLID", "ACAD_CAREER", "STDNT_CAR_NBR")
        .orderBy(F.col("EFFDT").desc(), F.col("EFFSEQ").desc())
    )

    ps_completed_df = (
        ps_prog_df
        .filter(F.col("PROG_ACTION") == "COMP")
        .withColumn("EMPLID", F.trim(F.col("EMPLID")))
        .withColumn("rn", F.row_number().over(w_comp))
        .filter(F.col("rn") == 1)
        .withColumn("comp_effdt", F.to_date(F.col("EFFDT")))
        .withColumn("comp_term", F.trim(F.col("COMPLETION_TERM")))
        .withColumn("comp_period_name",
                    _ps_term_to_period_name(F.col("comp_term")))
        .join(
            _ps_period_end_df,
            F.col("comp_period_name") == F.col("ct_period_name"),
            "left",
        )
        .select(
            F.col("EMPLID").alias("c_emplid"),
            F.col("ACAD_CAREER").alias("c_career"),
            F.col("STDNT_CAR_NBR").alias("c_car_nbr"),
            F.col("comp_term").alias("c_completion_term"),
            F.col("comp_period_name").alias("c_completion_period_name"),
            F.col("comp_effdt").alias("c_legacy_effdt"),
            F.when(F.col("ct_period_end_date").isNotNull(), F.lit(1))
             .otherwise(F.lit(0)).alias("c_term_resolved"),
            # The swap. COALESCE keeps today's behaviour where the term is
            # unusable, so this can only move dates, never remove them.
            F.coalesce(F.col("ct_period_end_date"), F.col("comp_effdt"))
             .alias("ps_completion_date"),
        )
    )

    # Coverage. falling_back large means COMPLETION_TERM is thinner than
    # the 19 Oct call assumed and NWTC needs to hear it.
    _ct_total    = ps_completed_df.count()
    _ct_resolved = ps_completed_df.filter(F.col("c_term_resolved") == 1).count()
    _ct_moved    = ps_completed_df.filter(
        F.col("ps_completion_date") != F.col("c_legacy_effdt")).count()
    print(f"[fact_cohort] PS COMP tracks                : {_ct_total}")
    print(f"[fact_cohort]   term resolved to a period   : {_ct_resolved}")
    print(f"[fact_cohort]   falling back to EFFDT       : {_ct_total - _ct_resolved}")
    print(f"[fact_cohort]   dates actually moved        : {_ct_moved}")

    # -----------------------------------------------------------------
    # 3. Shape to program_of_study_all.
    #
    #    program_of_study carries the MAPPED Workday title because the
    #    programme join in cell 12 is on NAME:
    #        PG.program_name == POS.program_of_study
    #    Both sides come from PS_PROGRAM_MAP.Workday_Program_Title, so
    #    they agree — see validation query V3.
    # -----------------------------------------------------------------
    ps_pos_df = (
        ps_plan_latest_df.alias("p")
        .join(
            ps_map_df.alias("m"),
            F.col("p.ACAD_PLAN") == F.trim(F.col("m.PeopleSoft_Acad_Plan_Code")),
            "inner",            # unmapped codes have no dim_program row anyway
        )
        .join(
            ps_completed_df,
            (F.col("p.EMPLID")        == F.col("c_emplid"))
            & (F.col("p.ACAD_CAREER") == F.col("c_career"))
            & (F.col("p.STDNT_CAR_NBR") == F.col("c_car_nbr")),
            "left",
        )
        .select(
            # STDNT_CAR_NBR is in the key: the same plan can appear under
            # two career numbers, and DECLARE_DT alone is not unique.
            F.sha2(F.concat_ws("||", F.lit("PS"),
                               F.col("p.EMPLID"),
                               F.col("p.STDNT_CAR_NBR").cast("string"),
                               F.col("p.ACAD_PLAN"),
                               F.col("p.declare_dt").cast("string")), 256).alias("pos_key"),
            F.col("p.EMPLID").alias("student_id"),
            F.lit(None).cast("string").alias("academic_unit"),
            F.col("p.declare_dt").alias("declare_date"),
            F.lit(None).cast("date").alias("expected_completion_date"),
            F.col("p.declare_dt").alias("effective_date"),
            F.col("m.Workday_Program_Title").alias("program_of_study"),
            F.when(F.col("p.PLAN_SEQUENCE") == 1, F.lit("Y"))
             .otherwise(F.lit("N")).alias("is_primary_program"),
            F.lit(None).cast("string").alias("earned_credential"),
            F.lit(None).cast("string").alias("program_type"),
            F.lit(None).cast("string").alias("academic_status"),
            F.col("ps_completion_date").alias("completion_date"),
            F.when(F.col("ps_completion_date").isNotNull(), F.lit("Complete"))
             .otherwise(F.lit("In Progress")).alias("program_status"),
            F.lit(1).alias("is_history"),
        )
    )

    # -----------------------------------------------------------------
    # 4. PRECEDENCE — PeopleSoft wins for pre-cutover history.
    #
    #    Where PS holds declarations for a (student, programme) pair,
    #    the Workday row for that pair is the pancaked version of the
    #    same history and must go, or the wrong entry term survives
    #    alongside the right one.
    #
    #    Scoped to Workday declarations BEFORE the cutover. Anything on
    #    or after it is Workday's own record and is left alone.
    # -----------------------------------------------------------------
    ps_pairs_df = (
        ps_pos_df.select("student_id", "program_of_study").distinct()
        .withColumn("_ps_covers", F.lit(1))
    )

    pos_df = pos_df.withColumn("is_history", F.lit(0))\
                   .withColumn("_wd_declare_date", F.col("declare_date"))

    pos_wd_kept_df = (
        pos_df.alias("w")
        .join(ps_pairs_df, ["student_id", "program_of_study"], "left")
        .filter(
            F.col("_ps_covers").isNull()                               # PS has nothing here
            | (F.col("w.declare_date") >= F.lit(WD_CUTOVER).cast("date"))  # or it is Workday's own era
        )
        .drop("_ps_covers")
    )

    _wd_before = pos_df.count()
    _wd_after  = pos_wd_kept_df.count()

    pos_df = pos_wd_kept_df.unionByName(ps_pos_df, allowMissingColumns=True)

    print(f"[fact_cohort] PS declarations in scope   : {ps_pos_df.count()}")
    print(f"[fact_cohort] Workday rows before        : {_wd_before}")
    print(f"[fact_cohort] Workday rows superseded    : {_wd_before - _wd_after}")
    print(f"[fact_cohort] pos_df total after union   : {pos_df.count()}")

    # PS completions and the period window
    _ps_comp = pos_df.filter((F.col("is_history") == 1) & (F.col("program_status") == "Complete"))
    print(
        f"[fact_cohort] PS completions: {_ps_comp.count()}, "
        f"null completion_date: {_ps_comp.filter(F.col('completion_date').isNull()).count()}"
    )

    # Apply completion filter AFTER PS union
    pos_df = pos_df.filter(F.col("program_status") == "Complete")

    # Read and select student dimensions
    dim_student_df = (
        spark.read.format("delta")
        .load(students_path)
        .select("student_key", "student_id")
    )

    # Read and select college dimensions with alias for academic_unit
    dim_college_df = (
        spark.read.format("delta")
        .load(dim_college_path)
        .selectExpr("college_key", "name as academic_unit")
    )

    # Read date dimension (kept for future joins if needed)
    dim_date_df = spark.read.format("delta").load(dim_date_path)\
                    .select("date_key", "calendar_date")

    dim_acad_period_df = spark.read.format("delta").load(dim_aca_period)\
                            .filter(F.col("session").isNull())\
                            .selectExpr("academic_period_key", "academic_period_name",
                                "declare_start_date", "declare_end_date")


    # Join pos_df with student and college dimensions
    # Key Changes:
    # - Removed the trailing backslash that caused the SyntaxError
    # - Ensured the chained joins are properly terminated
    joined_df = (
        pos_df.alias("POS")
        .join(dim_student_df, on="student_id", how="left")
        .join(dim_college_df, on="academic_unit", how="left")
        .join(dim_date_df.alias("DDT"), F.col("POS.declare_date") == F.col("DDT.calendar_date"), how="left")
        .join(dim_date_df.alias("EDT"), F.col("POS.completion_date") == F.col("EDT.calendar_date"), how="left")
        .join(dim_acad_period_df.alias("ACA"), (((F.col("POS.completion_date") >= F.col("ACA.declare_start_date")))
                                        & ((F.col("POS.completion_date") <= F.col("ACA.declare_end_date")))), "left")
        .join(program_df.alias("PG")
                            ,((F.col("PG.program_name") == F.col("POS.program_of_study"))
                                    & (F.col("PG.effective_date") <= F.col("POS.declare_date"))
                                    & (F.col("PG.expiration_date") >= F.col("POS.declare_date")))
                            ,"left")
        .selectExpr(
            "POS.pos_key AS completion_key",
            "student_key",
            "college_key",
            "PG.program_key",
            "DDT.date_key AS declare_date_key",
            "EDT.date_key AS completion_date_key",
            "ACA.academic_period_key AS completion_academic_period_key",
            "POS.is_primary_program",
            "POS.earned_credential",
            "POS.program_type",
            "POS.academic_status"
        )
    )

    fact_complition_df = joined_df

   
except Exception as e:
    
    raise Exception(f"Failed to create/load fact_completion table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Program_enrollment

# CELL ********************

from pyspark.sql.window import Window


try:

    # ---------------------------------------------------------------
    # Build ABFSS paths (base fact creation)
    # ---------------------------------------------------------------
    pos_all = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "program_of_study_all")
    dim_aca_period = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_date_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_date")
    students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    dim_college_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")


    pos_df = (
        spark.read.format("delta").load(pos_all)
    )

    # -----------------------------------------------------------------
    # WORKDAY COMPLETION PERIOD -- added 20 Oct 2026 (RPT-13 / RPT-15).
    #
    #    NWTC decision, 19 Oct call: "Workday: use the conferral
    #    information, not the completion date. Conferral period: the
    #    academic period the credential is awarded for. Use the period
    #    to assign the term."
    #
    #    conferral_academic_period arrives ALREADY in the dimension's
    #    format ("2011 Summer"), so this is a direct name join -- no
    #    STRM decode, unlike the PeopleSoft path.
    #
    #    conferral_date is deliberately NOT used as the completion date.
    #    It can fall outside its own conferral period: silver carries
    #    rows such as conferral_date 1985-01-16 against period
    #    "1984 Fall". The date is when the credential was processed; the
    #    period is what it was awarded for. Only the period assigns the
    #    term. Taking the period's END date keeps the completion inside
    #    its own term by construction, exactly as on the PeopleSoft side.
    #
    #    Measured on silver before this change (1,330,865 rows):
    #      has completion_date .......................... 193,177
    #      has conferral period ......................... 192,940
    #      completion_date NULL + conferral period set ...       0
    #      program_status Complete, no conferral period ..     117
    #
    #    The zero is why there is NO scope floor here. No row gains a
    #    completion, so this can only MOVE dates. The 87,577 pre-2013
    #    conferrals already carry completion dates today and are
    #    unaffected; flooring them would REMOVE completions the model
    #    already has.
    #
    #    The 237 rows with a completion date but no conferral period
    #    keep today's date via the COALESCE.
    # -----------------------------------------------------------------
    _wd_period_end_df = (
        spark.read.format("delta").load(dim_aca_period)
        .filter(F.col("session").isNull())
        .select(
            F.col("academic_period_name").alias("wd_ct_period_name"),
            F.col("period_end_date").alias("wd_ct_period_end_date"),
        )
        .dropDuplicates(["wd_ct_period_name"])       # 1 row per period name
    )

    _wd_rows_before = pos_df.count()
    _wd_comp_before = pos_df.filter(F.col("completion_date").isNotNull()).count()

    pos_df = (
        pos_df
        .join(
            _wd_period_end_df,
            F.col("conferral_academic_period") == F.col("wd_ct_period_name"),
            "left",
        )
        .withColumn(
            "completion_date",
            F.coalesce(
                F.to_date(F.col("wd_ct_period_end_date")),
                F.to_date(F.col("completion_date")),
            ),
        )
        .drop("wd_ct_period_name", "wd_ct_period_end_date")
    )

    _wd_rows_after = pos_df.count()
    _wd_comp_after = pos_df.filter(F.col("completion_date").isNotNull()).count()

    print(f"[fact_cohort] WD rows        before/after : {_wd_rows_before} / {_wd_rows_after}")
    print(f"[fact_cohort] WD completions before/after : {_wd_comp_before} / {_wd_comp_after}")
    if _wd_rows_after != _wd_rows_before:
        raise Exception(
            f"Conferral period join fanned out: {_wd_rows_before} -> "
            f"{_wd_rows_after}. dim_academic_period has duplicate "
            f"academic_period_name rows at session IS NULL. Do not load."
        )
    if _wd_comp_after != _wd_comp_before:
        print("[fact_cohort] WARNING: the conferral swap changed the NUMBER of "
              "Workday completions. It must only change their dates - "
              "investigate before handing numbers to NWTC.")


    ps_schema_name = "peoplesoft"        # VERIFY
    PS_SCOPE_START = "2013-08-01"        # Fall 2013 — matches nb_dim_program / nb_dim_cohort

    # Cohort entry term for PeopleSoft declarations.
    #
    #   "It would be the admit term of the program with no suffix at the
    #    end of their Academic Plan code. This is equivalent to the
    #    declare date in Workday."            (NWTC business user, 13 Oct)
    #
    # ADMIT_TERM varies across the effective-dated rows of a programme
    # stack: 114,369 of 411,837 stacks (28%) resolve to a DIFFERENT
    # academic year depending on which row is taken. "latest" matches
    # what the PeopleSoft screen displays and the records validated
    # against NWTC's AY2022-23 Radiography list.
    # Flip to "first" if NWTC's PeopleSoft team advises otherwise.
    PS_ADMIT_TERM_RULE = "latest"        # "latest" | "first"  — CONFIRM WITH NWTC

    # Workday became system of record around April 2025.
    #   "2526 for fall, that should all be in Workday because that
    #    would have kicked off in April."              (Oct 9 call)
    # Workday declarations on or after this date are authoritative and
    # are never suppressed. CONFIRM WITH NWTC.
    WD_CUTOVER = "2025-04-01"

    ps_plan_df = spark.read.format("delta").load(
        abfss_path_builder(workspace_id, silver_lakehouse_id, ps_schema_name, "PS_ACAD_PLAN"))
    ps_prog_df = spark.read.format("delta").load(
        abfss_path_builder(workspace_id, silver_lakehouse_id, ps_schema_name, "PS_ACAD_PROG"))

    # Programme resolution. Built by nb_peoplesoft_program_crosswalk,
    # which MUST run after nb_dim_program and before this notebook.
    #
    # Replaces the old join of PS_PROGRAM_MAP.Workday_Program_Title to
    # dim_program.program_name. That text match silently dropped every
    # declaration whose map title did not exist in dim_program, or
    # existed only on a programme with no valid date range — Nursing
    # 105431 among them, ~14,000 students in total.
    ps_xwalk_df = (
        spark.read.format("delta").load(
            abfss_path_builder(workspace_id, silver_lakehouse_id,
                               silver_schema_name,
                               "xwalk_program_peoplesoft_workday"))
        .select(
            F.trim(F.col("ps_acad_plan_code")).alias("x_code"),
            F.col("program_lineage_key").alias("x_lineage"),
            F.col("program_name").alias("x_name"),
            F.col("resolution_source").alias("x_source"),
        )
    )

    # -----------------------------------------------------------------
    # 0. STRM -> academic_period_name.
    #
    #    Modern PeopleSoft term codes are '2' + YY + term digit, where
    #    2 = Spring, 5 = Summer, 8 = Fall. 2225 -> "2022 Summer".
    #
    #    Legacy codes (0011-0265, all pre-2000) and anything malformed
    #    return NULL, and those rows fall back to the declare-date
    #    window in the ACA join below rather than being dropped.
    # -----------------------------------------------------------------
    def _ps_term_to_period_name(term_col):
        _t  = F.trim(term_col)
        _yr = (F.lit(2000) + F.substring(_t, 2, 2).cast("int")).cast("string")
        _d  = F.substring(_t, 4, 1)
        return F.when(
            (F.length(_t) == 4)
            & (F.substring(_t, 1, 1) == "2")
            & _d.isin("2", "5", "8"),
            F.concat(
                _yr,
                F.lit(" "),
                F.when(_d == "2", F.lit("Spring"))
                 .when(_d == "5", F.lit("Summer"))
                 .otherwise(F.lit("Fall")),
            ),
        )

    # -----------------------------------------------------------------
    # 1. One plan row per declared programme, per admission.
    #
    #    PS_ACAD_PLAN is effective-dated. Entry term now comes from the
    #    programme's ADMIT_TERM (step 2), not DECLARE_DT, so the grain
    #    is the admission: (student, career number, plan). Keeping
    #    declare_dt in the key would emit two cohort entries carrying
    #    the same admit term.
    #
    #    STDNT_CAR_NBR is a separate admission and a separate cohort
    #    entry: 24521401 holds RADIO at career 1 (admit 2215) and
    #    career 2 (admit 2225), and both count.
    #
    #    Excluded here:
    #      000000  - undeclared
    #      *Z      - pre-program
    #      *G      - waitlist
    #    dim_program excludes Z and G via enrollment_stage, but this
    #    frame joins on the mapped TITLE, so a 105041Z would otherwise
    #    resolve to the real programme and create a cohort entry.
    #    This is also the "no suffix at the end of their Academic Plan
    #    code" qualifier in NWTC's confirmation.
    # -----------------------------------------------------------------
    w_plan = (
        Window
        .partitionBy("EMPLID", "STDNT_CAR_NBR", "ACAD_PLAN")
        .orderBy(F.col("EFFDT").desc(), F.col("EFFSEQ").desc())
    )

    ps_plan_latest_df = (
        ps_plan_df
        .withColumn("ACAD_PLAN", F.trim(F.col("ACAD_PLAN")))
        .withColumn("EMPLID",    F.trim(F.col("EMPLID")))
        .withColumn("declare_dt", F.to_date(F.col("DECLARE_DT")))
        .filter(
            F.col("ACAD_PLAN").isNotNull()
            & (F.col("ACAD_PLAN") != "")
            & (F.col("ACAD_PLAN") != "000000")
            & ~F.col("ACAD_PLAN").endswith("Z")
            & ~F.col("ACAD_PLAN").endswith("G")
            # scope window; also disposes of the 1901-01-01 sentinel
            # and the 114 rows dated to 3201-08-20
            & F.col("declare_dt").between(
                F.lit(PS_SCOPE_START).cast("date"), F.current_date())
        )
        .withColumn("rn", F.row_number().over(w_plan))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    # -----------------------------------------------------------------
    # 2. Admit term, per career track.
    #
    #    PS_ACAD_PLAN hangs off PS_ACAD_PROG by
    #      EMPLID + ACAD_CAREER + STDNT_CAR_NBR (+ EFFDT + EFFSEQ)
    #    so STDNT_CAR_NBR identifies the programme track and carries one
    #    ADMIT_TERM per admission. PS_ACAD_PROG is effective-dated and
    #    ADMIT_TERM is not stable across those rows - see
    #    PS_ADMIT_TERM_RULE above.
    # -----------------------------------------------------------------
    _admit_order = (
        [F.col("EFFDT").desc(), F.col("EFFSEQ").desc()]
        if PS_ADMIT_TERM_RULE == "latest"
        else [F.col("EFFDT").asc(), F.col("EFFSEQ").asc()]
    )

    w_admit = (
        Window
        .partitionBy("EMPLID", "ACAD_CAREER", "STDNT_CAR_NBR")
        .orderBy(*_admit_order)
    )

    ps_admit_df = (
        ps_prog_df
        .withColumn("EMPLID", F.trim(F.col("EMPLID")))
        .withColumn("rn", F.row_number().over(w_admit))
        .filter(F.col("rn") == 1)
        .select(
            F.col("EMPLID").alias("a_emplid"),
            F.col("ACAD_CAREER").alias("a_career"),
            F.col("STDNT_CAR_NBR").alias("a_car_nbr"),
            F.trim(F.col("ADMIT_TERM")).alias("admit_term"),
        )
        .withColumn(
            "entry_period_name",
            _ps_term_to_period_name(F.col("admit_term")),
        )
    )

    # -----------------------------------------------------------------
    # 3. Completion, per career track.
    #
    #    Attributing completion at the STDNT_CAR_NBR grain means a
    #    student who finished one programme and abandoned another is not
    #    marked Complete on both.
    #
    #    PROG_ACTION = 'COMP' is the completion action (Randy's query
    #    used PROG_ACTION <> 'COMP' to find incomplete programmes).
    # -----------------------------------------------------------------
    # -----------------------------------------------------------------
    # COMPLETION DATE SOURCE -- changed 20 Oct 2026 (EGLFIND-144 / RPT-13).
    #
    #    PROG_ACTION='COMP' rows carry EFFDT on the PeopleSoft term END
    #    date -- 12/31 or 5/22. Those dates land in the gaps BETWEEN the
    #    gold academic periods (Fall 2024 ends 12/22, Spring 2025 starts
    #    01/21), so the completion resolved to no period at all and the
    #    student counted as never having completed. 5,248 completions sat
    #    in the Fall 2024 gap alone; Nursing 2023 Fall showed 0 of 80.
    #
    #    COMPLETION_TERM on the SAME ROW states the term outright, so the
    #    date never has to be interpreted. Decode it to a period name and
    #    take that period's END date from the gold dimension, which is
    #    inside the period by construction.
    #
    #    NWTC decision, 19 Oct call: "PeopleSoft: use the completion term
    #    (grad term), not the effective date." PS_TERM_TBL is deliberately
    #    NOT used -- its TERM_END_DT carries the same 12/31 and 5/22 values
    #    that cause the problem.
    #
    #    Grain is UNCHANGED: EMPLID + ACAD_CAREER + STDNT_CAR_NBR, the
    #    latest COMP row per track. Moving to plan grain (COMPLETION_TERM
    #    also exists on PS_ACAD_PLAN) would stop marking un-awarded plans
    #    Complete, but that is a separate number movement and is NOT in
    #    this change.
    #
    #    Rows with a missing or undecodable COMPLETION_TERM, or a term with
    #    no gold period, keep the latest EFFDT -- today's behaviour exactly
    #    -- so no completion is lost by the swap.
    # -----------------------------------------------------------------
    _ps_period_end_df = (
        spark.read.format("delta").load(dim_aca_period)
        .filter(F.col("session").isNull())
        .select(
            F.col("academic_period_name").alias("ct_period_name"),
            F.col("period_end_date").alias("ct_period_end_date"),
        )
        .dropDuplicates(["ct_period_name"])
    )

    # Latest COMP row per track. Equivalent to the previous MAX(EFFDT) for
    # the fallback date, and it also gives us that row's COMPLETION_TERM.
    w_comp = (
        Window
        .partitionBy("EMPLID", "ACAD_CAREER", "STDNT_CAR_NBR")
        .orderBy(F.col("EFFDT").desc(), F.col("EFFSEQ").desc())
    )

    ps_completed_df = (
        ps_prog_df
        .filter(F.col("PROG_ACTION") == "COMP")
        .withColumn("EMPLID", F.trim(F.col("EMPLID")))
        .withColumn("rn", F.row_number().over(w_comp))
        .filter(F.col("rn") == 1)
        .withColumn("comp_effdt", F.to_date(F.col("EFFDT")))
        .withColumn("comp_term", F.trim(F.col("COMPLETION_TERM")))
        .withColumn("comp_period_name",
                    _ps_term_to_period_name(F.col("comp_term")))
        .join(
            _ps_period_end_df,
            F.col("comp_period_name") == F.col("ct_period_name"),
            "left",
        )
        .select(
            F.col("EMPLID").alias("c_emplid"),
            F.col("ACAD_CAREER").alias("c_career"),
            F.col("STDNT_CAR_NBR").alias("c_car_nbr"),
            F.col("comp_term").alias("c_completion_term"),
            F.col("comp_period_name").alias("c_completion_period_name"),
            F.col("comp_effdt").alias("c_legacy_effdt"),
            F.when(F.col("ct_period_end_date").isNotNull(), F.lit(1))
             .otherwise(F.lit(0)).alias("c_term_resolved"),
            # The swap. COALESCE keeps today's behaviour where the term is
            # unusable, so this can only move dates, never remove them.
            F.coalesce(F.col("ct_period_end_date"), F.col("comp_effdt"))
             .alias("ps_completion_date"),
        )
    )

    # Coverage. falling_back large means COMPLETION_TERM is thinner than
    # the 19 Oct call assumed and NWTC needs to hear it.
    _ct_total    = ps_completed_df.count()
    _ct_resolved = ps_completed_df.filter(F.col("c_term_resolved") == 1).count()
    _ct_moved    = ps_completed_df.filter(
        F.col("ps_completion_date") != F.col("c_legacy_effdt")).count()
    print(f"[fact_cohort] PS COMP tracks                : {_ct_total}")
    print(f"[fact_cohort]   term resolved to a period   : {_ct_resolved}")
    print(f"[fact_cohort]   falling back to EFFDT       : {_ct_total - _ct_resolved}")
    print(f"[fact_cohort]   dates actually moved        : {_ct_moved}")

    # -----------------------------------------------------------------
    # 4. Shape to program_of_study_all.
    #
    #    program_of_study carries the MAPPED Workday title because the
    #    programme join below is on NAME:
    #        PG.program_name == POS.program_of_study
    #    Both sides come from PS_PROGRAM_MAP.Workday_Program_Title, so
    #    they agree — see validation query V3.
    #
    #    entry_period_name is the admit-term entry period. It is NULL on
    #    Workday rows, which keep the declare-date window logic.
    # -----------------------------------------------------------------
    ps_pos_df_all = (
        ps_plan_latest_df.alias("p")
        .join(
            ps_xwalk_df.alias("x"),
            F.col("p.ACAD_PLAN") == F.col("x.x_code"),
            "left",             # LEFT, so unresolved codes are counted, not vanished
        )
        .join(
            ps_admit_df,
            (F.col("p.EMPLID")          == F.col("a_emplid"))
            & (F.col("p.ACAD_CAREER")   == F.col("a_career"))
            & (F.col("p.STDNT_CAR_NBR") == F.col("a_car_nbr")),
            "left",
        )
        .join(
            ps_completed_df,
            (F.col("p.EMPLID")        == F.col("c_emplid"))
            & (F.col("p.ACAD_CAREER") == F.col("c_career"))
            & (F.col("p.STDNT_CAR_NBR") == F.col("c_car_nbr")),
            "left",
        )
        .select(
            # STDNT_CAR_NBR is in the key: the same plan can appear under
            # two career numbers, and each is its own admission. declare_dt
            # is NOT in the key — entry term now comes from ADMIT_TERM, so
            # two declarations on one admission are one cohort entry.
            F.sha2(F.concat_ws("||", F.lit("PS"),
                               F.col("p.EMPLID"),
                               F.col("p.STDNT_CAR_NBR").cast("string"),
                               F.col("p.ACAD_PLAN")), 256).alias("pos_key"),
            F.col("p.EMPLID").alias("student_id"),
            F.lit(None).cast("string").alias("academic_unit"),
            F.col("p.declare_dt").alias("declare_date"),
            F.lit(None).cast("date").alias("expected_completion_date"),
            F.col("p.declare_dt").alias("effective_date"),
            # Resolved programme name. Must match dim_program exactly:
            # it is also the precedence key below, so a wrong name stops
            # PeopleSoft superseding the pancaked Workday row and the
            # student is counted twice.
            F.col("x.x_name").alias("program_of_study"),
            F.col("x.x_lineage").alias("program_lineage_key"),
            F.when(F.col("p.PLAN_SEQUENCE") == 1, F.lit("Y"))
             .otherwise(F.lit("N")).alias("is_primary_program"),
            F.lit(None).cast("string").alias("earned_credential"),
            F.lit(None).cast("string").alias("program_type"),
            F.lit(None).cast("string").alias("academic_status"),
            F.col("ps_completion_date").alias("completion_date"),
            F.when(F.col("ps_completion_date").isNotNull(), F.lit("Complete"))
             .otherwise(F.lit("In Progress")).alias("program_status"),
            F.col("entry_period_name").alias("entry_period_name"),
            F.lit(1).alias("is_history"),
        )
    )

    # -----------------------------------------------------------------
    # Unresolved programmes. These used to disappear inside an inner
    # join. Now they are counted and named before being dropped.
    # Expected: the PS plan codes with no declarations since
    # PS_SCOPE_START, which have no live dim_program row to resolve to.
    # A jump here means the crosswalk is stale — rebuild it.
    # -----------------------------------------------------------------
    ps_pos_df_all.cache()
    _ps_all   = ps_pos_df_all.count()
    _ps_unres = ps_pos_df_all.filter(F.col("program_lineage_key").isNull()).count()

    print(f"[fact_cohort] PS declarations before resolve : {_ps_all}")
    print(f"[fact_cohort] PS declarations unresolved     : {_ps_unres}")

    if _ps_unres:
        (ps_pos_df_all
         .filter(F.col("program_lineage_key").isNull())
         .groupBy("program_of_study")
         .agg(F.countDistinct("student_id").alias("students"),
              F.count("*").alias("declarations"))
         .orderBy(F.desc("declarations"))
         .show(40, truncate=False))

    ps_pos_df = ps_pos_df_all.filter(F.col("program_lineage_key").isNotNull())
    ps_pos_df_all.unpersist()

    # Admit-term coverage. Rows with no decoded period fall back to the
    # declare-date window and should be rare; a large number here means
    # the ADMIT_TERM join or the decode needs looking at.
    _ps_total      = ps_pos_df.count()
    _ps_no_admit   = ps_pos_df.filter(F.col("entry_period_name").isNull()).count()
    print(f"[fact_cohort] PS rule                    : admit term ({PS_ADMIT_TERM_RULE} effective row)")
    print(f"[fact_cohort] PS rows with admit period  : {_ps_total - _ps_no_admit} of {_ps_total}")
    print(f"[fact_cohort] PS rows falling back       : {_ps_no_admit}")

    # -----------------------------------------------------------------
    # 5. PRECEDENCE — PeopleSoft wins for pre-cutover history.
    #
    #    Where PS holds declarations for a (student, programme) pair,
    #    the Workday row for that pair is the pancaked version of the
    #    same history and must go, or the wrong entry term survives
    #    alongside the right one.
    #
    #    Scoped to Workday declarations BEFORE the cutover. Anything on
    #    or after it is Workday's own record and is left alone.
    # -----------------------------------------------------------------
    ps_pairs_df = (
        ps_pos_df.select("student_id", "program_of_study").distinct()
        .withColumn("_ps_covers", F.lit(1))
    )

    pos_df = pos_df.withColumn("is_history", F.lit(0))\
                   .withColumn("entry_period_name", F.lit(None).cast("string"))\
                   .withColumn("program_lineage_key", F.lit(None).cast("string"))\
                   .withColumn("_wd_declare_date", F.col("declare_date"))

    pos_wd_kept_df = (
        pos_df.alias("w")
        .join(ps_pairs_df, ["student_id", "program_of_study"], "left")
        .filter(
            F.col("_ps_covers").isNull()                               # PS has nothing here
            | (F.col("w.declare_date") >= F.lit(WD_CUTOVER).cast("date"))  # or it is Workday's own era
        )
        .drop("_ps_covers")
    )

    _wd_before = pos_df.count()
    _wd_after  = pos_wd_kept_df.count()

    pos_df = pos_wd_kept_df.unionByName(ps_pos_df, allowMissingColumns=True)

    print(f"[fact_cohort] PS declarations in scope   : {_ps_total}")
    print(f"[fact_cohort] Workday rows before        : {_wd_before}")
    print(f"[fact_cohort] Workday rows superseded    : {_wd_before - _wd_after}")
    print(f"[fact_cohort] pos_df total after union   : {pos_df.count()}")

    # Read and select student dimensions
    dim_student_df = (
        spark.read.format("delta")
        .load(students_path)
        .select("student_key", "student_id")
    )

    # Read and select college dimensions with alias for academic_unit
    dim_college_df = (
        spark.read.format("delta")
        .load(dim_college_path)
        .selectExpr("college_key", "name as academic_unit")
    )

    # Read date dimension (kept for future joins if needed)
    dim_date_df = (
        spark.read.format("delta").load(dim_date_path)
        .select("date_key", "calendar_date")
    )

    # ---------------------------------------------------------------
    # FIX B - widen each term's window to span its sessions.
    #
    # term_start_date / term_end_date span every session of the term, so
    # a declaration anywhere in the term - including the pre-term
    # registration window - resolves to the main-term key.
    # ---------------------------------------------------------------
    term_bounds_df = (
        spark.read.format("delta").load(dim_aca_period)
        .select(
            F.col("academic_period_name"),
            F.col("declare_start_date").alias("term_start_date"),
            F.col("declare_end_date").alias("term_end_date"),
        )
    )

    dim_acad_period_df = (
        spark.read.format("delta").load(dim_aca_period)
        .filter(F.col("session").isNull())
        .selectExpr(
            "academic_period_key",
            "academic_period_name",
            "declare_start_date",
            "declare_end_date",
        )
        .join(term_bounds_df, "academic_period_name", "left")
    )

    # ---------------------------------------------------------------
    # Build joined_df (program enrollments enriched with dims)
    # ---------------------------------------------------------------
    joined_df = (
        pos_df.alias("POS")
        .join(dim_student_df, on="student_id", how="left")
        .join(dim_college_df, on="academic_unit", how="left")
        .join(
            dim_date_df.alias("DDT"),
            F.col("POS.declare_date") == F.col("DDT.calendar_date"),
            how="left",
        )
        .join(
            dim_date_df.alias("EDT"),
            F.col("POS.expected_completion_date") == F.col("EDT.calendar_date"),
            how="left",
        )
        .join(
            dim_date_df.alias("EFDT"),
            F.col("POS.effective_date") == F.col("EFDT.calendar_date"),
            how="left",
        )
        .join(
            # PeopleSoft rows resolve on the admit-term period name.
            # Workday rows keep FIX C - containment against the widened
            # term window. A PS row whose ADMIT_TERM did not decode has a
            # NULL entry_period_name and falls back to the window too.
            dim_acad_period_df.alias("ACA"),
            F.when(
                F.col("POS.entry_period_name").isNotNull(),
                F.col("ACA.academic_period_name") == F.col("POS.entry_period_name"),
            ).otherwise(
                (F.col("POS.declare_date") >= F.col("ACA.term_start_date"))
                & (F.col("POS.declare_date") <= F.col("ACA.term_end_date"))
            ),
            "left",
        )
        .join(
            # Programme version in force at DECLARATION, not at admission.
            # Stated choice: it is the programme the student declared into.
            program_df.alias("PG"),
            # PeopleSoft rows resolve on program_lineage_key (from the
            # crosswalk) AND the programme name; Workday rows keep the
            # name match.
            #
            # EGLFIND-144: lineage ALONE is not unique. dim_program holds 25
            # rows under lineage 105431 - the degree, waitlist, pre-program
            # and a dozen bridge tracks - and ~15 are open
            # 1900-01-01..9999-12-31. Matching on lineage + dates made a
            # single declaration join ALL of them; the pos_key collapse below
            # then kept an arbitrary row, usually a Waitlist or Pre-program
            # one, which cell 14 drops as cohort_exclusion_reason='No cohort'.
            # 110 PS Nursing rows survived as 9.
            #
            # The crosswalk carries the MATCHED row's own name in x_name
            # (-> POS.program_of_study), so requiring the name costs nothing
            # and restores selectivity. Lineage stays in the predicate to
            # guard the shared-name case (three live programmes are called
            # 'Fire Science').
            F.when(
                F.col("POS.program_lineage_key").isNotNull(),
                (F.col("PG.program_lineage_key") == F.col("POS.program_lineage_key"))
                & (F.col("PG.program_name") == F.col("POS.program_of_study")),
            ).otherwise(
                F.col("PG.program_name") == F.col("POS.program_of_study")
            )
            & (F.col("PG.effective_date") <= F.col("POS.declare_date"))
            & (F.col("PG.expiration_date") >= F.col("POS.declare_date")),
            "left",
        )
        .selectExpr(
            "POS.pos_key AS program_enrollment_key",
            "student_key",
            "college_key",
            "PG.program_key",
            "PG.program_name",
            "DDT.date_key AS declare_date_key",
            "EDT.date_key AS expected_completion_date_key",
            "EFDT.date_key AS effective_date_key",
            "ACA.academic_period_key AS entry_academic_period_key",
            "POS.is_primary_program",
            "POS.earned_credential",
            "POS.program_type",
            "POS.academic_status",
            "POS.completion_date",
            "POS.is_history",
        )
    )


    w_period = (
        Window
        .partitionBy("program_enrollment_key")
        .orderBy(F.col("entry_academic_period_key").asc_nulls_last())
    )

    joined_df = (
        joined_df
        .withColumn("period_rn", F.row_number().over(w_period))
        .filter(F.col("period_rn") == 1)
        .drop("period_rn")
    )

    # ---------------------------------------------------------------
    # Diagnostics - both NULLs are dropped by the inner joins in cte_df,
    # so they are silent losses unless counted here.
    # ---------------------------------------------------------------
    jd_total = joined_df.count()
    jd_null_period = joined_df.filter(
        F.col("entry_academic_period_key").isNull()).count()
    jd_null_program = joined_df.filter(
        F.col("program_key").isNull()).count()
    jd_null_prog_ps = joined_df.filter(
        F.col("program_key").isNull() & (F.col("is_history") == 1)).count()

    print(f"joined_df rows            : {jd_total}")
    print(f"  null entry period       : {jd_null_period} "
          f"({100.0 * jd_null_period / jd_total:.1f}%)")
    print(f"  null program_key        : {jd_null_program} "
          f"({100.0 * jd_null_program / jd_total:.1f}%)")
    print(f"    of which PeopleSoft   : {jd_null_prog_ps}"
          "   <- should be 0; the crosswalk resolved these")

    if jd_total and jd_null_period / jd_total > 0.05:
        print("  WARNING: more than 5% of rows have no entry period - "
              "check declare_date coverage against the term windows")

    # ---------------------------------------------------------------
    # Build paths for gold layer tables
    # ---------------------------------------------------------------
    dim_program_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_program",
    )

    dim_academic_period_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_academic_period",
    )

    dim_cohort_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_cohort",
    )

    ref_map_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_ref_cohort_measurement_map",
    )



except Exception as e:
    raise Exception(f"Failed to create/update fact_cohort_retention table: {e}")    
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# program_cohort

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable


dim_ap_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
dim_ap_df = spark.read.format("delta").load(dim_ap_path)

try:
    # ---------------------------------------------------------------
    # Read source tables for base fact and PASS_B (program-level)
    # ---------------------------------------------------------------
    # Program-enrollment-like base is the program-level joined_df built in the Program_enrollment cell
    pe_df = joined_df

    # Use the full date dimension here
    date_df = spark.read.format("delta").load(dim_date_path)
    program_df = spark.read.format("delta").load(dim_program_path)
    period_df = spark.read.format("delta").load(dim_ap_path)
    cohort_df = spark.read.format("delta").load(dim_cohort_path)
    map_df = spark.read.format("delta").load(ref_map_path)

    # Completion fact and registration fact from earlier cells
    completion_df = fact_complition_df
    enrollment_df = joined_df

    # Student dimension (to get student_id)
    dim_student_df = (
        spark.read.format("delta")
        .load(students_path)
        .select("student_key", "student_id")
    )

    # ---------------------------------------------------------------
    # Derive completion_1/2/3 date keys per student + program lineage
    # and first_completion_date_key (Y1–Y4 ready)
    # ---------------------------------------------------------------
    # completion_df already has completion_date_key (int), program_key, student_key
    comp_with_lineage = (
        completion_df.alias("fc")
        .join(
            program_df.alias("cp"),
            F.col("fc.program_key") == F.col("cp.program_key"),
            "inner",
        )
        .select(
            F.col("fc.student_key").alias("student_key"),
            F.col("cp.program_lineage_key").alias("program_lineage_key"),
            F.col("fc.completion_date_key").alias("completion_date_key"),
        )
    )

    # First completion per student + lineage
    completion_family_df = (
        comp_with_lineage
        .groupBy("student_key", "program_lineage_key")
        .agg(F.min("completion_date_key").alias("first_completion_date_key"))
    )

    # Top 3 completion keys per student + lineage
    w_comp = (
        Window
        .partitionBy("student_key", "program_lineage_key")
        .orderBy("completion_date_key")
    )

    ranked_comp = comp_with_lineage.withColumn(
        "completion_rank",
        F.row_number().over(w_comp)
    )

    completion_keys_df = (
        ranked_comp
        .filter(F.col("completion_rank") <= 3)
        .groupBy("student_key", "program_lineage_key")
        .pivot("completion_rank", [1, 2, 3])
        .agg(F.first("completion_date_key"))
        .select(
            "student_key",
            "program_lineage_key",
            F.col("1").alias("completion_1_date_key"),
            F.col("2").alias("completion_2_date_key"),
            F.col("3").alias("completion_3_date_key"),
        )
    )

    # ---------------------------------------------------------------
    # CTE equivalent (base population at program level)
    # ---------------------------------------------------------------
    cte_df = (
        pe_df.alias("pe")
        .join(
            program_df.alias("p"),
            F.col("pe.program_key") == F.col("p.program_key"),
            "inner",
        )
        .join(
            period_df.alias("ap"),
            F.col("ap.academic_period_key")
            == F.col("pe.entry_academic_period_key"),
            "inner",
        )
        .join(
            date_df.alias("dt"),
            F.col("dt.calendar_date") == F.col("pe.completion_date"),
            "left",
        )
        .filter(F.col("p.enrollment_stage") == "Enrolled")
        .filter(
            F.col("p.cohort_exclusion_reason").isin(
                "Direct", "Roll up to parent"
            )
        )
        .filter(~F.col("pe.program_name").like("%Undeclared%"))
        .select(
            F.col("pe.student_key"),
            F.col("pe.declare_date_key"),
            F.col("ap.period_start_date"),
            F.col("ap.academic_period_name").alias("entry_academic_period_name"),  # alias to avoid ambiguity later
            F.col("pe.entry_academic_period_key"),
            F.col("pe.program_key"),
            F.col("p.program_code"),
            F.col("p.program_lineage_key"),
            F.col("p.program_duration_years").alias("entry_program_duration_years"),  # NEW
            F.col("pe.is_primary_program"),
            F.col("pe.college_key"),
            F.col("dt.date_key"),
            F.col("pe.program_name").alias("program_name"),
        )
    )

    # ---------------------------------------------------------------
    # cnt equivalent (cohort assignment)
    # ---------------------------------------------------------------
    cnt_df = (
        cte_df.alias("cte")
        .join(
            cohort_df.alias("c"),
            (F.col("c.cohort_type") == "Program")
            & (F.col("c.program_lineage_key")
               == F.col("cte.program_lineage_key"))
            & (F.col("c.academic_period_name")
               == F.col("cte.entry_academic_period_name")),  # use entry_academic_period_name to avoid ambiguity
            "inner",
        )
        .select(
            F.col("cte.student_key"),
            F.col("c.cohort_key"),
            F.col("cte.entry_academic_period_key").alias(
                "academic_period_key"),
            F.col("cte.program_key"),
            F.col("cte.period_start_date"),
            F.col("cte.program_code"),
            F.col("cte.program_lineage_key"),
            F.col("cte.entry_program_duration_years"),  # NEW
            F.col("cte.is_primary_program"),
            F.col("cte.declare_date_key"),
            F.col("cte.entry_academic_period_name").alias("academic_period_name"),
            F.col("cte.college_key"),
            F.lit(1).alias("cohort_count"),
            F.col("cte.program_name"),
        )
    )

    # ---------------------------------------------------------------
    # PASS_B due dataset, completion, switches and retention
    # ---------------------------------------------------------------

    # Build due dataset from cnt_df, supporting Y1-Y4 when available
    map_cols = set(map_df.columns)

    has_y3y4 = {
        "y3_academic_period_key",
        "y4_academic_period_key"
    }.issubset(map_cols)

    # ===============================================================
    # 1. BUILD BASE DUE DATASET (add term2 + alt periods)
    # ===============================================================

    if has_y3y4:

        base_due_df = (
            cnt_df.alias("f")
            .join(
                cohort_df.alias("c"),
                F.col("f.cohort_key") == F.col("c.cohort_key"),
                "inner",
            )
            .join(
                period_df.alias("entry_ap"),
                F.col("c.academic_period_name")
                == F.col("entry_ap.academic_period_name"),
                "inner",
            )
            .join(
                map_df.alias("m"),
                F.col("m.entry_academic_period_key")
                == F.col("entry_ap.academic_period_key"),
                "inner",
            )
            .join(
                period_df.alias("y1"),
                F.col("y1.academic_period_key")
                == F.col("m.y1_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("y2"),
                F.col("y2.academic_period_key")
                == F.col("m.y2_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("term2"),
                F.col("term2.academic_period_key")
                == F.col("m.term2_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("t2a"),
                F.col("t2a.academic_period_key")
                == F.col("m.term2_alt_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("y1a"),
                F.col("y1a.academic_period_key")
                == F.col("m.y1_alt_academic_period_key"),
                "left",
            )
            .select(
                F.col("f.cohort_key"),
                F.col("f.student_key"),
                F.col("f.program_lineage_key"),
                F.col("f.entry_program_duration_years"),                       # NEW

                F.col("entry_ap.academic_period_key").alias(
                    "entry_academic_period_key"
                ),

                # NEW: actual entry-term start date (window arithmetic needs a
                # real date, not just the yyyyMMdd int key below).
                F.col("entry_ap.period_start_date").alias(
                    "entry_period_start_date"
                ),

                # ---------------------------------------------------
                # Y1/Y2/Y3/Y4/Term2 academic period keys
                # ---------------------------------------------------
                F.col("m.y1_academic_period_key").alias(
                    "y1_academic_period_key"
                ),

                F.col("m.y2_academic_period_key").alias(
                    "y2_academic_period_key"
                ),

                F.col("m.y3_academic_period_key").alias(
                    "y3_academic_period_key"
                ),

                F.col("m.y4_academic_period_key").alias(
                    "y4_academic_period_key"
                ),

                F.col("m.term2_academic_period_key").alias(
                    "term2_academic_period_key"
                ),

                # Alt academic period keys
                F.col("m.term2_alt_academic_period_key").alias(
                    "term2_alt_academic_period_key"
                ),
                F.col("m.y1_alt_academic_period_key").alias(
                    "y1_alt_academic_period_key"
                ),

                # ---------------------------------------------------
                # Entry period start date used to identify switches
                # after the student's entry period
                # ---------------------------------------------------
                F.date_format(
                    F.col("entry_ap.period_start_date"),
                    "yyyyMMdd",
                ).cast("int").alias(
                    "entry_period_start_date_key"
                ),

                # ---------------------------------------------------
                # Existing Y1/Y2 measurement dates + Term2
                # ---------------------------------------------------
                F.col("y1.pell_recalculation_date").alias(
                    "y1_measure_date"
                ),

                F.col("y2.pell_recalculation_date").alias(
                    "y2_measure_date"
                ),

                F.col("term2.pell_recalculation_date").alias(
                    "term2_measure_date"
                ),

                # Alt measurement dates
                F.col("t2a.pell_recalculation_date").alias(
                    "term2_alt_measure_date"
                ),
                F.col("y1a.pell_recalculation_date").alias(
                    "y1_alt_measure_date"
                ),
            )
        )

        base = base_due_df.alias("base")

        due_df = (
            base
            .join(
                period_df.alias("y3"),
                F.col("y3.academic_period_key")
                == F.col("base.y3_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("y4"),
                F.col("y4.academic_period_key")
                == F.col("base.y4_academic_period_key"),
                "left",
            )
            .withColumn(
                "y3_measure_date",
                F.col("y3.pell_recalculation_date"),
            )
            .withColumn(
                "y4_measure_date",
                F.col("y4.pell_recalculation_date"),
            )
        )

    else:

        base_due_df = (
            cnt_df.alias("f")
            .join(
                cohort_df.alias("c"),
                F.col("f.cohort_key") == F.col("c.cohort_key"),
                "inner",
            )
            .join(
                period_df.alias("entry_ap"),
                F.col("c.academic_period_name")
                == F.col("entry_ap.academic_period_name"),
                "inner",
            )
            .join(
                map_df.alias("m"),
                F.col("m.entry_academic_period_key")
                == F.col("entry_ap.academic_period_key"),
                "inner",
            )
            .join(
                period_df.alias("y1"),
                F.col("y1.academic_period_key")
                == F.col("m.y1_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("y2"),
                F.col("y2.academic_period_key")
                == F.col("m.y2_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("term2"),
                F.col("term2.academic_period_key")
                == F.col("m.term2_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("t2a"),
                F.col("t2a.academic_period_key")
                == F.col("m.term2_alt_academic_period_key"),
                "left",
            )
            .join(
                period_df.alias("y1a"),
                F.col("y1a.academic_period_key")
                == F.col("m.y1_alt_academic_period_key"),
                "left",
            )
            .select(

                # ---------------------------------------------------
                # Existing columns
                # ---------------------------------------------------
                F.col("f.cohort_key"),
                F.col("f.student_key"),
                F.col("f.program_lineage_key"),
                F.col("f.entry_program_duration_years"),  # NEW

                F.col("entry_ap.academic_period_key").alias(
                    "entry_academic_period_key"
                ),

                # NEW: actual entry-term start date
                F.col("entry_ap.period_start_date").alias(
                    "entry_period_start_date"
                ),

                # ---------------------------------------------------
                # Y1/Y2 academic periods
                # ---------------------------------------------------
                F.col("m.y1_academic_period_key").alias(
                    "y1_academic_period_key"
                ),

                F.col("m.y2_academic_period_key").alias(
                    "y2_academic_period_key"
                ),

                # ---------------------------------------------------
                # No Y3/Y4 in this version
                # ---------------------------------------------------
                F.lit(None).cast("long").alias(
                    "y3_academic_period_key"
                ),

                F.lit(None).cast("long").alias(
                    "y4_academic_period_key"
                ),

                # Term2 academic period key
                F.col("m.term2_academic_period_key").alias(
                    "term2_academic_period_key"
                ),

                # Alt academic period keys
                F.col("m.term2_alt_academic_period_key").alias(
                    "term2_alt_academic_period_key"
                ),
                F.col("m.y1_alt_academic_period_key").alias(
                    "y1_alt_academic_period_key"
                ),

                # ---------------------------------------------------
                # Entry period start date
                # ---------------------------------------------------
                F.date_format(
                    F.col("entry_ap.period_start_date"),
                    "yyyyMMdd",
                ).cast("int").alias(
                    "entry_period_start_date_key"
                ),

                # ---------------------------------------------------
                # Y1/Y2 measurement dates + Term2
                # ---------------------------------------------------
                F.col("y1.pell_recalculation_date").alias(
                    "y1_measure_date"
                ),

                F.col("y2.pell_recalculation_date").alias(
                    "y2_measure_date"
                ),

                F.col("term2.pell_recalculation_date").alias(
                    "term2_measure_date"
                ),

                # Alt measurement dates
                F.col("t2a.pell_recalculation_date").alias(
                    "term2_alt_measure_date"
                ),
                F.col("y1a.pell_recalculation_date").alias(
                    "y1_alt_measure_date"
                ),
            )
        )

        due_df = (
            base_due_df
            .withColumn(
                "y3_measure_date",
                F.lit(None).cast("date"),
            )
            .withColumn(
                "y4_measure_date",
                F.lit(None).cast("date"),
            )
        )

    # ===============================================================
    # 2. COMPLETION IN SAME PROGRAM LINEAGE
    # ===============================================================

    completion_family_df = (
        completion_df.alias("fc")
        .join(
            program_df.alias("cp"),
            F.col("fc.program_key")
            == F.col("cp.program_key"),
            "inner",
        )
        .groupBy(
            F.col("fc.student_key"),
            F.col("cp.program_lineage_key"),
        )
        .agg(
            F.min(
                "fc.completion_date_key"
            ).alias(
                "first_completion_date_key"
            )
        )
    )

    # ===============================================================
    # 3. QUALIFYING PROGRAM SWITCHES
    # ===============================================================

    switch_df = (
        enrollment_df.alias("pe")
        .join(
            program_df.alias("p2"),
            F.col("pe.program_key")
            == F.col("p2.program_key"),
            "inner",
        )
        .filter(
            F.col("p2.enrollment_stage") == "Enrolled"
        )
        .filter(
            F.col("p2.cohort_exclusion_reason").isin(
                "Direct",
                "Roll up to parent",
            )
        )
        .select(
            F.col("pe.student_key").alias(
                "switch_student_key"
            ),

            F.col("pe.declare_date_key").alias(
                "switch_declare_date_key"
            ),

            F.col("p2.program_lineage_key").alias(
                "switch_lineage"
            ),
        )
    )

    # ===============================================================
    # 4. REGISTRATION DATA (add term2 + alt points)
    # ===============================================================

    reg = (
        fact_reg_audit_df.alias("r")
        .where(F.col("r.registration_status").isin("Completed", "Registered"))
        .where(F.col("r.credits") >= 1)
        .join(
            dim_ap_df.alias("rp"),
            F.col("r.academic_period_key") == F.col("rp.academic_period_key"),
            "inner",
        )
        .join(
            dim_ap_df.alias("main"),
            (F.col("main.academic_period_name") == F.col("rp.academic_period_name"))
        & F.col("main.session").isNull(),
            "inner",
        )
        .select(
            F.col("r.student_key"),
            F.col("main.academic_period_key").alias("academic_period_key"),
        )
        .distinct()
        )

    # ===============================================================
    # 5. ADD Y1/Y2/Y3/Y4/TERM2 + ALT REGISTRATION FLAGS TO DUE_DF
    # ===============================================================

    for yr in ["y1", "y2", "y3", "y4", "term2", "term2_alt", "y1_alt"]:

        r = (
            reg
            .select(
                F.col("student_key").alias(
                    f"{yr}_sid"
                ),

                F.col("academic_period_key").alias(
                    f"{yr}_apk"
                ),
            )
            .withColumn(
                f"registered_{yr}_flag",
                F.lit(1),
            )
        )

        due_df = (
            due_df
            .join(
                r,
                (
                    F.col("student_key")
                    == F.col(f"{yr}_sid")
                )
                &
                (
                    F.col(
                        f"{yr}_academic_period_key"
                    )
                    ==
                    F.col(f"{yr}_apk")
                ),
                "left",
            )
            .drop(
                f"{yr}_sid",
                f"{yr}_apk",
            )
        )

    # ===============================================================
    # 6. RETENTION FUNCTION (extended for alt_col and alt_md_col)
    # ===============================================================

    def retained_flag(yr, alt_col=None, alt_md_col=None):
        # effective measure date: the alt term's PRD when an alt exists
        if alt_md_col is not None:
            md = F.coalesce(F.col(alt_md_col), F.col(f"{yr}_measure_date"))
            mdk = F.coalesce(
                F.date_format(F.col(alt_md_col), "yyyyMMdd").cast("int"),
                F.col(f"{yr}_measure_date_key"),
            )
        else:
            md = F.col(f"{yr}_measure_date")
            mdk = F.col(f"{yr}_measure_date_key")

        # Base registration evidence for this term/year
        registered = F.col(f"registered_{yr}_flag") == 1

        # Optional alternate registration flag (e.g., summer alt term)
        if alt_col is not None:
            registered = registered | (F.col(alt_col) == 1)

        return (
            F.when(
                md.isNull() | (md > F.current_date()),
                F.lit(None).cast("int"),
            )
            .when(
                F.col("first_completion_date_key") <= mdk,
                F.lit(1),
            )
            .when(
                F.col("switch_target_lineage_key").isNotNull()
                & (F.col("switch_declare_date_key") <= mdk),
                F.lit(0),
            )
            .when(registered, F.lit(1))
            .otherwise(F.lit(0))
        )

    # helper for per-point completion flags
    def comp_flag(yr):
        return (
            F.when(
                F.col(f"{yr}_measure_date").isNull(),
                F.lit(None).cast("int"),
            )
            .when(
                F.col("first_completion_date_key")
                <= F.col(f"{yr}_measure_date_key"),
                F.lit(1),
            )
            .otherwise(F.lit(0))
        )

    # ------------------------------------------------------------------
    # Completion within N% of normal time.
    #
    # The window is anchored to the COHORT ENTRY term and the duration of
    # the programme the student ENTERED -- not to the completion record's
    # own declare date or programme. A 1-year diploma and a 2-year
    # associate therefore get different deadlines from the same code.
    #
    # NULL means the window has not yet elapsed (not measurable).
    # 0 means the window closed without a qualifying completion.
    # ------------------------------------------------------------------
    def completed_within(pct):
        window_end = F.add_months(
            F.col("entry_period_start_date"),
            (F.col("entry_program_duration_years") * F.lit(pct * 12)).cast("int"),
        )
        window_end_key = F.date_format(window_end, "yyyyMMdd").cast("int")

        return (
            F.when(
                F.col("entry_program_duration_years").isNull()
                | F.col("entry_period_start_date").isNull(),
                F.lit(None).cast("int"),
            )
            # EGLFIND-233, 20 Oct 2026. The completion test MUST come
            # before the not-yet-elapsed test.
            #
            # A 2-year programme's 400% window runs eight years. For any
            # recent cohort it has not closed, so the old order returned
            # NULL for students who had ALREADY graduated -- and the 400%
            # count fell below the 100% count. Trista, 19 Oct: "I would
            # expect this N size to keep growing." Sarah: "I would expect
            # the numbers to continue to go up over time."
            #
            # Testing the completion first makes 100 -> 150 -> 200 -> 400
            # monotonic by construction: a completion inside the window is
            # knowable now whether or not the window has closed.
            .when(F.col("first_completion_date_key") <= window_end_key, F.lit(1))
            .when(window_end > F.current_date(), F.lit(None).cast("int"))
            .when(F.col("first_completion_date_key").isNull(), F.lit(0))
            .otherwise(F.lit(0))
        )

    # ===============================================================
    # 7. CALCULATE PASS_B METRICS (add term2 flags + completion flags)
    # ===============================================================

    upd_df = (
        due_df.alias("d")
        .join(
            completion_family_df.alias("cf"),
            (
                (
                    F.col("d.student_key")
                    ==
                    F.col("cf.student_key")
                )
                &
                (
                    F.col("d.program_lineage_key")
                    ==
                    F.col("cf.program_lineage_key")
                )
            ),
            "left",
        )
        .join(
            switch_df.alias("sw"),
            (
                (
                    F.col("d.student_key")
                    ==
                    F.col("sw.switch_student_key")
                )
                &
                (
                    F.col("sw.switch_lineage")
                    !=
                    F.col("d.program_lineage_key")
                )
                &
                (
                    F.col("sw.switch_declare_date_key")
                    >
                    F.col("d.entry_period_start_date_key")
                )
            ),
            "left",
        )
        .groupBy(
            "d.cohort_key",
            "d.student_key",
            "d.program_lineage_key",
            "d.entry_program_duration_years",  # NEW
            "d.entry_period_start_date",       # NEW
            "d.y1_measure_date",
            "d.y2_measure_date",
            "d.y3_measure_date",
            "d.y4_measure_date",
            "d.term2_measure_date",
            "d.term2_alt_measure_date",
            "d.y1_alt_measure_date",
            "d.y1_academic_period_key",
            "d.y2_academic_period_key",
            "d.y3_academic_period_key",
            "d.y4_academic_period_key",
            "d.term2_academic_period_key",
            "d.term2_alt_academic_period_key",
            "d.y1_alt_academic_period_key",
            "d.registered_y1_flag",
            "d.registered_y2_flag",
            "d.registered_y3_flag",
            "d.registered_y4_flag",
            "d.registered_term2_flag",
            "d.registered_term2_alt_flag",
            "d.registered_y1_alt_flag",
            "cf.first_completion_date_key",
        )
        .agg(
            F.min(
                "sw.switch_declare_date_key"
            ).alias(
                "switch_declare_date_key"
            ),
            F.first(
                "sw.switch_lineage",
                ignorenulls=True,
            ).alias(
                "switch_target_lineage_key"
            ),
        )
        .withColumn(
            "y1_measure_date_key",
            F.when(
                F.col("y1_measure_date").isNotNull(),
                F.date_format(
                    "y1_measure_date",
                    "yyyyMMdd",
                ).cast("int"),
            ),
        )
        .withColumn(
            "y2_measure_date_key",
            F.when(
                F.col("y2_measure_date").isNotNull(),
                F.date_format(
                    "y2_measure_date",
                    "yyyyMMdd",
                ).cast("int"),
            ),
        )
        .withColumn(
            "y3_measure_date_key",
            F.when(
                F.col("y3_measure_date").isNotNull(),
                F.date_format(
                    "y3_measure_date",
                    "yyyyMMdd"),
            ).cast("int"),
        )
        .withColumn(
            "y4_measure_date_key",
            F.when(
                F.col("y4_measure_date").isNotNull(),
                F.date_format(
                    "y4_measure_date",
                    "yyyyMMdd"),
            ).cast("int"),
        )
        .withColumn(
            "term2_measure_date_key",
            F.when(
                F.col("term2_measure_date").isNotNull(),
                F.date_format(
                    "term2_measure_date",
                    "yyyyMMdd"),
            ).cast("int"),
        )
        # New retained flags with alt maturity guards
        .withColumn(
            "retained_term2_flag",
            retained_flag(
                "term2",
                alt_col="registered_term2_alt_flag",
                alt_md_col="term2_alt_measure_date",
            ),
        )
        .withColumn(
            "retained_y1_flag",
            retained_flag(
                "y1",
                alt_col="registered_y1_alt_flag",
                alt_md_col="y1_alt_measure_date",
            ),
        )
        .withColumn("retained_y2_flag", retained_flag("y2"))
        .withColumn("retained_y3_flag", retained_flag("y3"))
        .withColumn("retained_y4_flag", retained_flag("y4"))
        .withColumn(
            "completed_by_term2_flag",
            F.when(
                F.col("term2_measure_date").isNull(),
                F.lit(None).cast("int"),
            )
            .when(
                F.col("first_completion_date_key")
                <= F.col("term2_measure_date_key"),
                F.lit(1),
            )
            .otherwise(F.lit(0)),
        )
        # Term7/10/13 completion flags (primary measure dates only)
        .withColumn("completed_by_term7_flag", comp_flag("y2"))
        .withColumn("completed_by_term10_flag", comp_flag("y3"))
        .withColumn("completed_by_term13_flag", comp_flag("y4"))
        # -----------------------------------------------------------
        # Completion-within-N%-of-normal-time flags (entry-anchored)
        # -----------------------------------------------------------
        .withColumn("completed_within_100_flag", completed_within(1.0))
        .withColumn("completed_within_150_flag", completed_within(1.5))
        .withColumn("completed_within_200_flag", completed_within(2.0))
        .withColumn("completed_within_400_flag", completed_within(4.0))
        # -----------------------------------------------------------
        # FIX E - the cascading NULL rule has been REMOVED.
        # -----------------------------------------------------------
        .withColumn(
            "completed_in_family_by_measure_flag",
            F.when(
                F.col("y1_measure_date").isNull(),
                F.lit(None).cast("int"),
            )
            .when(
                F.col("first_completion_date_key")
                <=
                F.col("y1_measure_date_key"),
                F.lit(1),
            )
            .otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "first_qualifying_completion_date_key",
            F.col("first_completion_date_key"),
        )
        .withColumn(
            "exit_reason",
            F.when(
                F.col("retained_y1_flag").isNull(),
                F.lit(None).cast("string"),
            )
            .when(
                (
                    F.col(
                        "completed_in_family_by_measure_flag"
                    ) == 1
                )
                &
                (
                    F.col(
                        "switch_target_lineage_key"
                    ).isNull()
                ),
                F.lit("Graduated"),
            )
            .when(
                F.col(
                    "completed_in_family_by_measure_flag"
                ) == 1,
                F.lit("Progression"),
            )
            .when(
                F.col("switch_target_lineage_key").isNotNull()
                & (F.col("retained_y1_flag") == 0),
                F.lit("Program Switch"),
            )
            .when(
                F.col("retained_y1_flag") == 0,
                F.lit("Not Retained"),
            )
            .otherwise(
                F.lit(None).cast("string")
            ),
        )
        .withColumn(
            "last_measured_date",
            F.current_date(),
        )
        .withColumn(
            "modified_on",
            F.current_timestamp(),
        )
        .withColumn(
            "modified_by",
            F.lit("PASS_B"),
        )
    )

    # ===============================================================
    # 14. COMBINE BASE FACT WITH PASS_B METRICS
    # ===============================================================

    base_final_df = (
        cnt_df.alias("f")
        .join(
            upd_df.alias("u"),
            [
                "cohort_key",
                "student_key",
            ],
            "left",
        )
        .join(
            dim_student_df.alias("s"),
            F.col("f.student_key")
            == F.col("s.student_key"),
            "left",
        )
        .join(
            program_df.alias("p"),
            F.col("f.program_key") == F.col("p.program_key"),
            "left",
        )
        .select(
            F.col("f.student_key").alias("study_student_key"),  # renamed key
            F.col("s.student_id").alias("student_id"),
            F.col("f.cohort_key").alias("study_cohort_key"),    # renamed key
            F.col("f.academic_period_key").alias("academic_period_key"),
            F.col("f.program_key").alias("program_key"),
            F.col("f.period_start_date").alias("period_start_date"),
            F.col("f.program_code").alias("program_code"),
            F.col("f.program_lineage_key").alias("program_lineage_key"),
            F.col("f.is_primary_program").alias("is_primary_program"),
            F.col("f.declare_date_key").alias("declare_date_key"),
            F.col("f.cohort_count").alias("cohort_count"),
            F.col("f.academic_period_name").alias("academic_period_name"),
            F.col("f.college_key").alias("college_key"),
            F.col("u.retained_term2_flag").alias("retained_term2_flag"),
            F.col("u.completed_by_term2_flag").alias("completed_by_term2_flag"),
            F.col("u.completed_by_term7_flag").alias("completed_by_term7_flag"),
            F.col("u.completed_by_term10_flag").alias("completed_by_term10_flag"),
            F.col("u.completed_by_term13_flag").alias("completed_by_term13_flag"),
            F.col("u.retained_y1_flag").alias("retained_y1_flag"),
            F.col("u.retained_y2_flag").alias("retained_y2_flag"),
            F.col("u.retained_y3_flag").alias("retained_y3_flag"),
            F.col("u.retained_y4_flag").alias("retained_y4_flag"),
            F.col("u.completed_in_family_by_measure_flag").alias("completed_in_family_by_measure_flag"),
            F.col("u.completed_within_100_flag").alias("completed_within_100_flag"),
            F.col("u.completed_within_150_flag").alias("completed_within_150_flag"),
            F.col("u.completed_within_200_flag").alias("completed_within_200_flag"),
            F.col("u.completed_within_400_flag").alias("completed_within_400_flag"),
            F.col("u.first_qualifying_completion_date_key").alias("completion_date_key"),
            F.col("u.switch_target_lineage_key").alias("switch_target_lineage_key"),
            F.col("u.exit_reason").alias("exit_reason"),
            F.col("u.last_measured_date").alias("last_measured_date"),
            F.col("u.modified_on").alias("modified_on"),
            F.col("u.modified_by").alias("modified_by"),
            # expose academic period keys for term4/7/10/13 + term2
            F.col("u.y1_academic_period_key").alias("term4_academic_period_key"),
            F.col("u.y2_academic_period_key").alias("term7_academic_period_key"),
            F.col("u.y3_academic_period_key").alias("term10_academic_period_key"),
            F.col("u.y4_academic_period_key").alias("term13_academic_period_key"),
            F.col("u.term2_academic_period_key").alias("term2_academic_period_key"),
            F.col("f.entry_program_duration_years").alias("entry_program_duration_years"),
            F.col("p.program_name").alias("program_name"),
        )
    )

    # ===============================================================
    # 15. JOIN COMPLETION KEYS
    # ===============================================================

    final_df = (
        base_final_df.alias("f")
        .join(
            completion_keys_df.alias("ck"),
            [
                F.col("f.study_student_key")  # use renamed key
                == F.col("ck.student_key"),
                F.col("f.program_lineage_key")
                == F.col("ck.program_lineage_key"),
            ],
            "left",
        )
        .select(
            F.col("f.study_student_key").alias("student_key"),  # rename back
            F.col("f.student_id").alias("student_id"),
            F.col("f.study_cohort_key").alias("cohort_key"),   # rename back
            F.col("f.academic_period_key").alias("academic_period_key"),
            F.col("f.program_key").alias("program_key"),
            F.col("f.period_start_date").alias("period_start_date"),
            F.col("f.program_code").alias("program_code"),
            F.col("f.program_lineage_key").alias("program_lineage_key"),
            F.col("f.is_primary_program").alias("is_primary_program"),
            F.col("f.declare_date_key").alias("declare_date_key"),
            F.col("f.cohort_count").alias("cohort_count"),
            F.col("f.academic_period_name").alias("academic_period_name"),
            F.col("f.college_key").alias("college_key"),
            F.col("f.retained_term2_flag").alias("retained_term2_flag"),
            F.col("f.completed_by_term2_flag").alias("completed_by_term2_flag"),
            F.col("f.completed_by_term7_flag").alias("completed_by_term7_flag"),
            F.col("f.completed_by_term10_flag").alias("completed_by_term10_flag"),
            F.col("f.completed_by_term13_flag").alias("completed_by_term13_flag"),
            # rename Y1–Y4 flags for reporting
            F.col("f.retained_y1_flag").alias("retained_term4_flag"),
            F.col("f.retained_y2_flag").alias("retained_term7_flag"),
            F.col("f.retained_y3_flag").alias("retained_term10_flag"),
            F.col("f.retained_y4_flag").alias("retained_term13_flag"),
            F.col("f.completed_in_family_by_measure_flag").alias("completed_in_family_by_measure_flag"),
            F.col("f.completed_within_100_flag").alias("completed_within_100_flag"),
            F.col("f.completed_within_150_flag").alias("completed_within_150_flag"),
            F.col("f.completed_within_200_flag").alias("completed_within_200_flag"),
            F.col("f.completed_within_400_flag").alias("completed_within_400_flag"),
            F.col("f.completion_date_key").alias("completion_date_key"),
            F.col("f.switch_target_lineage_key").alias("switch_target_lineage_key"),
            F.col("f.exit_reason").alias("exit_reason"),
            F.col("f.last_measured_date").alias("last_measured_date"),
            F.col("f.modified_on").alias("modified_on"),
            F.col("f.modified_by").alias("modified_by"),
            # propagate termX academic period keys
            F.col("f.term4_academic_period_key").alias("term4_academic_period_key"),
            F.col("f.term7_academic_period_key").alias("term7_academic_period_key"),
            F.col("f.term10_academic_period_key").alias("term10_academic_period_key"),
            F.col("f.term13_academic_period_key").alias("term13_academic_period_key"),
            F.col("f.term2_academic_period_key").alias("term2_academic_period_key"),
            F.col("f.entry_program_duration_years").alias("entry_program_duration_years"),
            F.col("f.program_name").alias("program_name"),
        )
    )

    # ===============================================================
    # 16. COMPLETION DATE FLAG
    # ===============================================================

    final_df = (
        final_df.alias("f")
        .join(
            dim_date_df.alias("dd"),
            F.col("f.completion_date_key")
            == F.col("dd.date_key"),
            "left",
        )
        .withColumn(
            "completion_years_diff",
            F.when(
                F.col("f.completion_date_key").isNull(),
                F.lit(None).cast("double"),
            ).otherwise(
                F.floor(
                    F.datediff(
                        F.col("dd.calendar_date"),
                        F.col("f.period_start_date"),
                    )
                    / F.lit(365.25)
                )
            ),
        )
        .withColumn(
            "completion_date_flag",
            F.when(
                F.col("completion_years_diff") == 1,
                F.lit(1),
            )
            .when(
                (F.col("completion_years_diff") > 1)
                &
                (F.col("completion_years_diff") <= 2),
                F.lit(2),
            )
            .when(
                (F.col("completion_years_diff") > 2)
                &
                (F.col("completion_years_diff") <= 3),
                F.lit(3),
            )
            .when(
                F.col("completion_years_diff") > 3,
                F.lit(4),
            )
            .otherwise(
                F.lit(None).cast("int")
            ),
        )
        .drop(
            "completion_years_diff"
        )
    )

    # ===============================================================
    # 17. ADD AUDIT COLUMNS
    # ===============================================================

    final_df = add_audit_column(
        final_df,
        JOB_NAME
    )

    # ===============================================================
    # 18. REMOVE DUPLICATES (now correctly keyed by student_key+cohort_key)
    # ===============================================================

    final_df = final_df.dropDuplicates(["student_key", "cohort_key"])

    # ===============================================================
    # 19. BUILD REGISTRATION DATASET WITH CREDITS
    # ===============================================================

    registration_df = (
        fact_reg_audit_df.alias("CR")
        .join(
            dim_ap_df.alias("AP"),
            F.col("CR.academic_period_key")
            == F.col("AP.academic_period_key"),
            "left",
        )
        .where(
            F.col("CR.registration_status").isin("Completed", "Registered")
        )
        .groupBy(
            F.col("AP.academic_period_name").alias("academic_period_name"),
            F.col("CR.student_key").alias("student_key"),
        )
        .agg(
            F.sum("CR.credits").alias("program_credits"),
            F.first("CR.college_key", ignorenulls=True).alias("college_key"),
        )
    )
    # ---------------------------------------------------------------
    # MEMBERSHIP evidence — registrations INCLUDING withdrawals.
    # registration_df above excludes Withdrawn, which is right for
    # retention but wrong for cohort membership. Used ONLY by the
    # entry gate below.
    #
    # LIMITATION: PeopleSoft-sourced registrations are filtered at cell 7
    # (grades W/NS/AU excluded) before reaching fact_reg_audit_df, so
    # PS-only students' withdrawals are not visible here. Membership is
    # therefore stricter for PS-only students than for Workday-sourced
    # ones. A withdrawal_date is available on Workday rows if a
    #census-date rule is required later.
    # ---------------------------------------------------------------
    entry_reg_df = (
        fact_reg_audit_df.alias("CR")
        .join(
            dim_ap_df.alias("AP"),
            F.col("CR.academic_period_key") == F.col("AP.academic_period_key"),
            "left",
        )
        .groupBy(
            F.col("AP.academic_period_name").alias("er_period_name"),
            F.col("CR.student_key").alias("er_student_key"),
        )
        .agg(F.sum("CR.credits").alias("er_credits"))
    )
    # ---------------------------------------------------------------
    # Campus selection per student + term (entry_campus_location_key)
    # ---------------------------------------------------------------

    w_campus = (
        Window
        .partitionBy("student_key", "academic_period_name")
        .orderBy(F.col("credits").desc(), F.col("campus_location_key").asc())
    )

    entry_campus_df = (
        fact_reg_audit_df.alias("CR")
        .join(
            dim_ap_df.alias("AP"),
            F.col("CR.academic_period_key") == F.col("AP.academic_period_key"),
            "left",
        )
        .where(F.col("CR.registration_status").isin("Completed", "Registered"))
        .where(F.col("CR.campus_location_key").isNotNull())
        .groupBy(
            F.col("AP.academic_period_name").alias("academic_period_name"),
            F.col("CR.student_key").alias("student_key"),
            F.col("CR.campus_location_key").alias("campus_location_key"),
        )
        .agg(F.sum("CR.credits").alias("credits"))
        .withColumn("rn", F.row_number().over(w_campus))
        .filter(F.col("rn") == 1)
        .select(
            "student_key",
            "academic_period_name",
            F.col("campus_location_key").alias("entry_campus_location_key"),
        )
    )

    # ===============================================================
    # 20. JOIN REGISTRATION CREDITS AND CAMPUS
    # ===============================================================

    final_df = (
        final_df.alias("f")
        .join(
            registration_df.alias("r"),
            (
                F.col("f.student_key")
                == F.col("r.student_key")
            )
            &
            (
                F.col("f.academic_period_name")
                == F.col("r.academic_period_name")
            ),
            "left",
        )
        .join(
            entry_campus_df.alias("ec"),
            (
                F.col("f.student_key")
                == F.col("ec.student_key")
            )
            &
            (
                F.col("f.academic_period_name")
                == F.col("ec.academic_period_name")
            ),
            "left",
        )
        .select(
            F.col("f.student_key").alias("student_key"),
            F.col("f.student_id").alias("student_id"),
            F.col("f.cohort_key").alias("cohort_key"),
            F.col("f.college_key").alias("college_key"),
            F.col("f.academic_period_key").alias("academic_period_key"),
            F.col("f.program_key").alias("program_key"),
            F.col("f.period_start_date").alias("period_start_date"),
            F.col("f.program_code").alias("program_code"),
            F.col("f.program_lineage_key").alias("program_lineage_key"),
            F.col("f.is_primary_program").alias("is_primary_program"),
            F.col("f.declare_date_key").alias("declare_date_key"),
            F.col("f.cohort_count").alias("cohort_count"),
            F.col("f.academic_period_name").alias("academic_period_name"),
            F.col("r.academic_period_name").alias("registration_academic_period_name"),
            F.col("r.program_credits").alias("program_credits"),  # renamed for clarity
            F.col("f.retained_term2_flag").alias("retained_term2_flag"),
            F.col("f.completed_by_term2_flag").alias("completed_by_term2_flag"),
            F.col("f.completed_by_term7_flag").alias("completed_by_term7_flag"),
            F.col("f.completed_by_term10_flag").alias("completed_by_term10_flag"),
            F.col("f.completed_by_term13_flag").alias("completed_by_term13_flag"),
            # renamed Y1–Y4 flags for reporting
            F.col("f.retained_term4_flag").alias("retained_term4_flag"),
            F.col("f.retained_term7_flag").alias("retained_term7_flag"),
            F.col("f.retained_term10_flag").alias("retained_term10_flag"),
            F.col("f.retained_term13_flag").alias("retained_term13_flag"),
            F.col("f.completed_in_family_by_measure_flag").alias("completed_in_family_by_measure_flag"),
            F.col("f.completed_within_100_flag").alias("completed_within_100_flag"),
            F.col("f.completed_within_150_flag").alias("completed_within_150_flag"),
            F.col("f.completed_within_200_flag").alias("completed_within_200_flag"),
            F.col("f.completed_within_400_flag").alias("completed_within_400_flag"),
            F.col("f.completion_date_key").alias("completion_date_key"),
            F.col("f.switch_target_lineage_key").alias("switch_target_lineage_key"),
            F.col("f.exit_reason").alias("exit_reason"),
            F.col("f.last_measured_date").alias("last_measured_date"),
            F.col("f.modified_on").alias("modified_on"),
            F.col("f.modified_by").alias("modified_by"),
            F.col("f.date_key").alias("date_key"),
            F.col("f.calendar_date").alias("calendar_date"),
            F.col("f.completion_date_flag").alias("completion_date_flag"),
            F.col("f.created_on").alias("created_on"),
            F.col("f.created_by").alias("created_by"),
            # expose termX academic period keys at program level for reporting
            F.col("f.term4_academic_period_key").alias("term4_academic_period_key"),
            F.col("f.term7_academic_period_key").alias("term7_academic_period_key"),
            F.col("f.term10_academic_period_key").alias("term10_academic_period_key"),
            F.col("f.term13_academic_period_key").alias("term13_academic_period_key"),
            F.col("f.term2_academic_period_key").alias("term2_academic_period_key"),
            F.col("f.entry_program_duration_years").alias("entry_program_duration_years"),
            # new campus column
            F.col("ec.entry_campus_location_key").alias("entry_campus_location_key"),
            F.col("f.program_name").alias("program_name"),
        )
    )

    # =====================================================================
    # ENTRY CREDITS GATE — Summer alternative term
    # =====================================================================

    entry_alt_df = (
        map_df.alias("m")
        .join(
            period_df.alias("e"),
            F.col("e.academic_period_key") == F.col("m.entry_academic_period_key"),
            "inner",
        )
        .join(
            period_df.alias("nf"),
            F.col("nf.academic_period_key") == F.col("m.term2_academic_period_key"),
            "inner",
        )
        .where(F.col("m.term_type") == "Summer")
        .where(F.col("nf.session").isNull())
        .select(
            F.col("e.academic_period_name").alias("academic_period_name"),
            F.col("nf.academic_period_name").alias("alt_period_name"),
        )
        .distinct()
    )

    # Guard: one alt term per entry term
    alt_rows = entry_alt_df.count()
    alt_keys = entry_alt_df.select("academic_period_name").distinct().count()
    print(f"entry_alt_df rows={alt_rows} distinct entry terms={alt_keys}")
    if alt_rows != alt_keys:
        (entry_alt_df.groupBy("academic_period_name")
                     .count().filter(F.col("count") > 1).show(20, truncate=False))
        raise Exception("entry_alt_df: more than one alt term per entry term")

        # --- 2. Credits in the alternative term -----------------------------
    final_df = (
        final_df.alias("f")
        .join(entry_alt_df.alias("ea"), ["academic_period_name"], "left")
        .join(
            registration_df.alias("ra"),
            (F.col("f.student_key") == F.col("ra.student_key"))
& (F.col("ea.alt_period_name") == F.col("ra.academic_period_name")),
            "left",
        )
        # membership evidence, entry term and alternative term
        .join(
            entry_reg_df.alias("er"),
            (F.col("f.student_key") == F.col("er.er_student_key"))
& (F.col("f.academic_period_name") == F.col("er.er_period_name")),
            "left",
        )
        .join(
            entry_reg_df.alias("er_alt"),
            (F.col("f.student_key") == F.col("er_alt.er_student_key"))
& (F.col("ea.alt_period_name") == F.col("er_alt.er_period_name")),
            "left",
        )
        .withColumn("alt_credits",       F.col("ra.program_credits"))
        .withColumn("entry_reg_credits", F.col("er.er_credits"))
        .withColumn("alt_reg_credits",   F.col("er_alt.er_credits"))
        .drop("alt_period_name")
    )
 
    # Drop columns from the registration aliases to avoid ambiguous student_key
    final_df = final_df.select("f.*", "alt_credits",
                               "entry_reg_credits", "alt_reg_credits")
 
    # --- 3. The widened gate --------------------------------------------
    before_ct = final_df.count()
 
    # MEMBERSHIP = registered in the entry term or its alternative term,
    # withdrawals included. Retention and completion below continue to use
    # program_credits, which excludes withdrawals.
    final_df = final_df.filter(
        (F.col("entry_reg_credits") > 0) | (F.col("alt_reg_credits") > 0)
    )

    after_ct = final_df.count()
    print(f"entry gate: {before_ct} rows in, {after_ct} rows kept")

    # --- 4. Credits that qualified the student --------------------------
    final_df = final_df.withColumn(
        "entry_credits",
        F.when(F.col("program_credits") > 0, F.col("program_credits")).otherwise(F.col("alt_credits")),
    )

    # Load status percentage based on entry_credits and term type
    load_status_per_expr = (
        F.when(
            F.col("academic_period_name").contains("Summer"),
            (F.col("entry_credits") / F.lit(6)) * F.lit(100),
        )
        .when(
            F.col("academic_period_name").contains("Fall")
            | F.col("academic_period_name").contains("Spring"),
            (F.col("entry_credits") / F.lit(12)) * F.lit(100),
        )
        .otherwise(F.lit(0.0))
    )

    final_df = (
        final_df
        .withColumn("load_status_per", load_status_per_expr)
        .withColumn(
            "load_status",
            F.when(F.col("load_status_per") >= 100, F.lit("Full-Time"))
            .when(
                (F.col("load_status_per") >= 50)
                & (F.col("load_status_per") < 100),
                F.lit("Half-Time"),
            )
            .otherwise(F.lit("Part-Time")),
        )
        .drop("load_status_per", "alt_credits")
    )

    # is_history — the entry declaration came from PeopleSoft.
    ps_entry_keys_df = (
        joined_df
        .filter(F.col("is_history") == 1)
        .select("student_key", "program_key")
        .distinct()
        .withColumn("_ps_entry", F.lit(1))
    )

    final_df = (
        final_df
        .join(ps_entry_keys_df, ["student_key", "program_key"], "left")
        .withColumn("is_history", F.coalesce(F.col("_ps_entry"), F.lit(0)))
        .drop("_ps_entry")
    )

    # --- 5. Uniqueness guard --------------------------------------------
    row_ct = final_df.count()
    key_ct = final_df.select("student_key", "cohort_key").distinct().count()
    print(f"program cohort rows={row_ct} distinct (student_key, cohort_key)={key_ct}")
    if row_ct != key_ct:
        (final_df.groupBy("student_key", "cohort_key")
                 .count().filter(F.col("count") > 1).show(30, truncate=False))
        raise Exception(f"{row_ct - key_ct} duplicate (student_key, cohort_key) rows")

    # 25. JOIN PROGRAM TAXONOMY AND ADD DUAL-CREDIT FLAG
    
    RCC_DUAL = [
        "1A", "1B", "1E", "8A", "8B",
        "9B", "9C", "9D", "9G", "9H", "9K"
    ]
    final_df = (
        final_df.alias("f")
        .join(
            program_tax_df.alias("pt"),
            (F.col("f.student_id") == F.col("pt.student_id")) &
            (F.col("f.academic_period_name") == F.col("pt.academic_period")) &
            (F.col("f.program_name") == F.col("pt.primary_program_of_study")),
            "left",
        )
        .withColumn("tax_code_list", F.col("pt.tax_code_list"))
    )

    final_df = final_df.withColumn(
        "is_dual_credit_flag",
        F.when(
            F.arrays_overlap(
                F.transform(
                    F.split(F.coalesce(F.col("tax_code_list"), F.lit("")), ","),
                    lambda c: F.upper(F.trim(c)),
                ),
                F.array(*[F.lit(c) for c in RCC_DUAL]),
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    fact_cohort_retention_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "fact_cohort_retention",
    )

    # Explicit projection with column names (no alias prefixes)
    final_df = final_df.select(
        "student_key",
        # "student_id",
        "cohort_key",
        "college_key",
        "academic_period_key",
        "program_key",
        "period_start_date",
        "program_code",
        "program_lineage_key",
        "is_primary_program",
        "declare_date_key",
        "cohort_count",
        "academic_period_name",
        "registration_academic_period_name",
        "program_credits",  # renamed column
        "retained_term2_flag",
        "completed_by_term2_flag",
        "completed_by_term7_flag",
        "completed_by_term10_flag",
        "completed_by_term13_flag",
        "completed_within_100_flag",
        "completed_within_150_flag",
        "completed_within_200_flag",
        "completed_within_400_flag",
        "retained_term4_flag",
        "retained_term7_flag",
        "retained_term10_flag",
        "retained_term13_flag",
        "completed_in_family_by_measure_flag",
        "completion_date_key",
        "exit_reason",
        "last_measured_date",
        "date_key",
        "calendar_date",
        "completion_date_flag",
        "term4_academic_period_key",
        "term7_academic_period_key",
        "term10_academic_period_key",
        "term13_academic_period_key",
        "term2_academic_period_key",
        "entry_campus_location_key",
        "entry_program_duration_years",
        "tax_code_list",
        "is_dual_credit_flag",
        "program_name",
        "load_status",
        "is_history",
    )

    # ===============================================================
    # 23. LOAD STATUS
    # ===============================================================

   

    final_df = final_df.withColumn(
        "switch_target_lineage_key",
        F.lit(None).cast("string")
    )
    final_df = add_audit_column(final_df, PIPELINE_NAME)

    # Use utility writer
    write_delta_tbl(
        final_df,
        fact_cohort_retention_path,
        ["student_key", "cohort_key"],
    )

    print(
        "fact_cohort_retention creation with "
        "PASS_B metrics (Y1-Y4 + Term2) completed successfully"
    )

except Exception as e:
     raise Exception(f"Failed to create/update fact_cohort_retention table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    # ---------------------------------------------------------------
    # Source tables
    # ---------------------------------------------------------------
    # Build ABFSS paths for each table in workday_cs schema
    # fpe_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_program_enrollment")
    dim_program_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")
    dim_ap_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_cohort_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_cohort")
    dim_students_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    ref_map_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_ref_cohort_measurement_map")

    # Read Delta tables via ABFSS
    fpe_df          = joined_df
    dim_program_df  = spark.read.format("delta").load(dim_program_path)
    dim_ap_df       = spark.read.format("delta").load(dim_ap_path)
    dim_cohort_df   = spark.read.format("delta").load(dim_cohort_path)
    dim_students_df = spark.read.format("delta").load(dim_students_path)
    fact_comp_df    = fact_complition_df
    fact_reg_df     = fact_reg_audit_df
    ref_map_df      = spark.read.format("delta").load(ref_map_path)

    # ---------------------------------------------------------------
    # completion_date_key -> date
    # ---------------------------------------------------------------
    fact_comp_df = fact_comp_df.withColumn(
        "completion_date",
        F.to_date(F.col("completion_date_key").cast("string"), "yyyyMMdd")
    )

    # ---------------------------------------------------------------
    # base CTE
    # ---------------------------------------------------------------
    w_base = (
        Window
        .partitionBy("student_key")
        .orderBy(
            F.col("period_start_date"),
            F.col("is_primary_program").desc(),
            F.col("program_key")
        )
    )

    base_df = (
        fpe_df.alias("pe")
        .join(dim_program_df.alias("p"), F.col("pe.program_key") == F.col("p.program_key"), "inner")
        .join(dim_ap_df.alias("ap"), F.col("ap.academic_period_key") == F.col("pe.entry_academic_period_key"), "inner")
        .where(
            (F.col("p.enrollment_stage") == "Enrolled")
            & (F.col("p.cohort_exclusion_reason").isin("Direct", "Roll up to parent"))
            & (F.col("p.academic_level") == "Post Secondary")
        )
        .select(
            F.col("pe.student_key"),
            F.col("pe.entry_academic_period_key"),
            F.col("ap.academic_period_name").alias("entry_academic_period_name"),
            F.col("ap.period_start_date"),
            F.col("pe.program_key"),
            F.col("p.program_code"),
            F.col("p.program_lineage_key"),
            F.col("pe.is_primary_program"),
            F.col("pe.program_name").alias("program_name"),  # carry program_name so we can use it in dual-credit join
        )
        .withColumn("rn", F.row_number().over(w_base))
    )

    base_df = base_df.filter(F.col("rn") == 1)

    # ---------------------------------------------------------------
    # fact_cohort_retention CTE
    # ---------------------------------------------------------------
    fcr_df = (
        base_df.alias("fe")
        .join(
            dim_ap_df.alias("ap_base"),
            F.col("ap_base.academic_period_key") == F.col("fe.entry_academic_period_key"),
            "inner"
        )
        .join(
            dim_cohort_df.alias("c"),
            (F.col("c.cohort_type") == F.lit("Institutional")) &
            (F.col("c.academic_period_name") == F.col("ap_base.academic_period_name")),
            "inner"
        )
        .join(
            dim_students_df.alias("s"),
            F.col("s.student_key") == F.col("fe.student_key"),
            "inner"
        )
        .select(
            F.col("s.student_key"),
            F.col("s.student_id"),
            F.col("c.cohort_key"),
            F.col("ap_base.academic_period_name").alias("entry_academic_period_name"),
            F.col("fe.entry_academic_period_key").alias("entry_academic_period_key"),  # keep true entry period key
            F.date_format("ap_base.pell_recalculation_date", "yyyyMMdd").cast("int").alias("entry_prd_date_key"),
            F.col("fe.program_key").alias("entry_program_key"),
            F.col("fe.program_code").alias("entry_program_code"),
            F.col("fe.program_lineage_key").alias("entry_program_lineage_key"),
            F.col("fe.is_primary_program").alias("is_primary_at_entry"),
            F.col("fe.program_name").alias("program_name"),  # keep program_name at institutional level
            F.lit(1).alias("cohort_count"),
            # initialise metrics as nulls; will be recomputed later
            F.lit(None).cast("int").alias("retained_y1_flag"),
            F.lit(None).cast("int").alias("retained_y2_flag"),
            F.lit(None).cast("int").alias("retained_y3_flag"),
            F.lit(None).cast("int").alias("retained_y4_flag"),
            F.lit(None).cast("int").alias("completed_in_family_by_measure_flag"),
            F.lit(None).cast("int").alias("first_qualifying_completion_date_key"),
            F.lit(None).cast("string").alias("exit_reason"),
            F.lit(None).cast("string").alias("switch_target_lineage_key"),
            F.lit(None).cast("date").alias("last_measured_date"),
            F.lit(0).alias("restatement_count"),
            F.current_timestamp().alias("created_on"),
            F.lit("INST_PASS_A").alias("created_by")
        )
    )

    # ---------------------------------------------------------------
    # fc CTE (due dates: y1/y2/term2 per cohort/student)
    # ---------------------------------------------------------------
    fc_df = (
        fcr_df.alias("f")
        .join(dim_cohort_df.alias("c"), F.col("c.cohort_key") == F.col("f.cohort_key"), "inner")
        .join(
            dim_ap_df.alias("ap_cohort"),
            F.col("ap_cohort.academic_period_name") == F.col("c.academic_period_name"),
            "inner"
        )
        .join(
            ref_map_df.alias("m"),
            F.col("m.entry_academic_period_key") == F.col("ap_cohort.academic_period_key"),
            "inner"
        )
        .join(
            dim_ap_df.alias("y1"),
            F.col("y1.academic_period_key") == F.col("m.y1_academic_period_key"),
            "left"
        )
        .join(
            dim_ap_df.alias("y2"),
            F.col("y2.academic_period_key") == F.col("m.y2_academic_period_key"),
            "left"
        )
        .join(
            dim_ap_df.alias("term2"),
            F.col("term2.academic_period_key") == F.col("m.term2_academic_period_key"),
            "left"
        )
        .join(
            dim_ap_df.alias("t2a"),
            F.col("t2a.academic_period_key") == F.col("m.term2_alt_academic_period_key"),
            "left"
        )
        .join(
            dim_ap_df.alias("y1a"),
            F.col("y1a.academic_period_key") == F.col("m.y1_alt_academic_period_key"),
            "left"
        )
        .where(F.col("c.cohort_type") == "Institutional")
        .select(
            F.col("f.cohort_key"),
            F.col("f.student_key"),
            F.col("ap_cohort.academic_period_name").alias("academic_period_name"),
            F.col("m.y1_academic_period_key").alias("y1_academic_period_key"),
            F.col("m.y2_academic_period_key").alias("y2_academic_period_key"),
            F.col("m.term2_academic_period_key").alias("term2_academic_period_key"),
            F.col("m.term2_alt_academic_period_key").alias("term2_alt_academic_period_key"),
            F.col("m.y1_alt_academic_period_key").alias("y1_alt_academic_period_key"),
            F.col("y1.pell_recalculation_date").alias("y1_measure_date"),
            F.col("y2.pell_recalculation_date").alias("y2_measure_date"),
            F.col("term2.pell_recalculation_date").alias("term2_measure_date"),
            F.col("t2a.pell_recalculation_date").alias("term2_alt_measure_date"),
            F.col("y1a.pell_recalculation_date").alias("y1_alt_measure_date")
        )
    )

    # ---------------------------------------------------------------
    # Pre-aggregations to emulate EXISTS and MIN subquery
    # ---------------------------------------------------------------

    # First qualifying completion date per student (yyyyMMdd int)
    # NOTE: use a unique column name to avoid ambiguity with the placeholder
    first_completion_df = (
        fact_comp_df
        .groupBy("student_key")
        .agg(F.min("completion_date_key").alias("fc_first_qualifying_completion_date_key"))
    )

    # Y1 completion: any completion_date <= y1_measure_date
    comp_y1_df = (
        fc_df.alias("d")
        .join(
            fact_comp_df.alias("fc"),
            (F.col("fc.student_key") == F.col("d.student_key")) &
            (F.col("fc.completion_date") <= F.col("d.y1_measure_date")),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_completion_y1", F.lit(1))
    )

    # Y2 completion: any completion_date <= y2_measure_date
    comp_y2_df = (
        fc_df.alias("d")
        .join(
            fact_comp_df.alias("fc"),
            (F.col("fc.student_key") == F.col("d.student_key")) &
            (F.col("fc.completion_date") <= F.col("d.y2_measure_date")),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_completion_y2", F.lit(1))
    )

    # Term2 completion: any completion_date <= term2_measure_date
    comp_term2_df = (
        fc_df.alias("d")
        .join(
            fact_comp_df.alias("fc"),
            (F.col("fc.student_key") == F.col("d.student_key")) &
            (F.col("fc.completion_date") <= F.col("d.term2_measure_date")),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_completion_term2", F.lit(1))
    )

    # Y1 registration
    reg_y1_df = (
        fc_df.alias("d")
        .join(
            fact_reg_df.alias("r"),
            (F.col("r.student_key") == F.col("d.student_key")) &
            (F.col("r.academic_period_key") == F.col("d.y1_academic_period_key")) &
            (F.col("r.registration_status").isin("Registered", "Completed")) &
            (F.col("r.credits") >= 1),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_reg_y1", F.lit(1))
    )

    # Y1 ALT registration
    reg_y1_alt_df = (
        fc_df.alias("d")
        .join(
            fact_reg_df.alias("r"),
            (F.col("r.student_key") == F.col("d.student_key")) &
            (F.col("r.academic_period_key") == F.col("d.y1_alt_academic_period_key")) &
            (F.col("r.registration_status").isin("Registered", "Completed")) &
            (F.col("r.credits") >= 1),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_reg_y1_alt", F.lit(1))
    )

    # Y2 registration
    reg_y2_df = (
        fc_df.alias("d")
        .join(
            fact_reg_df.alias("r"),
            (F.col("r.student_key") == F.col("d.student_key")) &
            (F.col("r.academic_period_key") == F.col("d.y2_academic_period_key")) &
            (F.col("r.registration_status").isin("Registered", "Completed")) &
            (F.col("r.credits") >= 1),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_reg_y2", F.lit(1))
    )

    # Term2 registration
    reg_term2_df = (
        fc_df.alias("d")
        .join(
            fact_reg_df.alias("r"),
            (F.col("r.student_key") == F.col("d.student_key")) &
            (F.col("r.academic_period_key") == F.col("d.term2_academic_period_key")) &
            (F.col("r.registration_status").isin("Registered", "Completed")) &
            (F.col("r.credits") >= 1),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_reg_term2", F.lit(1))
    )

    # Term2 ALT registration
    reg_term2_alt_df = (
        fc_df.alias("d")
        .join(
            fact_reg_df.alias("r"),
            (F.col("r.student_key") == F.col("d.student_key")) &
            (F.col("r.academic_period_key") == F.col("d.term2_alt_academic_period_key")) &
            (F.col("r.registration_status").isin("Registered", "Completed")) &
            (F.col("r.credits") >= 1),
            "inner"
        )
        .select("d.cohort_key", "d.student_key")
        .dropDuplicates()
        .withColumn("has_reg_term2_alt", F.lit(1))
    )

    # ---------------------------------------------------------------
    # Combine and derive flags (retained_y1/y2, completed_in_family, etc.)
    # ---------------------------------------------------------------

    f_base = (
        fcr_df.alias("f")
        .join(fc_df.alias("d"), ["cohort_key", "student_key"], "inner")
        .join(first_completion_df.alias("fc_min"), "student_key", "left")
        .join(comp_y1_df.alias("cy1"), ["cohort_key", "student_key"], "left")
        .join(comp_y2_df.alias("cy2"), ["cohort_key", "student_key"], "left")
        .join(comp_term2_df.alias("ct2"), ["cohort_key", "student_key"], "left")
        .join(reg_y1_df.alias("ry1"), ["cohort_key", "student_key"], "left")
        .join(reg_y1_alt_df.alias("ry1a"), ["cohort_key", "student_key"], "left")
        .join(reg_y2_df.alias("ry2"), ["cohort_key", "student_key"], "left")
        .join(reg_term2_df.alias("rt2"), ["cohort_key", "student_key"], "left")
        .join(reg_term2_alt_df.alias("rt2a"), ["cohort_key", "student_key"], "left")
    )

    final_df = (
        f_base
        # effective Y1/Term2 measure dates (use alt if present)
        .withColumn(
            "effective_y1_measure_date",
            F.coalesce(F.col("y1_alt_measure_date"), F.col("y1_measure_date"))
        )
        .withColumn(
            "effective_term2_measure_date",
            F.coalesce(F.col("term2_alt_measure_date"), F.col("term2_measure_date"))
        )
        # retained_y1_flag
        .withColumn(
            "retained_y1_flag",
            F.when(
                (F.col("effective_y1_measure_date").isNull()) |
                (F.col("effective_y1_measure_date") > F.current_date()),
                F.lit(None).cast("int")
            ).when(
                F.col("cy1.has_completion_y1") == 1,
                F.lit(1)
            ).when(
                (F.col("ry1.has_reg_y1") == 1) | (F.col("ry1a.has_reg_y1_alt") == 1),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        # retained_y2_flag
        .withColumn(
            "retained_y2_flag",
            F.when(
                (F.col("y2_measure_date").isNull()) |
                (F.col("y2_measure_date") > F.current_date()),
                F.lit(None).cast("int")
            ).when(
                F.col("cy2.has_completion_y2") == 1,
                F.lit(1)
            ).when(
                F.col("ry2.has_reg_y2") == 1,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        # retained_y3_flag (not computed here, keep as null)
        .withColumn("retained_y3_flag", F.lit(None).cast("int"))
        # retained_y4_flag (not computed here, keep as null)
        .withColumn("retained_y4_flag", F.lit(None).cast("int"))
        # y2_measure_date_key for completed_by_term7_flag
        .withColumn(
            "y2_measure_date_key",
            F.when(
                F.col("y2_measure_date").isNotNull(),
                F.date_format("y2_measure_date", "yyyyMMdd").cast("int"),
            )
        )
        # term2_measure_date_key based on effective term2 measure date
        .withColumn(
            "term2_measure_date_key",
            F.when(
                F.col("effective_term2_measure_date").isNotNull(),
                F.date_format("effective_term2_measure_date", "yyyyMMdd").cast("int"),
            )
        )
        # retained_term2_flag (same pattern as program-level, using alt period)
        .withColumn(
            "retained_term2_flag",
            F.when(
                (F.col("effective_term2_measure_date").isNull()) |
                (F.col("effective_term2_measure_date") > F.current_date()),
                F.lit(None).cast("int")
            )
            .when(
                F.col("ct2.has_completion_term2") == 1,
                F.lit(1)
            )
            .when(
                (F.col("rt2.has_reg_term2") == 1) | (F.col("rt2a.has_reg_term2_alt") == 1),
                F.lit(1)
            )
            .otherwise(F.lit(0))
        )
        # completed_in_family_by_measure_flag (Y1-based)
        .withColumn(
            "completed_in_family_by_measure_flag",
            F.when(
                F.col("effective_y1_measure_date").isNull(),
                F.lit(None).cast("int")
            ).when(
                F.col("cy1.has_completion_y1") == 1,
                F.lit(1)
            ).otherwise(F.lit(0))
        )
        # completed_by_term2_flag
        .withColumn(
            "completed_by_term2_flag",
            F.when(
                F.col("effective_term2_measure_date").isNull(),
                F.lit(None).cast("int")
            )
            .when(
                F.col("fc_min.fc_first_qualifying_completion_date_key")
                <= F.col("term2_measure_date_key"),
                F.lit(1)
            )
            .otherwise(F.lit(0))
        )
        # completed_by_term7_flag (institutional – any completion by Y2 measure date)
        .withColumn(
            "completed_by_term7_flag",
            F.when(
                F.col("y2_measure_date").isNull(),
                F.lit(None).cast("int")
            )
            .when(
                F.col("fc_min.fc_first_qualifying_completion_date_key")
                <= F.col("y2_measure_date_key"),
                F.lit(1)
            )
            .otherwise(F.lit(0))
        )
        # first_qualifying_completion_date_key from fc_min (unique source column)
        .withColumn(
            "first_qualifying_completion_date_key",
            F.col("fc_min.fc_first_qualifying_completion_date_key").cast("int")
        )
        .drop("fc_first_qualifying_completion_date_key")
        # switch_target_lineage_key always NULL in this pass (string)
        .withColumn("switch_target_lineage_key", F.lit(None).cast("string"))
        .withColumn("last_measured_date", F.current_date())
        .withColumn("restatement_count", F.col("restatement_count") + F.lit(1))
        .withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit("INST_PASS_B"))
    )

    # exit_reason (needs retained_y1_flag & completed_in_family_by_measure_flag)
    final_df = final_df.withColumn(
        "exit_reason",
        F.when(F.col("retained_y1_flag").isNull(), F.lit(None).cast("string"))
         .when(F.col("completed_in_family_by_measure_flag") == 1, F.lit("Graduated"))
         .when(F.col("retained_y1_flag") == 0, F.lit("Left NWTC"))
         .otherwise(F.lit(None).cast("string"))
    )

    # Align schema with PASS_B final_df by adding missing columns
    # For Institutional level, program-related keys must be NULL
    final_df = (
        final_df
        .withColumn("academic_period_key", F.col("entry_academic_period_key"))  # use true academic period key, not date key
        .withColumn("program_key", F.lit(None).cast("int"))              # institutional level: no program
        .withColumn("program_code", F.lit(None).cast("string"))          # institutional level: no program
        .withColumn("program_lineage_key", F.lit(None).cast("int"))      # institutional level: no program lineage
        .withColumn("is_primary_program", F.lit(None).cast("int"))       # institutional level: not program-scoped
        .withColumn("declare_date_key", F.col("entry_prd_date_key"))     # keep date key as declare_date_key
        .withColumn("completion_date_flag", F.lit(None).cast("int"))
        # Institutional block -- no programme scope, so NULL for union
        # alignment with the program-level (PASS_B) fact_cohort_retention
        # rows, which now carry these 5 columns.
        .withColumn("entry_program_duration_years", F.lit(None).cast("decimal(4,1)"))
        .withColumn("completed_within_100_flag", F.lit(None).cast("int"))
        .withColumn("completed_within_150_flag", F.lit(None).cast("int"))
        .withColumn("completed_within_200_flag", F.lit(None).cast("int"))
        .withColumn("completed_within_400_flag", F.lit(None).cast("int"))
    )

    # Add audit columns
    final_df = add_audit_column(final_df, JOB_NAME)

    # Build registration_df using the same aggregation logic as program-level
    registration_df = (
        fact_reg_audit_df.alias("CR")
        .join(
            dim_ap_df.alias("AP"),
            F.col("CR.academic_period_key") == F.col("AP.academic_period_key"),
            "left"
        )
        .where(F.col("CR.registration_status").isin("Completed", "Registered"))
        .groupBy(
            F.col("AP.academic_period_name").alias("academic_period_name"),
            F.col("CR.student_key").alias("student_key"),
        )
        .agg(
            F.sum("CR.credits").alias("inst_credits"),  # renamed to avoid ambiguity later
            F.first("CR.college_key", ignorenulls=True).alias("college_key"),
        )
    )

    # ---------------------------------------------------------------
    # Campus selection per student + term (entry_campus_location_key)
    # ---------------------------------------------------------------

    w_campus = (
        Window
        .partitionBy("student_key", "academic_period_name")
        .orderBy(F.col("credits").desc(), F.col("campus_location_key").asc())
    )

    entry_campus_df = (
        fact_reg_audit_df.alias("CR")
        .join(
            dim_ap_df.alias("AP"),
            F.col("CR.academic_period_key") == F.col("AP.academic_period_key"),
            "left",
        )
        .where(F.col("CR.registration_status").isin("Completed", "Registered"))
        .where(F.col("CR.campus_location_key").isNotNull())
        .groupBy(
            F.col("AP.academic_period_name").alias("academic_period_name"),
            F.col("CR.student_key").alias("student_key"),
            F.col("CR.campus_location_key").alias("campus_location_key"),
        )
        .agg(F.sum("CR.credits").alias("credits"))
        .withColumn("rn", F.row_number().over(w_campus))
        .filter(F.col("rn") == 1)
        .select(
            "student_key",
            "academic_period_name",
            F.col("campus_location_key").alias("entry_campus_location_key"),
        )
    )

    # Join on student_key + academic_period_name instead of student_id, and bring in entry_campus_location_key
    final_df = (
        final_df.alias("f")
        .join(
            registration_df.alias("r"),
            (F.col("f.student_key") == F.col("r.student_key")) &
            (F.col("f.academic_period_name") == F.col("r.academic_period_name")),
            "left"
        )
        .join(
            entry_campus_df.alias("ec"),
            (F.col("f.student_key") == F.col("ec.student_key")) &
            (F.col("f.academic_period_name") == F.col("ec.academic_period_name")),
            "left",
        )
        .select(
            F.col("f.student_key").alias("student_key"),
            F.col("f.student_id").alias("student_id"),
            F.col("f.academic_period_name").alias("academic_period_name"),
            F.col("f.cohort_key").alias("cohort_key"),
            F.col("f.academic_period_key").alias("academic_period_key"),
            F.lit(None).cast("int").alias("college_key"),
            F.col("f.program_key").alias("program_key"),
            F.col("f.program_code").alias("program_code"),
            F.col("f.program_lineage_key").alias("program_lineage_key"),
            F.col("f.is_primary_program").alias("is_primary_program"),
            F.col("f.declare_date_key").alias("declare_date_key"),
            F.col("f.cohort_count").alias("cohort_count"),
            F.col("f.y1_academic_period_key").alias("term4_academic_period_key"),
            F.col("f.y2_academic_period_key").alias("term7_academic_period_key"),
            # y3/y4 not tracked at institutional level today; expose as NULLs for schema alignment
            F.lit(None).cast("int").alias("term10_academic_period_key"),
            F.lit(None).cast("int").alias("term13_academic_period_key"),
            F.col("f.term2_academic_period_key").alias("term2_academic_period_key"),
            # Term2 metrics and renamed Y1–Y4 metrics
            F.col("f.retained_term2_flag").alias("retained_term2_flag"),
            F.col("f.completed_by_term2_flag").alias("completed_by_term2_flag"),
            F.col("f.completed_by_term7_flag").alias("completed_by_term7_flag"),
            F.col("f.retained_y1_flag").alias("retained_term4_flag"),
            F.col("f.retained_y2_flag").alias("retained_term7_flag"),
            F.col("f.retained_y3_flag").alias("retained_term10_flag"),
            F.col("f.retained_y4_flag").alias("retained_term13_flag"),
            F.col("f.completed_in_family_by_measure_flag").alias("completed_in_family_by_measure_flag"),
            F.col("f.completed_within_100_flag").alias("completed_within_100_flag"),
            F.col("f.completed_within_150_flag").alias("completed_within_150_flag"),
            F.col("f.completed_within_200_flag").alias("completed_within_200_flag"),
            F.col("f.completed_within_400_flag").alias("completed_within_400_flag"),
            F.col("f.first_qualifying_completion_date_key").alias("completion_date_key"),
            F.col("f.switch_target_lineage_key").alias("switch_target_lineage_key"),
            F.col("f.exit_reason").alias("exit_reason"),
            F.col("f.last_measured_date").alias("last_measured_date"),
            F.col("f.modified_on").alias("modified_on"),
            F.col("f.modified_by").alias("modified_by"),
            F.col("f.completion_date_flag").alias("completion_date_flag"),
            F.col("f.created_on").alias("created_on"),
            F.col("f.created_by").alias("created_by"),
            F.col("r.inst_credits").alias("credits"),   # unambiguous credits column (will be renamed later)
            F.col("ec.entry_campus_location_key").alias("entry_campus_location_key"),
            F.col("f.entry_program_duration_years").alias("entry_program_duration_years"),
            F.col("f.program_name").alias("program_name"),
        )
    )

    # ---------------------------------------------------------------
    # Drop duplicates in final dataframe before writing
    # ---------------------------------------------------------------
    final_df = final_df.dropDuplicates(["student_key", "cohort_key"])

    # ---------------------------------------------------------------
    # dual-credit logic
    # ---------------------------------------------------------------

    RCC_DUAL = [
        "1A", "1B", "1E", "8A", "8B",
        "9B", "9C", "9D", "9G", "9H", "9K"
    ]

    # Join program taxonomy at institutional level using student_id, term, and program_name
    final_df = (
        final_df.alias("f")
        .join(
            program_tax_df.alias("pt"),
            (F.col("f.student_id") == F.col("pt.student_id")) &
            (F.col("f.academic_period_name") == F.col("pt.academic_period")) &
            (F.col("f.program_name") == F.col("pt.primary_program_of_study")),
            "left",
        )
        .withColumn("tax_code_list", F.col("pt.tax_code_list"))
    )

    final_df = final_df.withColumn(
        "is_dual_credit_flag",
        F.when(
            F.arrays_overlap(
                F.transform(
                    F.split(F.coalesce(F.col("tax_code_list"), F.lit("")), ","),
                    lambda c: F.upper(F.trim(c)),
                ),
                F.array(*[F.lit(c) for c in RCC_DUAL]),
            ),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    # =====================================================================
    # ENTRY CREDITS GATE — Summer alternative term (institutional)
    # =====================================================================

    entry_alt_df = (
        ref_map_df.alias("m")
        .join(
            dim_ap_df.alias("e"),
            F.col("e.academic_period_key") == F.col("m.entry_academic_period_key"),
            "inner",
        )
        .join(
            dim_ap_df.alias("nf"),
            F.col("nf.academic_period_key") == F.col("m.term2_academic_period_key"),
            "inner",
        )
        .where(F.col("m.term_type") == "Summer")
        .where(F.col("nf.session").isNull())
        .select(
            F.col("e.academic_period_name").alias("academic_period_name"),
            F.col("nf.academic_period_name").alias("alt_period_name"),
        )
        .distinct()
    )

    # Guard: one alt term per entry term
    alt_rows = entry_alt_df.count()
    alt_keys = entry_alt_df.select("academic_period_name").distinct().count()
    print(f"institutional entry_alt_df rows={alt_rows} distinct entry terms={alt_keys}")
    if alt_rows != alt_keys:
        (entry_alt_df.groupBy("academic_period_name")
                     .count().filter(F.col("count") > 1).show(20, truncate=False))
        raise Exception("institutional entry_alt_df: more than one alt term per entry term")

    # --- 2. Credits in the alternative term -----------------------------
    final_df = (
        final_df.alias("f")
        .join(entry_alt_df.alias("ea"), ["academic_period_name"], "left")
        .join(
            registration_df.alias("ra"),
            (F.col("f.student_key") == F.col("ra.student_key")) &
            (F.col("ea.alt_period_name") == F.col("ra.academic_period_name")),
            "left",
        )
        .withColumn("alt_credits", F.col("ra.inst_credits"))
        .drop("alt_period_name", "ra.academic_period_name")
    )

    # Drop duplicate academic_period_name from registration alias to avoid ambiguity
    final_df = final_df.select("f.*", "alt_credits")

    # --- 3. The widened gate --------------------------------------------
    before_ct = final_df.count()

    final_df = final_df.filter(
        (F.col("credits") > 0) | (F.col("alt_credits") > 0)
    )

    after_ct = final_df.count()
    print(f"institutional entry gate: {before_ct} rows in, {after_ct} rows kept")

    # --- 4. Credits that qualified the student --------------------------
    final_df = final_df.withColumn(
        "entry_credits",
        F.when(F.col("credits") > 0, F.col("credits")).otherwise(F.col("alt_credits")),
    )

    # ---------------------------------------------------------------
    # 23. LOAD STATUS (institutional, based on entry_credits)
    # ---------------------------------------------------------------

    load_status_per_expr_inst = (
        F.when(
            F.col("academic_period_name").contains("Summer"),
            (F.col("entry_credits") / F.lit(6)) * F.lit(100),
        )
        .when(
            F.col("academic_period_name").contains("Fall")
            | F.col("academic_period_name").contains("Spring"),
            (F.col("entry_credits") / F.lit(12)) * F.lit(100),
        )
        .otherwise(F.lit(0.0))
    )

    final_df = (
        final_df
        .withColumn("load_status_per", load_status_per_expr_inst)
        .withColumn(
            "load_status",
            F.when(F.col("load_status_per") >= 100, F.lit("Full-Time"))
            .when(
                (F.col("load_status_per") >= 50)
                & (F.col("load_status_per") < 100),
                F.lit("Half-Time"),
            )
            .otherwise(F.lit("Part-Time")),
        )
        .drop("load_status_per", "alt_credits", "entry_credits")
    )

    # At this point, "credits" is the institutional credits column. For schema
    # alignment with the existing fact_cohort_retention (which uses
    # program_credits at the program level), rename to program_credits so the
    # Delta merge does not reference a non-existent t.credits column.
    final_df = final_df.withColumnRenamed("credits", "program_credits")

    final_df = final_df.withColumn(
        "switch_target_lineage_key",
        F.lit(None).cast("string")
    )
    final_df = add_audit_column(final_df, PIPELINE_NAME)

    fact_cohort_retention_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "fact_cohort_retention",
    )

    # Explicit projection with column names
    final_df = final_df.withColumn("is_history", F.lit(0))

    final_df = final_df.select(
        "student_key",
        # "f.student_id",
        "cohort_key",
        "college_key",
        "academic_period_key",
        "program_key",
        "program_code",
        "program_lineage_key",
        "is_primary_program",
        "declare_date_key",
        "cohort_count",
        "academic_period_name",
        "program_credits",           # renamed for alignment with program-level rows
        "retained_term2_flag",
        "completed_by_term2_flag",
        "completed_by_term7_flag",
        "completed_within_100_flag",
        "completed_within_150_flag",
        "completed_within_200_flag",
        "completed_within_400_flag",
        "retained_term4_flag",
        "retained_term7_flag",
        "retained_term10_flag",
        "retained_term13_flag",
        "completed_in_family_by_measure_flag",
        "completion_date_key",
        # "switch_target_lineage_key",
        "exit_reason",
        "last_measured_date",
        "completion_date_flag",
        "term4_academic_period_key",
        "term7_academic_period_key",
        "term10_academic_period_key",
        "term13_academic_period_key",
        "term2_academic_period_key",
        "entry_campus_location_key",
        "entry_program_duration_years",
        "tax_code_list",
        "is_dual_credit_flag",
        "program_name",
        "load_status",
        "is_history",
    )

    #Use existing utility to upsert on business key
    write_delta_tbl(
        final_df,
        fact_cohort_retention_path,
        ["student_key", "cohort_key"],
    )

    print("Institutional-level fact_cohort_retention upsert completed")

except Exception as e:
    raise Exception(f"Failed to create/update institutional fact_cohort_retention table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
