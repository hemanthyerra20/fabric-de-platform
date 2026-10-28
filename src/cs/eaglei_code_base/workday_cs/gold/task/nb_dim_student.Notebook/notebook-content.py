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

# CELL ********************

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

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
ps_silver_schema_name = "peoplesoft"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_students"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Student

# CELL ********************

try:
    student_sil_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "students")
    person_name_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "person_name_data")
    personal_profile_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "personal_profile_data")
    student_status_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_record_for_student_data")
    reporting_reords_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "reporting_records")
    record_status_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_record_status_references")
    geocoder_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_geocoder")
    address_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "address_data")
    address_line_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "home_contact_address_line_data")
    country_region_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "country_region_references")
    country_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "country_references")
    com_usage_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "communication_usage_type_references")
    education_credential_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_education_credential")

    address_data_df = spark.read.format("delta").load(address_data_path)\
                        .filter(F.col("primary") == 1)\
                        .withColumn("rn", row_number().over(Window.partitionBy("student_key").orderBy(F.desc("modified_on"))))\
                        .filter(F.col("rn") == 1)\
                        .drop("rn")
                        
    address_line_df = spark.read.format("delta").load(address_line_path)
    country_region_df = spark.read.format("delta").load(country_region_path)
    country_df = spark.read.format("delta").load(country_path)
    com_usage_df = spark.read.format("delta").load(com_usage_path)



    country_joined_df = (
        address_data_df.alias("ADD")
        .join(
            country_df.alias("CN"),
            F.col("ADD.country_reference_id") == F.col("CN.country_key"),
            "left"
        )
        .join(
            country_region_df.alias("REG"),
            F.col("ADD.country_region_reference_id") == F.col("REG.country_region_key"),
            "left"
        )
        .join(
            com_usage_df.alias("CU"),
            F.col("ADD.communication_usage_type_id") == F.col("CU.communication_usage_type_id"),
            "left"
        )
        .selectExpr(
            "ADD.student_key",
            "CU.description AS communication_usage_type",
            "ADD.postal_code",
            "CN.country_name AS country",
            "REG.region_name AS country_region"
        )
    )

    geocoder_df = spark.read.format("delta").load(geocoder_path)\
                    .selectExpr("student_id", "districtcode as district_code", "wtcscounty as county", "row_number() over(partition by student_id order by effectivedate desc) as rn")\
                    .filter(F.col("rn") == 1).drop("rn")

    education_credential_df = spark.read.format("delta").load(education_credential_path)\
                                .selectExpr("student_id", "hs_institution AS high_school", "hs_grad_date AS high_school_graduation_date")

    reporting_reords_df = spark.read.format("delta").load(reporting_reords_path)\
                            .selectExpr("affective_date", "student_record_id")

    record_status_df = spark.read.format("delta").load(record_status_path)

    std_status_df = spark.read.format("delta")\
                            .load(student_status_path)\
                            .selectExpr("student_key", "student_record_status_id", "student_record_id")

    status_joined_df = std_status_df.alias("LH")\
                            .join(reporting_reords_df.alias("RH"), F.col("LH.student_record_id") == F.col("RH.student_record_id"), "inner")\
                            .join(record_status_df.alias("ST"), F.col("LH.student_record_status_id") == F.col("ST.student_record_status_id"), "left")\
                            .selectExpr("LH.student_key", "ST.description AS current_student_record_status",
                            "row_number() over(partition by student_key order by affective_date desc) as rn")\
                            .filter(F.col("rn") == 1)\
                            .drop("rn")

    students_silv_df = spark.read.format("delta")\
                            .load(student_sil_path)\
                            .selectExpr("student_key", "student_id", "universal_id", "first_generation_college_student", "account_user_name", "is_veteran"
                            )

    name_data_silv_df = spark.read.format("delta")\
                            .load(person_name_data_path)\
                            .select("student_key", "legal_first_name", "legal_last_name")

    personal_profile_df = spark.read.format("delta")\
                            .load(personal_profile_data_path)\
                            .selectExpr("student_key",
                                    "gender_reference as gender", "hispanic_or_latino",
                                    "CAST(date_of_birth AS DATE) AS date_of_birth")\
                            .drop_duplicates()

    ethnicity_df = (
                    spark.read.format("delta")
                        .load(personal_profile_data_path)
                        .selectExpr("student_key", "ethnicity_id as ethnicity")
                        .filter((F.col("ethnicity").isNotNull()) | (F.col("ethnicity") != ""))
                        .drop_duplicates()
                )

    # Aggregate by student_key and derive final ethnicity
    personal_profile_agg_df = (
        ethnicity_df
        .groupBy("student_key")
        .agg(
            F.max("ethnicity").alias("max_ethnicity"),
            F.count("*").alias("ethnicity_count")
        )
        .withColumn(
            "ethnicity",
            F.when(F.col("ethnicity_count") > 1, F.lit("USA_Two_or_More_Races"))
            .otherwise(F.col("max_ethnicity"))
        )
        .select("student_key", "ethnicity")
    )


    std_joined_name_df = students_silv_df.alias("LH")\
                            .join(name_data_silv_df.alias("RH"),
                                        ((F.col("LH.student_key") == F.col("RH.student_key")))
                                        ,"left")\
                            .join(education_credential_df.alias("APP"), F.col("LH.student_id") == F.col("APP.student_id"), "left")\
                            .selectExpr("LH.*", "RH.legal_first_name", "RH.legal_last_name", "APP.high_school", "APP.high_school_graduation_date")

    joined_personal_prof_df = std_joined_name_df.alias("LH")\
                            .join(personal_profile_df.alias("RH"),
                                        ((F.col("LH.student_key") == F.col("RH.student_key")))
                                        ,"left")\
                            .join(personal_profile_agg_df.alias("ETH"),
                                        ((F.col("LH.student_key") == F.col("ETH.student_key")))
                                        ,"left")\
                            .join(status_joined_df.alias("SJ"),
                                        ((F.col("LH.student_key") == F.col("SJ.student_key")))
                                        ,"left")\
                            .selectExpr("LH.*", "RH.gender", "RH.hispanic_or_latino",
                                        "ETH.ethnicity", "RH.date_of_birth", "SJ.current_student_record_status")

    add_eth_grp_df = joined_personal_prof_df.withColumn(
        "ethnicity_group",
        F.when(F.col("hispanic_or_latino") == 1, "USA_Hispanic_Latino")
        .when(
            ((F.col("ethnicity").isNotNull()) &
            (F.instr(F.col("ethnicity"), ",") > 0)),
            "USA_Two_or_More_Races"
        )
        .otherwise(F.col("ethnicity"))
    )

    address_df = add_eth_grp_df\
                    .join(country_joined_df, on = "student_key", how="left")\
                    .join(geocoder_df, on = "student_id", how="left")

    address_df = address_df.withColumn(
        "high_school_graduation_date",
        F.when(
            F.col("high_school_graduation_date") < F.lit("1900-01-01").cast("date"),
            F.lit("1900-01-01").cast("date")
        ).otherwise(F.col("high_school_graduation_date"))
    )

    address_df.cache()

    
except Exception as e:
     # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_students table: {e}")

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
            F.to_date("map.academic_period_start_date", "yyyy-MM-dd").alias("period_start_date"),
            F.to_date("map.academic_period_end_date", "yyyy-MM-dd").alias("period_end_date"),
            F.col("map.academic_period_weeks_of_instruction").alias("weeks_of_instruction"),
            F.col("map.academic_calendar_id").alias("academic_calendar_id"),
            F.col("rd.registration_open_date"),
            F.col("rd.registration_close_date"),
            F.col("map.current_flag").alias("current_flag")
        )
        .filter(F.col("academic_period_id").isNotNull())
    )

    taxonomy_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_section_educational_taxonomy_code_reference")
    taxonomy_ref_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "educational_taxonomy_code_references")

    taxonomy_ref_df = (spark.read.format("delta").load(taxonomy_ref_path))
    taxonomy_df = (spark.read.format("delta").load(taxonomy_path))

    taxonomy_joined_df = (
        taxonomy_df.alias("LH")
        .join(
            taxonomy_ref_df.alias("RH"),
            F.col("LH.educational_taxonomy_code_id") ==
            F.col("RH.educational_taxonomy_id"),
            "inner"
        )
        .selectExpr(
            "LH.course_section_key AS course_section_key",
            "LH.course_section_id",
            "RH.code AS code"
        )
    )


    course_sections_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_sections")
    cs_df = spark.read.format("delta").load(course_sections_path)\
                    .select("course_section_key", "course_section_id")

    section_listing_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "section_listing_data")
    section_listing_df = spark.read.format("delta").load(section_listing_path)\
                            .selectExpr("course_section_key", "course_listing_number AS course_code")

    cs_with_listing_df = cs_df.join(
        section_listing_df,
        on="course_section_key",
        how="left"
    )

    transfer_credit_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_transfer_credit")
    transfer_credit_df = spark.read.format("delta").load(transfer_credit_path)\
                                    .select("student_id", "course", "credits")

    has_ext_df = (
        transfer_credit_df
        .where(F.col("credits") > 0)
        .select("student_id")
        .distinct()
        .withColumn("has_ext", F.lit(1))
    )

 


    std_course_reg_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_registration")
    std_course_reg_df = (
            spark.read.format("delta").load(std_course_reg_path)
            .filter(F.col("registration_status").isin("Registered", "Completed", "Promoted"))
            .select(
                    F.col("registration_key"),
                    F.col("academic_period"),
                    F.col("course_section_id"),
                    F.col("academic_record_status"),
                    F.col("student_id"),
                    F.col("course_listing"),
                    F.coalesce(
                            F.to_date(F.to_timestamp(F.col("registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
                            F.to_date(F.to_timestamp(F.col("waitlist_date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
                    ).alias("converted_reg_date")
            ))


    # Equivalent to the SQL EXISTS against taxonomy.
    dual_credit_sections_df = (
        taxonomy_joined_df
        .filter(
            F.col("code").isin(
                "1A", "1B", "1E", "8A", "8B",
                "9B", "9C", "9D", "9G", "9H", "9K"
            )
        )
        .select("course_section_key")
        .distinct()
        .withColumn("is_dual_credit", F.lit(1))
    )


    reg = (
        std_course_reg_df.alias("CR")
        .join(cs_with_listing_df.alias("CS")
                ,(F.col("CR.course_section_id") == F.col("CS.course_section_id"))
                ,"left")
        .join(maintained_academic_periods_silver_df.alias("AP")
                ,F.col("CR.academic_period") == F.col("AP.period_name")
                ,"left")
        .join(
            dual_credit_sections_df.alias("DC"),
            F.col("CS.course_section_key") == F.col("DC.course_section_key"),
            "left")
        .join(address_df.alias("STD")
                ,(F.col("CR.student_id") == F.col("STD.student_id"))
                ,"left")\
        .select(
            F.col("CR.student_id"),
            F.col("AP.academic_period_key").alias("academic_period_key"),
            F.col("AP.period_start_date").alias("period_start_date"),
            F.col("AP.period_name").alias("period_name"),
            F.when(F.col("CS.course_code").like("7%"), F.lit(1)).otherwise(F.lit(0)).alias("is_abe_ell"),
            F.coalesce(F.col("DC.is_dual_credit"),F.lit(0)).alias("is_dual_credit")
        )
    )


    entry_window = (
        Window
        .partitionBy("student_id")
        .orderBy("period_start_date")
    )

    entry = (
        reg
        .filter(
            (F.col("is_dual_credit") == 0) &
            (F.col("is_abe_ell") == 0) &
            (F.col("period_start_date").isNotNull())
        )
        .withColumn(
            "rn",
            F.row_number().over(entry_window)
        )
        .filter(F.col("rn") == 1)
        .select(
            "student_id",
            F.col("period_start_date").alias("entry_period_start"),
            F.col("period_name").alias("entry_academic_period")
        )
    )



    r = reg.alias("r")
    e = entry.alias("e")

    evidence = (
        r.join(
            e,
            F.col("r.student_id") ==
            F.col("e.student_id"),
            "inner"
        )
        .groupBy(F.col("r.student_id"))
        .agg(
            F.max(
                F.when(
                    (F.col("r.is_dual_credit") == 1) &
                    (
                        F.col("r.period_start_date") <
                        F.col("e.entry_period_start")
                    ),
                    1
                ).otherwise(0)
            ).alias("has_dual"),
            F.max(
                F.when(
                    (F.col("r.is_abe_ell") == 1) &
                    (
                        F.col("r.period_start_date") <
                        F.col("e.entry_period_start")
                    ),
                    1
                ).otherwise(0)
            ).alias("has_abe")
        )
    )



    s = address_df.alias("s")
    e = entry.alias("e")
    v = evidence.alias("v")
    x = has_ext_df.alias("x")

    final_df = (
        s
        .join(e, F.col("s.student_id") == F.col("e.student_id"), "left")
        .join(v, F.col("s.student_id") == F.col("v.student_id"), "left" )
        .join(x, F.col("s.student_id") == F.col("x.student_id"), "left" )
        .select(
            F.col("s.*"),
            F.when(F.col("e.student_id").isNull(),"Not Applicable")
            .when(F.coalesce(F.col("x.has_ext"), F.lit(0)) == 1,"New: Transferred In Previous Postsecondary Credit")
            .when(F.coalesce(F.col("v.has_dual"), F.lit(0)) == 1,"New: NWTC Dual Credit")
            .when(F.coalesce(F.col("v.has_abe"), F.lit(0)) == 1,"New: NWTC Adult Basic Ed/ELL Credit")
            .otherwise("New: First Time Ever in College")
            .alias("admission_status_name"),
            F.coalesce(F.col("x.has_ext"),F.lit(0)).alias("has_external_postsec_credit"),
            F.coalesce(F.col("v.has_dual"),F.lit(0)).alias("has_nwtc_dual_credit"),
            F.coalesce(F.col("v.has_abe"),F.lit(0)).alias("has_nwtc_abe_ell_only"),
            F.lit(0).alias("is_history")
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
    raise Exception(f"Failed to calculate dim student admission table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # 1) Add SCD2 audit columns
    students_audit_df = add_scd2_audit_column(final_df)
    
    # 3) Proceed with counts for monitoring
    final_count = students_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    # 4) Build target path in gold lakehouse and execute SCD2
    students_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")

    perform_scd_typ2(spark, students_audit_df, students_gold_path, ["student_key"])

    students_audit_df.unpersist()
except Exception as e:
     # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to calculate dim student table: {e}")

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

# CELL ********************

try:
    # Build ABFSS paths to PeopleSoft silver-layer source tables and the dim_students gold target
    ps_std_enrl_path = abfss_path_builder(workspace_id, silver_lakehouse_id, ps_silver_schema_name, "PS_STDNT_ENRL")
    ps_acad_prog_path = abfss_path_builder(workspace_id, silver_lakehouse_id, ps_silver_schema_name, "PS_ACAD_PROG")
    ps_nw_stdnt_path = abfss_path_builder(workspace_id, silver_lakehouse_id, ps_silver_schema_name, "PS_NW_STDNT_PRF_VW")
    ps_drivers_path = abfss_path_builder(workspace_id, silver_lakehouse_id, ps_silver_schema_name, "PS_DIVERS_ETHNIC")
    dim_student_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")

    # Existing dim_students rows coming from Workday only (used to exclude overlap)
    existing_students_df = (
        spark.read.format("delta").load(dim_student_path)
        .filter((F.col("is_history") == 0) & (F.col("is_current") == 1))
        .select("student_id")
        .distinct()
    )

    # Base DF for PeopleSoft student profile view
    ps_nw_stdnt_df = spark.read.format("delta").load(ps_nw_stdnt_path)

    # Enrollment table: course career and units taken by student
    ps_stdnt_enrl_df = (
        spark.read.format("delta").load(ps_std_enrl_path)
        .select("CRSE_CAREER", "UNT_TAKEN", "EMPLID")
    )

    # Academic program table: basic population of EMPLID
    acad_prog_df = (
        spark.read.format("delta").load(ps_acad_prog_path)
        .filter(
            (F.col("EMPLID").isNotNull()) &
            (F.col("EMPLID") != "") &
            (F.length(F.trim(F.col("ADMIT_TERM"))) == 4) &
            (F.trim(F.col("ADMIT_TERM")) >= F.lit("2132")) &
            (F.col("ACAD_CAREER") == "PS")
        )
        .select("EMPLID")
        .distinct()
    )

    # Trim EMPLID once for reuse; this is the PeopleSoft student ID
    p = acad_prog_df.withColumn("EMPLID_TRIM", F.trim(F.col("EMPLID")))

    # Existing Workday students mapped to trimmed EMPLID so we can exclude them
    wd_trimmed = existing_students_df.withColumn("EMPLID_TRIM", F.trim(F.col("student_id")))

    # Students with at least 1 unit taken in PS_STDNT_ENRL in the PS career
    e = (
        ps_stdnt_enrl_df
        .filter(
            (F.col("CRSE_CAREER") == "PS") &
            (F.col("UNT_TAKEN") >= 1)
        )
        .select("EMPLID")
        .distinct()
    )

    # PeopleSoft population to be loaded into dim_students:
    # - Valid admit term (length 4) and >= 2132
    # - Academic career == 'PS'
    # - NOT already in Workday dim_students
    # - EXISTS at least one enrollment row with units >= 1
    ps_population_df = (
        p
        # Anti-join to remove students already present in Workday-sourced dim_students
        .join(
            wd_trimmed.select("EMPLID_TRIM").distinct(),
            on="EMPLID_TRIM",
            how="left_anti"
        )
        # Semi-join to keep only students that appear in enrollment (e)
        .join(
            e,
            on="EMPLID",
            how="left_semi"
        )
        .select(
            F.col("EMPLID_TRIM").alias("EMPLID")
        )
    )

    # ------------------------
    # ps_demo CTE: most recent demographic record per EMPLID
    # ------------------------
    ps_demo_df = (
        ps_nw_stdnt_df
        .where(F.col("EMPLID").isNotNull())
        .withColumn("EMPLID_TRIM", F.trim(F.col("EMPLID")))
        # Rank rows per student by application date and number (latest first)
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("EMPLID_TRIM")
                .orderBy(F.col("ADM_APPL_DT").desc(), F.col("ADM_APPL_NBR").desc())
            )
        )
        .where(F.col("rn") == 1)
        .select(
            F.col("EMPLID_TRIM").alias("EMPLID"),
            "SEX",
            "BIRTHDATE",
            "ETHNICITY_DESCR",
            "POSTAL"
        )
    )

    # ------------------------
    # ps_firstgen CTE: first-generation college indicator per EMPLID
    # ------------------------
    ps_firstgen_df = (
        ps_nw_stdnt_df
        .where(
            (F.col("RESPONSE_REASON") == "1STY") &
            (F.col("EMPLID").isNotNull())
        )
        .withColumn("EMPLID_TRIM", F.trim(F.col("EMPLID")))
        .groupBy("EMPLID_TRIM")
        .agg(
            # If any response contains "NOT", treat as not first-gen (0), else first-gen (1)
            F.max(
                F.when(F.col("DESCR").like("%NOT%"), F.lit(0)).otherwise(F.lit(1))
            ).alias("first_generation")
        )
        .select(
            F.col("EMPLID_TRIM").alias("EMPLID"),
            "first_generation"
        )
    )

    # Aliases for clarity when joining
    p  = ps_population_df.alias("p")
    d  = ps_demo_df.alias("d")
    fg = ps_firstgen_df.alias("fg")

    # Build PeopleSoft-based student records to append into dim_students
    ps_students_df = (
        p
        # Demographic join
        .join(d,  F.col("d.EMPLID")  == F.col("p.EMPLID"),  "left")
        # First-generation flag join
        .join(fg, F.col("fg.EMPLID") == F.col("p.EMPLID"), "left")
        .select(
            # student_key: SHA2_256 of EMPLID, lowercase hex (Spark sha2 already lowercases)
            F.sha2(
                F.col("p.EMPLID").cast("string"),
                256
            ).alias("student_key"),

            # Use EMPLID as student identifier, universal_id, and account name
            F.col("p.EMPLID").alias("student_id"),
            F.col("p.EMPLID").alias("universal_id"),
            F.col("p.EMPLID").alias("account_user_name"),

            # Gender mapping from single-character code
            F.when(F.col("d.SEX") == "F", "Female")
            .when(F.col("d.SEX") == "M", "Male")
            .otherwise(None)
            .alias("gender"),
            F.col("d.BIRTHDATE").alias("date_of_birth"),

            # Placeholder; detailed ethnicity not available from this source
            F.lit(None).cast("string").alias("ethnicity"),

            # Map ETHNICITY_DESCR to standard ethnicity_group codes
            F.when(F.col("d.ETHNICITY_DESCR") == "White",           "USA_White")
            .when(F.col("d.ETHNICITY_DESCR") == "Asian",           "USA_Asian")
            .when(F.col("d.ETHNICITY_DESCR") == "Black",           "USA_Black_or_African_American")
            .when(F.col("d.ETHNICITY_DESCR") == "Hispanic",        "USA_Hispanic_or_Latino")
            .when(F.col("d.ETHNICITY_DESCR") == "American Indian", "USA_American_Indian_or_Alaska_Native")
            .when(F.col("d.ETHNICITY_DESCR") == "Native Hawaiian",
                "USA_Native_Hawaiian_or_Other_Pacific_Islander")
            .when(F.col("d.ETHNICITY_DESCR") == "Two or More",     "USA_Two_or_More_Races")
            .otherwise(None)
            .alias("ethnicity_group"),

            # hispanic_or_latino: NULL when descriptor missing, else 1 if contains 'Hispanic'
            F.when(F.col("d.ETHNICITY_DESCR").isNull(), None)
            .when(F.col("d.ETHNICITY_DESCR").like("%Hispanic%"), F.lit(1))
            .otherwise(F.lit(0))
            .alias("hispanic_or_latino"),

            # First-generation flag from ps_firstgen_df
            F.col("fg.first_generation").alias("first_generation_college_student"),

            # Placeholders for attributes not available in PeopleSoft input
            F.lit(None).cast("boolean").alias("is_veteran"),
            F.lit(None).cast("string").alias("high_school"),
            F.lit(None).cast("string").alias("legal_first_name"),
            F.lit(None).cast("string").alias("legal_last_name"),

            # Address and geography
            F.col("d.POSTAL").alias("postal_code"),
            F.lit(None).cast("string").alias("country"),
            F.lit(None).cast("string").alias("country_region"),

            # Admission status and enrollment attributes not derived from PS here
            F.lit("Not Applicable").alias("admission_status_name"),
            
            F.lit(None).cast("date").alias("high_school_graduation_date"),
            F.lit(None).cast("string").alias("current_student_record_status"),
            F.lit(None).cast("string").alias("communication_usage_type"),
            F.lit(None).cast("string").alias("district_code"),
            F.lit(None).cast("string").alias("county"),
            F.lit(None).cast("int").alias("has_external_postsec_credit"),
            F.lit(None).cast("int").alias("has_nwtc_dual_credit"),
            F.lit(None).cast("int").alias("has_nwtc_abe_ell_only"),
            F.lit(1).alias("is_history")
        )
    )

    # 1) Add SCD2 audit columns (effective dates, current flag, etc.)
    students_audit_df = add_scd2_audit_column(ps_students_df)

    # Cache before reuse (count + SCD load)
    students_audit_df.cache()

    # 3) Record counts for monitoring
    final_count = students_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    # 4) Build target path in gold lakehouse and execute SCD Type 2 merge on student_key
    students_gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")

    perform_scd_typ2(spark, students_audit_df, students_gold_path, ["student_key"])

    # Release cached DataFrame
    students_audit_df.unpersist()
except Exception as e:
    # Surface any failure as a descriptive exception for the pipeline
    raise Exception(f"Failed to calculate dim student from peoplesoft data: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
