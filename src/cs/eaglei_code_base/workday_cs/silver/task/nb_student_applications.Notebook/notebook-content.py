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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_NAME = ""
DOMAIN = ""
SERVICE_NAME = ""
OPERATION_NAME = ""
SOURCE_NAME = ""
VAR_CS_LIB_NAME = ""
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
SERVICE_NAME = "Admissions"
OPERATION_NAME = "Get_Student_Applications"
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
    parent_object = get_param_value(config_df,SOURCE_NAME, SERVICE_NAME.lower())
    bronze_table_name = parent_object[OPERATION_NAME.lower()]["bronze_table_name"]
    workspace_id = get_workspace_id(VAR_CS_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, silver_lh_name)

    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.student_applications"

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
    bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)
    bronze_df = spark.read.format("delta").load(bronze_abfss_path)
    filtered_df = bronze_df.filter(
    (F.col("student_application_id").isNull()) |
    (F.col("student_application_reference_id").isNull())
)
    from pyspark.sql.functions import lit
    columns_df = spark.createDataFrame([(c,) for c in bronze_df.columns], ["column_name"])
    from pyspark.sql.types import MapType, StringType
# bronze_df_clean = bronze_df.withColumn(
#     "student_application_details_secondary_education_data_clean",
#     F.from_json(
#         F.regexp_replace(
#             F.regexp_replace(
#                 F.col("student_application_details_secondary_education_data"),
#                 '^\\"|\\"$', ""  # remove leading/trailing quotes
#             ),
#             "/", ""               # remove stray slashes
#         ),
#         MapType(StringType(), StringType())
#     )
# )


    bronze_df_clean = bronze_df.withColumn(
    "student_application_details_secondary_education_data_clean",
    F.regexp_replace(
        F.to_json(
            F.col("student_application_details_secondary_education_data")
        ),
        '\\\\',
        ''    )
)
    flat_bronze_df = bronze_df_clean\
                        .select(
                            F.col("student_application_reference_id"),
                            F.col("student_application_reference_wid"),
                            F.col("student_application_id"),
                            F.explode_outer(F.col("student_tags_reference")).alias("student_tags_reference"),
                            # F.col("student_tags_reference.Student_Tag_ID").alias("student_tag_id"),
                            # F.col("student_tags_reference.WID").alias("student_tags_reference_wid"),
                            F.col("student_application_active"),
                            F.col("student_prospect_stage_reference.Student_Prospect_Stage_ID").alias("Student_prospect_stage_id"),
                            F.col("student_prospect_stage_reference.WID").alias("Student_prospect_stage_reference_wid"),
                            F.col("student_applicant_type_reference.Tenanted_Student_Applicant_Type_ID").alias("tenanted_student_applicant_type_id"),
                            F.col("student_applicant_type_reference.WID").alias("student_applicant_type_reference_wid"),
                            F.col("program_of_study_reference.Program_of_Study_ID").alias("program_of_study_id"),
                            F.col("program_of_study_reference.WID").alias("program_of_study_reference_wid"),
                            F.col("academic_period_data.Academic_Period_Reference.Academic_Period_ID").alias("academic_period_id"),
                            F.col("academic_period_data.Academic_Period_Reference.WID").alias("academic_period_reference_wid"),
                            F.col("anticipated_load_status_reference.Student_Load_Status_ID").alias("student_load_status_id"),
                            F.col("anticipated_load_status_reference.WID").alias("anticipated_load_status_reference_wid"),
                            F.col("location_reference.Location_ID").alias("location_id"),
                            F.col("location_reference.WID").alias("location_reference_wid"),                            
                            F.col("student_application_source_reference.Student_Application_Source_ID").alias("student_application_source_id"),
                            F.col("student_application_source_reference.WID").alias("student_application_source_reference_wid"),
                            F.col("graduation_or_equivalency_reference.Student_Graduation_or_Equavalency_ID").alias("student_graduation_or_equavalency_id"),
                            F.col("graduation_or_equivalency_reference.WID").alias("student_graduation_or_equavalency_wid"),
                            F.col("application_date"),
                            F.col("student_application_program_of_study_line_data_id"),
                            F.col("learning_mode_reference"),
                            F.col("payment_amount"),
                            F.col("student_document_data"),
                            F.col("previous_college_attendance"),
                            F.col("first_generation_college_student"),
                            F.col("application_identifier"),
                            F.col("attended_this_institution"),
                            F.col("legacy"),
                            F.col("academic_disciplinary_history"),
                            F.col("homeschooled"),
                            F.col("student_application_details_secondary_education_data_clean"),# its is a string                            
                            F.col("student_application_details_post_secondary_education_data").cast("string"),
                            F.col("selective_service"),
                            F.col("questionnaire_response_data"),
                            F.col("criminal_history"),
                            F.col("application_fee_waived"),
                            F.col("student_prospect_data"),
                        )
    flat_bronze_df_stage2=flat_bronze_df\
                        .select(
                            F.col("student_application_reference_id"),
                            F.col("student_application_reference_wid"),
                            F.col("student_application_id"),
                            F.col("student_tags_reference.Student_Tag_ID").alias("student_tag_id"),
                            F.col("student_tags_reference.WID").alias("student_tags_reference_wid"),
                            F.col("student_application_active"),
                            F.col("Student_prospect_stage_id"),
                            F.col("Student_prospect_stage_reference_wid"),
                            F.col("tenanted_student_applicant_type_id"),
                            F.col("student_applicant_type_reference_wid"),
                            F.col("program_of_study_id"),
                            F.col("program_of_study_reference_wid"),
                            F.col("academic_period_id"),
                            F.col("academic_period_reference_wid"),
                            F.col("student_load_status_id"),
                            F.col("anticipated_load_status_reference_wid"),
                            F.col("location_id"),
                            F.col("location_reference_wid"),
                            F.col("student_application_source_id"),
                            F.col("student_application_source_reference_wid"),
                            F.col("student_graduation_or_equavalency_id"),
                            F.col("student_graduation_or_equavalency_wid"),
                            F.col("application_date"),
                            F.col("student_application_program_of_study_line_data_id"),
                            F.col("learning_mode_reference"),
                            F.col("payment_amount"),
                            F.col("student_document_data"),
                            F.col("previous_college_attendance"),
                            F.col("first_generation_college_student"),
                            F.col("application_identifier"),
                            F.col("attended_this_institution"),
                            F.col("legacy"),
                            F.col("academic_disciplinary_history"),
                            F.col("homeschooled"),
                            #F.col("student_application_details_secondary_education_data_clean"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Educational_Institution_Attendance_Reference.Student_Educational_Institution_Attendance_ID").alias("student_secondary_educational_institution_attendance_id"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Educational_Institution_Attendance_Reference.WID").alias("student_secondary_educational_institution_attendance_wid"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Graduation_Date").alias("student_secondary_educational_graduation_date"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Educational_Institution_Reference.Educational_Institution_ID").alias("student_secondary_educational_institution_id"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Educational_Institution_Reference.WID").alias("student_secondary_educational_institution_id_wid"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Educational_Institution_Reference.Educational_Institution_Secondary_Other_ID").alias("student_secondary_educational_other_id"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Exit_Date").alias("student_secondary_educational_exit_date"),
        F.get_json_object("student_application_details_secondary_education_data_clean","$.Most_Recently_Attended").alias("student_secondary_educational_most_recently_attended"),
        F.col("student_application_details_post_secondary_education_data"),
        F.col("selective_service"),
        F.col("questionnaire_response_data"),
        F.col("criminal_history"),
        F.col("application_fee_waived"),
        F.col("student_prospect_data.Student_Application_Prospect_Subedit_Data.ID").alias("student_prospect_id"),
        F.col("student_prospect_data.Student_Application_Prospect_Subedit_Data.Student_ID").alias("student_id"),
        F.lit("").cast("string").alias("student_prospect_person_data"))
        # F.col("student_prospect_data.Student_Application_Prospect_Subedit_Data.Student_Prospect_Person_Data").alias("student_prospect_person_data")

    audit_df = add_audit_column(flat_bronze_df_stage2, PIPELINE_NAME)
    audit_df2=audit_df.distinct()

    final_count = audit_df2.count()
    rows_read = final_count
    rows_written = final_count

    distinct_row_count = audit_df.distinct().count()
    distinct_key_count = audit_df.select("student_application_reference_id", "student_application_reference_wid","student_application_id","student_tag_id","student_id").distinct().count()

    # Create a small summary DataFrame with matching values/columns
    counts_df = spark.createDataFrame(
    [(distinct_row_count, distinct_key_count)],
    ["distinct_row_count", "distinct_key_count"]
)

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_applications")
    write_delta_tbl(source_df = audit_df2
                ,target_table_path = silver_path
                ,keys = ["student_application_reference_id", "student_application_reference_wid","student_application_id","student_tag_id","student_id"])
    
    audit_df2.unpersist()
                   
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
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
