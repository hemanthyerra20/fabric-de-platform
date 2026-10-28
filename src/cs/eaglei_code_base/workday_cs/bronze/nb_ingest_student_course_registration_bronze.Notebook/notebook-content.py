# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5bd544a5-340c-493c-aa88-61dc1992c614",
# META       "default_lakehouse_name": "lh_cs_landing",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "5bd544a5-340c-493c-aa88-61dc1992c614"
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

from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

MASTER_RUN_ID = "test123"
RUN_ID = "test123"
PIPELINE_ID = "test123"
PIPELINE_NAME = "test123"
DOMAIN = "cs"
SOURCE_NAME = "workday"
ACTIVITY_NAME = "notebook activity"
LEVEL="notebook"
LAYER="landing"
ENVIRONMENT = "dev"
TABLENAME="Program_Of_Study_all"
SERVICE_NAME = "ras_report"
OPERATION_NAME =  "Get_Program_Of_Study_all"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_landing"
job_name = "nb_ingest_program_of_study_all"
source_name = "workday"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"
service_name = "Ras_Report"
operation_name = "Get_Student_Course_Registration"

landing_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name, lh_name)
print('landing:',landing_workspace_id,landing_lakehouse_id)
bronze_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
bronze_lakehouse_id = get_variable(var_cs_lib_name, "lh_cs_bronze_id")
print('bronze:',bronze_workspace_id,bronze_lakehouse_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Lakehouse Params
landing_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name, lh_name)

landing_abfss_path = abfss_path_builder(landing_workspace_id, landing_lakehouse_id, "workday_cs", "student_course_registration")


landing_df = spark.read.format("delta")\
        .load(landing_abfss_path)

selected_df = landing_df.select(
    F.col("Attempted_Credits").alias("attempted_credits"),
    F.col("Cf_Lrv_Course_Section_ID").alias("cf_lrv_course_section_id"),
    F.col("CF_LRV_Academic_Period_into_Student_Course_section_Record").alias(
        "cf_lrv_academic_period_into_student_course_section_record"
    ),
    F.col("CF_LRV_Student_Academic_Level").alias("cf_lrv_student_academic_level"),
    F.col("CF_LRV_Studnet_Academic_Record_Status").alias(
        "cf_lrv_studnet_academic_record_status"
    ),
    F.col("CF_RLV_Student_ID").alias("cf_rlv_student_id"),
    F.col("Course_Listing").alias("course_listing"),
    F.col("Course_Section").alias("course_section"),
    F.col("Course_Section_Definition").alias("course_section_definition"),
    F.col("Credits").alias("credits"),
    F.col("Drop_Date").alias("drop_date"),
    F.col("Earned_Credits").alias("earned_credits"),
    F.col("GPA_Credits").alias("gpa_credits"),
    F.col("Grade").alias("grade"),
    F.col("Grade_Due_Date").alias("grade_due_date"),
    F.col("Grade_Points").alias("grade_points"),
    F.col("Graded_Date").alias("graded_date"),
    F.col("Grading_Basis").alias("grading_basis"),
    F.col("Grading_Scheme").alias("grading_scheme"),
    F.col("Instructional_Format").alias("instructional_format"),
    F.col("Original_Credits").alias("original_credits"),
    F.col("Registered_Date").alias("registered_date"),
    F.col("Registration_Status").alias("registration_status"),
    F.col("Repeating").alias("repeating"),
    F.col("Student").alias("student"),
    F.col(
        "Primary_Program_of_Study_from_Student_Record_on_Student_Course_Registration_Record"
    ).alias("primary_program_of_study"),
    F.col("Student_Record").alias("student_record"),
    F.col("Waitlist_Date").alias("waitlist_date"),
    F.col("nwtc_cf_lrv_locations_from_course_section_tags").alias("locations_from_course_tags"),
    F.col("campus_locations").alias("campus_locations"),
    F.col("Withdrawal_Date").alias("withdrawal_date"),
    F.col("Program_of_Study").alias("program_of_study"),
    F.col("academic_unit").alias("academic_unit"),
    F.col("modified_on").alias("modified_on")
)

removed_duplicates_df = (
    selected_df
    .selectExpr(
        "*",
        """
        row_number() over (
            partition by cf_rlv_student_id, cf_lrv_course_section_id
            order by
                coalesce(
                    to_timestamp(graded_date, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                    to_timestamp(drop_date, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                    to_timestamp(waitlist_date, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                    to_timestamp(withdrawal_date, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
                    to_timestamp(registered_date, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                ) desc
        ) as rn
        """
    )
    .filter(F.col("rn") == 1)
    .drop("rn", "modified_on")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_abfss_path = abfss_path_builder(landing_workspace_id, bronze_lakehouse_id, "workday_cs", "student_course_registration")

if table_exists(bronze_abfss_path):
        upsert_data(
            bronze_abfss_path,
            removed_duplicates_df,
            ["cf_rlv_student_id", "cf_lrv_course_section_id"]
        )
else:
    writer = (
        removed_duplicates_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .save(bronze_abfss_path)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
