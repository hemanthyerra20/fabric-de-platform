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

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
import json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "pipeline_config_param"

config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)


config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

table_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("source", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("Service", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("active", StringType(), True),
    StructField("landing_notebook", StringType(), True),
    StructField("bronze_notebook", StringType(), True),
    StructField("silver_notebook", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("modified_on", StringType(), True),
    StructField("modified_by", StringType(), True),
])

now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

default_active = "1"
default_landing_notebook = "nb_ingest_landing_layer"
default_bronze_notebook = "nb_ingest_bronze_layer"
default_created_on = now_str
default_created_by = "nb_ingest_pipeline_param_config_table"
default_modified_on = now_str
default_modified_by = "nb_ingest_pipeline_param_config_table"

rows = [
    Row("workday", "cs", "Ras_Report", "Get_Student_Enrollment", default_active , default_landing_notebook, default_bronze_notebook, "nb_student_enrollment", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Student_Course_Registration", default_active, "nb_ingest_student_course_registration", "nb_ingest_student_course_registration_bronze", "nb_course_registration", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Program_Of_Study_all", default_active, "nb_ingest_program_of_study_all_landing", "nb_ingest_program_of_study_all_bronze", "nb_ingest_program_of_study_all", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Prospect", 0, default_landing_notebook, default_bronze_notebook, "nb_prospect", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Educational_Taxonomy_Schemes", default_active, default_landing_notebook, default_bronze_notebook, "nb_educational_taxonomy_schemes", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Campus_Locations", default_active, default_landing_notebook, default_bronze_notebook, "nb_campus_locations", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Student_Transfer_Credits", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_transfer_credit", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Country_Subregion_Internal_References", '0', default_landing_notebook, default_bronze_notebook, "nb_country_subregion_internal_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Communication_Usage_Type_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_communication_usage_type_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Country_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_country_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Country_Region_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_country_region_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Ethnicity_References", '0', default_landing_notebook, default_bronze_notebook, "nb_ethnicity_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Delivery_Mode_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_delivery_mode_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Instructional_Format_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_instructional_format_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Academic_Period_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_academic_period_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Student_Course_Tag_References", '0', default_landing_notebook, default_bronze_notebook, "nb_student_course_tag_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Academic_Unit_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_academic_unit_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Student_Record_Status_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_record_status_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Student_Load_Status_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_load_status_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Student_Grade_Element_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_grade_reference", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Educational_Taxonomy_Code_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_educational_taxonomy_code_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Educational_Credential_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_educational_credential_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Academic_Level_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_academic_level_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Cip_Code_References", default_active, default_landing_notebook, default_bronze_notebook, "nb_cip_code_references", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Integrations", "Get_Tenanted_Student_Applicant_Type", default_active, default_landing_notebook, default_bronze_notebook, "nb_tenanted_student_applicant_type", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Human_Resources", "Get_Organizations_Request", '0', default_landing_notebook, default_bronze_notebook, "nb_organizations", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Foundation", "Get_Students", default_active, default_landing_notebook, default_bronze_notebook, "nb_students", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Foundation", "Get_Maintained_Academic_Periods", default_active, default_landing_notebook, default_bronze_notebook, "nb_maintained_academic_periods", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Foundation", "Get_Programs_of_Study", default_active, default_landing_notebook, default_bronze_notebook, "nb_programs_of_study", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Foundation", "Get_Student_Tags", '0', default_landing_notebook, default_bronze_notebook, "nb_student_tags", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Foundation", "Get_Academic_Period_Date_Controls", default_active, default_landing_notebook, default_bronze_notebook, "nb_academic_period_date_controls", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Foundation", "Get_Student_Grading_Schemes_Request", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_grade_schemes", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Advising", "Get_Student_Cohorts", '0', default_landing_notebook, default_bronze_notebook, "nb_student_cohorts", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Academic_Advising", "Get_Student_Cohort_Memberships", '0', default_landing_notebook, default_bronze_notebook, "nb_student_cohort_memberships", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Financial_Aid", "Get_Isir_Transactions_Request", '0', default_landing_notebook, default_bronze_notebook, "nb_pell_eligibility", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Core", "Get_Student_Residencies", '0', default_landing_notebook, default_bronze_notebook, "nb_student_residencies", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Core", "Get_Student_Veterans_Benefits", '0', default_landing_notebook, default_bronze_notebook, "nb_student_veterans_benefits", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Admissions", "Get_Student_Application_Admission_Responses", '0', default_landing_notebook, default_bronze_notebook, "nb_student_application_admission_responses", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Student_Application_Decision", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_application_admission_responses", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Admissions", "Get_Student_Applications", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_applications", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Student_Course_Tag_Categories", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_course_tag_categories", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Student_Course_Tags", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_course_tags", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Reporting_Records", default_active, default_landing_notebook, default_bronze_notebook, "nb_reporting_records", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Program_of_Study_Assignment_Snapshots", '0', default_landing_notebook, default_bronze_notebook, "nb_program_of_study_assignment_snapshots", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Historical_Academic_Records", '0', default_landing_notebook, default_bronze_notebook, "nb_historical_academic_records", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Course_Sections", default_active, default_landing_notebook, default_bronze_notebook, "nb_course_sections", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Student_Schedules_for_Registration", '0', default_landing_notebook, default_bronze_notebook, "nb_student_schedules_for_registration", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Student_Records", "Get_Student_Location_Assignment_Snapshots", '0', default_landing_notebook, default_bronze_notebook, "nb_student_location_assignment_snapshots", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Student_Pell_Recalculation", default_active, "nb_ingest_pellrecalc_landing_layer", "nb_ingest_bronze_layer_pellrecal", "nb_academic_pelrecalculate", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Pellgrant_Eligible", default_active, default_landing_notebook, default_bronze_notebook, "nb_pellgrant_eligible", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Student_Education_Credential", default_active, default_landing_notebook, default_bronze_notebook, "nb_student_education_credential", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "cs", "Ras_Report", "Get_Student_Stage_Progress_Funnel", default_active, "nb_ingest_landing_layer_prospect_stage", default_bronze_notebook, "nb_prospect_funnel_stage", default_created_on, default_created_by, default_modified_on, default_modified_by),
    
]

df_source_services = spark.createDataFrame(rows, schema)

display(df_source_services)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_delta_tbl(
        source_df=df_source_services,
        target_table_path=table_path,
        keys=["source", "domain", "Service", "operation"]
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Successfully inserted/updated the pipeline config param details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_pipeline_config = spark.read.format("delta").load(table_path)
display(df_pipeline_config)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
