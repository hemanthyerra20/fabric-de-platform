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
config_table_name = "config_param"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
worksapce_env = get_variable(var_cs_lib_name, "env")
print("worksapce_env:", worksapce_env)

# workspace environment to Key Vault environment
if worksapce_env == "tst":
    kv_env = "test"
elif worksapce_env == "prd":
    kv_env = "prd"
else:
    kv_env = "dev"

print("kv_env:", kv_env)

config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workday_cs_config_dict = {
    "token_url": "https://services1.myworkday.com/ccx/oauth2/nwtc/token",
    "grant_type": "refresh_token",
    "kv_url": f"https://az1-prod-eaglei-{kv_env}-kv.vault.azure.net",
    "api_url": "https://services1.myworkday.com/ccx/service/nwtc/${place_holder}/v46.0",
    "source_name": "workday",
    "kv_client_key_name": "eaglei-workday-client",
    "kv_secret_key_name": "eaglei-workday-secret",
    "kv_std_refreshtoken_name": "eaglei-workday-std-refreshtoken",
    "tenant_name": "nwtc",
    "version": "v46.1",
    "root_path_suffix": "/Files/workday",
    "landing_schema_name": "workday_cs",
    "landing_lakehouse_name": "lh_cs_landing",
    "bronze_schema_name": "workday_cs",
    "bronze_lakehouse_name": "lh_cs_bronze",
    "silver_schema_name": "workday_cs",
    "silver_lakehouse_name": "lh_cs_silver",
    "ras_report" : {
         "get_student_stage_progress_funnel" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Student_Stage_Progression_Funnel?format=json",
            "landing_table_name": "student_stage_progress_funnel",
            "bronze_table_name": "student_stage_progress_funnel",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_stage_progression" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_student_stage_progression?format=json",
            "landing_table_name": "student_stage_progress",
            "bronze_table_name": "student_stage_progress",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_education_credential" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Student_education_credential?format=json",
            "landing_table_name": "student_edu_credential",
            "bronze_table_name": "student_edu_credential",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_pellgrant_eligible" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_GetPellGrant_Eligible?format=json",
            "landing_table_name": "student_pellgrant",
            "bronze_table_name": "student_pellgrant",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_changes" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Get_Student_Changes?format=json",
            "landing_table_name": "student_changes",
            "bronze_table_name": "student_changes",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_enrollment" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Student_Enrollment_term_and_Level_Report?format=json",
            "landing_table_name": "student_enrollment",
            "bronze_table_name": "student_enrollment",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_course_registration" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Student_Course_Registration_Report?format=json",
            "landing_table_name": "student_course_registration",
            "bronze_table_name": "student_course_registration",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_program_of_study_all" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle_1_Program_of_study_All_records_RaaS?format=json",
            "landing_table_name": "program_of_study_all",
            "bronze_table_name": "program_of_study_all",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_prospect": {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Prospect?format=json",
            "landing_table_name": "prospect",
            "bronze_table_name": "prospect",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_educational_taxonomy_schemes": {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Find_Educational_Taxonomy_Schemes?format=json",
            "landing_table_name": "educational_taxonomy_schemes",
            "bronze_table_name": "educational_taxonomy_schemes",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_campus_locations": {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_GetCampusLocations?format=json",
            "landing_table_name": "campuslocations",
            "bronze_table_name": "campuslocations",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_transfer_credits": {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR__Eagle1_Student_Transfer_Credits?format=json",
            "landing_table_name": "student_transfer_credit",
            "bronze_table_name": "student_transfer_credit",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_pell_recalculation": {
            "landing_table_name": "academic_pell_recalc",
            "bronze_table_name": "academic_pell_recalc",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_student_application_decision": {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle1_Student_Application_Decision?format=json",
            "landing_table_name": "student_application_decision",
            "bronze_table_name": "student_application_decision",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        }
    },
    "integrations" : {
        "get_country_subregion_internal_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "country_subregion_internal_references",
            "bronze_table_name": "country_subregion_internal_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Country_Subregion_Internal_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_communication_usage_type_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "communication_usage_type_references",
            "bronze_table_name": "communication_usage_type_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Communication_Usage_Type_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_country_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "country_references",
            "bronze_table_name": "country_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>ISO_3166-1_Alpha-2_Code</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_country_region_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "country_region_references",
            "bronze_table_name": "country_region_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Country_Region_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_ethnicity_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "ethnicity_references",
            "bronze_table_name": "ethnicity_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Ethnicity_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_delivery_mode_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "delivery_mode_references",
            "bronze_table_name": "delivery_mode_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Delivery_Mode_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_instructional_format_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "instructional_format_references",
            "bronze_table_name": "instructional_format_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Instructional_Format_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_academic_period_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "academic_period_references",
            "bronze_table_name": "academic_period_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Academic_Period_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_student_course_tag_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "student_course_tag_references",
            "bronze_table_name": "student_course_tag_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Student_Course_Tag_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_academic_unit_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "academic_unit_references",
            "bronze_table_name": "academic_unit_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Academic_Unit_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_student_record_status_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "student_record_status_references",
            "bronze_table_name": "student_record_status_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Student_Record_Status_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_student_load_status_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "student_load_status_references",
            "bronze_table_name": "student_load_status_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Student_Load_Status_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_student_grade_element_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "student_grade_element_references",
            "bronze_table_name": "student_grade_element_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Student_Grade_Element_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_educational_taxonomy_code_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "educational_taxonomy_code_references",
            "bronze_table_name": "educational_taxonomy_code_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Educational_Taxonomy_Code_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_educational_credential_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "educational_credential_references",
            "bronze_table_name": "educational_credential_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Educational_Credential_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_academic_level_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "academic_level_references",
            "bronze_table_name": "academic_level_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Academic_Level_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_cip_code_references" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "cip_code_references",
            "bronze_table_name": "cip_code_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Cip_Code_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        },
        "get_tenanted_student_applicant_type" : {
            "tag_name" : "Body.Get_References_Response.Response_Data.Reference_ID",
            "count_tag_name": "Body.Get_References_Response.Response_Results.Total_Results",
            "landing_table_name": "tenanted_student_applicant_type_references",
            "bronze_table_name": "tenanted_student_applicant_type_references",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_References_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bvsc:Request_Criteria><bvsc:Reference_ID_Type>Tenanted_Student_Applicant_Type_ID</bvsc:Reference_ID_Type></bvsc:Request_Criteria><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><!--Optional:--><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_References_Request></soapenv:Body></soapenv:Envelope>"
        }
    },
    "human_resources":{
        "get_organizations_request" : {
            "tag_name" : "Body.Get_Organizations_Response.Response_Data.Organization",
            "count_tag_name": "Body.Get_Organizations_Response.Response_Results.Total_Results",
            "landing_table_name": "organizations",
            "bronze_table_name": "organizations",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:wd=\"urn:com.workday/bsvc\"><soapenv:Body><wd:Get_Organizations_Request wd:version=\"v46.1\"><wd:Request_References><wd:Organization_Reference><wd:ID wd:type=\"Organization_Reference_ID\">All_NWTC_Locations</wd:ID></wd:Organization_Reference></wd:Request_References><wd:Response_Filter><wd:Page>1</wd:Page><wd:Count>1</wd:Count></wd:Response_Filter></wd:Get_Organizations_Request></soapenv:Body></soapenv:Envelope>"
            }
    },
    "academic_foundation": {
        "get_students": {
            "tag_name": "Body.Get_Students_Response.Response_Data.Student",
            "count_tag_name": "Body.Get_Students_Response.Response_Results.Total_Results",
            "landing_table_name": "students",
            "bronze_table_name": "students",
            "response_body_block": "<bsvc:Response_Group><!--Optional:--><bsvc:Include_Reference>true</bsvc:Include_Reference><!--Optional:--><bsvc:Include_Person_Data>true</bsvc:Include_Person_Data><!--Optional:--><bsvc:Include_Additional_Data_In_Response>true</bsvc:Include_Additional_Data_In_Response><!--Optional:--><bsvc:Include_Program_of_Study_Record_Data>true</bsvc:Include_Program_of_Study_Record_Data><!--Optional:--><bsvc:Include_Registered_Registration_Data>true</bsvc:Include_Registered_Registration_Data><!--Optional:--><bsvc:Include_Student_Friends_and_Family_Data>false</bsvc:Include_Student_Friends_and_Family_Data><!--Optional:--><bsvc:Include_Student_Housing_Assignment_Data>true</bsvc:Include_Student_Housing_Assignment_Data><!--Optional:--><bsvc:Include_Student_Deceased_Data>false</bsvc:Include_Student_Deceased_Data><!--Optional:--><bsvc:Include_Discontinued_Programs>true</bsvc:Include_Discontinued_Programs></bsvc:Response_Group>"
        },
        "get_maintained_academic_periods": {
            "tag_name": "Response_Data.Maintained_Academic_Period",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "maintained_academic_periods",
            "landing_table_name": "maintained_academic_periods"
        },
        "get_programs_of_study": {
            "tag_name": "Response_Data.Program_of_Study",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "programs_of_study",
            "landing_table_name": "programs_of_study"
        },
        "get_student_tags": {
            "tag_name": "Response_Data.Student_Tag",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_tags",
            "landing_table_name": "student_tags"
        },
        "get_student_educational_institution": {
            "tag_name": "Response_Data.Student_Educational_Institution",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_educational_institution",
            "landing_table_name": "student_educational_institution"
        },
        "get_student_educational_institution_districts": {
            "tag_name": "Body.Get_Student_Educational_Institution_Districts_Response.Response_Data.Student_Educational_Institution_District",
            "count_tag_name": "Body.Get_Student_Educational_Institution_Districts_Response.Response_Results.Total_Results",
            "bronze_table_name": "student_educational_institution_districts",
            "landing_table_name": "student_educational_institution_districts"
        },
        "get_academic_period_date_controls": {
            "tag_name": "Response_Data.Academic_Period_Date_Controls",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "academic_period_date_controls",
            "landing_table_name": "academic_period_date_controls"
        },
        "get_student_grading_schemes_request": {
            "tag_name": "Body.Get_Student_Grading_Schemes_Response.Response_Data.Student_Grading_Scheme",
            "count_tag_name": "Body.Get_Student_Grading_Schemes_Response.Response_Results.Total_Results",
            "bronze_table_name": "student_grading_schemes",
            "landing_table_name": "student_grading_schemes",
            "soap_body": "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bsvc=\"urn:com.workday/bsvc\"><soapenv:Body><bsvc:Get_Student_Grading_Schemes_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bsvc:Get_Student_Grading_Schemes_Request></soapenv:Body></soapenv:Envelope>"
        }
    },
    "academic_advising": {
        "get_student_cohorts": {
            "tag_name": "Response_Data.Student_Cohort",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_cohorts",
            "landing_table_name": "student_cohorts"
        },
        "get_student_cohort_memberships": {
            "tag_name": "Body.Get_Student_Cohort_Memberships_Response.Response_Data.Student_Cohort_Membership",
            "count_tag_name": "Body.Get_Student_Cohort_Memberships_Response.Response_Results.Total_Results",
            "bronze_table_name": "student_cohort_memberships",
            "landing_table_name": "student_cohort_memberships",
            "response_body_block": "<bsvc:Response_Group><!--Optional:--><bsvc:Include_Reference>true</bsvc:Include_Reference></bsvc:Response_Group>"
        }
    },
    "financial_aid": {
        "get_isir_transactions_request":{
            "tag_name": "Body.Get_ISIR_Transactions_Response.Response_Data.ISIR_Transaction",
            "count_tag_name": "Body.Get_ISIR_Transactions_Response.Response_Results.Total_Results",
            "bronze_table_name": "isir_transactions_request",
            "landing_table_name": "isir_transactions_request",
            "soap_body":"<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:bvsc=\"urn:com.workday/bsvc\"><soapenv:Body><bvsc:Get_ISIR_Transactions_Request xmlns:bsvc=\"urn:com.workday/bsvc\"><bsvc:Response_Filter><bsvc:Page>1</bsvc:Page><bsvc:Count>100</bsvc:Count></bsvc:Response_Filter></bvsc:Get_ISIR_Transactions_Request></soapenv:Body></soapenv:Envelope>"
        }
    },
    "student_core": {
        "get_student_residencies": {
            "tag_name": "Body.Get_Student_Residencies_Response.Response_Data.Student_Residency",
            "count_tag_name": "Body.Get_Student_Residencies_Response.Response_Results.Total_Results",
            "bronze_table_name": "student_residencies",
            "landing_table_name": "student_residencies",
            "response_body_block": "<bsvc:Response_Group><!--Optional:--><bsvc:Include_Reference>true</bsvc:Include_Reference><!--Optional:--><bsvc:Include_Student_Documents_in_Response>false</bsvc:Include_Student_Documents_in_Response></bsvc:Response_Group>"
        },
        "get_student_veterans_benefits": {
            "tag_name": "Response_Data.Student_Veteran_Benefits",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_veterans_benefits",
            "landing_table_name": "student_veterans_benefits"
        }
    },
    "admissions": {
        "get_student_application_admission_responses": {
            "tag_name": "Response_Data.Student_Application_Admission_Response",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_application_admission_responses",
            "landing_table_name": "student_application_admission_responses"
        },
        "get_student_applications": {
            "tag_name": "Body.Get_Student_Applications_Response.Response_Data.Student_Application",
            "count_tag_name": "Body.Get_Student_Applications_Response.Response_Results.Total_Results",
            "bronze_table_name": "student_applications",
            "landing_table_name": "student_applications",
            "response_body_block": "<bsvc:Response_Group><!--Optional:--><bsvc:Include_Reference>true</bsvc:Include_Reference><!--Optional:--><bsvc:Include_Person_Data>true</bsvc:Include_Person_Data><!--Optional:--><bsvc:Include_Friends_and_Family_Data>false</bsvc:Include_Friends_and_Family_Data><!--Optional:--><bsvc:Include_Supporting_Material_Data>true</bsvc:Include_Supporting_Material_Data><!--Optional:--><bsvc:Include_Additional_Data_In_Response>true</bsvc:Include_Additional_Data_In_Response></bsvc:Response_Group>"
        }
    },
    "student_records": {
        "get_student_course_tag_categories": {
            "tag_name": "Response_Data.Student_Course_Tag_Category",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_course_tag_categories",
            "landing_table_name": "student_course_tag_categories"
        },
        "get_student_course_tags": {
            "tag_name": "Response_Data.Student_Course_Tag",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_course_tags",
            "landing_table_name": "student_course_tags"
        },
        "get_reporting_records": {
            "tag_name": "Response_Data.Reporting_Record",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "reporting_records",
            "landing_table_name": "reporting_records"
        },
        "get_program_of_study_assignment_snapshots": {
            "tag_name": "Response_Data.Program_of_Study_Assignment_Snapshot_for_Academic_Record_Data",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "program_of_study_assignment_snapshots",
            "landing_table_name": "program_of_study_assignment_snapshots"
        },
        "get_historical_academic_records": {
            "tag_name": "Response_Data.Historical_Academic_Record",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "historical_academic_records",
            "landing_table_name": "historical_academic_records"
        },
        "get_course_sections": {
            "tag_name": "Response_Data.Course_Section",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "course_sections",
            "landing_table_name": "course_sections"
        },
        "get_student_schedules_for_registration": {
            "tag_name": "Response_Data.Student_Schedule_for_Registration",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_schedules_for_registration",
            "landing_table_name": "student_schedules_for_registration"
        },
        "get_student_location_assignment_snapshots": {
            "tag_name": "Response_Data.Student_Location_Assignment",
            "count_tag_name": "Response_Results.Total_Results",
            "bronze_table_name": "student_location_assignment_snapshots",
            "landing_table_name": "student_location_assignment_snapshots"
        }
    }
}

# Serialize to compact JSON string for param_value
workday_param_value_str = json.dumps(workday_cs_config_dict, separators=(",",":"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

canvas_config_dict = {
"sync_mode" : "FULL" ,
"incremental_since" : "2026-05-20T00:00:00Z",
"base_url":"https://api-gateway.instructure.com",
"cd2_format"  : "jsonl",
"poll_interval_sec":20,
"kv_url"      : f"https://az1-prod-eaglei-{kv_env}-kv.vault.azure.net",
"kv_client_key_name"  : "cv-eaglei-client-id",
"kv_secret_key_name"  : "cv-eaglei-client-secret",
"landing_schema_name" : "canvas",
"landing_lakehouse_name": "lh_cs_landing",
"bronze_schema_name": "canvas",
"bronze_lakehouse_name": "lh_cs_bronze",
"silver_schema_name": "canvas",
"silver_lakehouse_name": "lh_cs_silver",
"root_path_suffix": "/Files/canvas",
"entities": {
    
    "courses":"courses",
    "course_sections":"course_sections",
    "enrollments":"enrollments",
    "enrollment_terms":"enrollment_terms",
    
    "learning_outcomes":"learning_outcomes",
    "learning_outcome_results":"learning_outcome_results",
    "learning_outcome_groups":"learning_outcome_groups",
    
    "assignments":"assignments",
    "submissions":"submissions",
    "rubric_assessments":"rubric_assessments",
    
    "users":"users",
    "pseudonyms":"pseudonyms"
    }


}

# Serialize to compact JSON string for param_value
canvas_param_value_str = json.dumps(canvas_config_dict, separators=(",",":"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# 2. Define schema
# ------------------------------------------------------------------
schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("param_values", StringType(), True),
])

# ------------------------------------------------------------------
# 3. Create a single-row DataFrame
# ------------------------------------------------------------------
rows = [Row(
    id=str(uuid.uuid4()),
    source="workday",
    domain="cs",
    param_values=workday_param_value_str,
),
# Add new rows here
Row(
    id=str(uuid.uuid4()),
    source="canvas",
    domain="cs",
    param_values=canvas_param_value_str,
)
]

df_config = spark.createDataFrame(rows, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Staged data (new/changed records)
staged_df = df_config

try:
    delta_table = DeltaTable.forPath(spark, config_abfss_path)
except AnalysisException:
    # Table/path does not exist yet – create it from the staged data
    (
        staged_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(config_abfss_path)
    )
    delta_table = DeltaTable.forPath(spark, config_abfss_path)

# Only treat as "changed" when any of these columns differ
change_condition = (
    "tgt.param_values <> src.param_values"
)

(
    delta_table.alias("tgt")
    .merge(
        staged_df.alias("src"),
        "tgt.source = src.source AND tgt.domain = src.domain"   # join condition (business key)
    )
    .whenMatchedUpdate(
        condition=change_condition,
        set={
            "param_values": "src.param_values",
        },
    )
    .whenNotMatchedInsert(
        values={
            "id": "src.id",
            "source": "src.source",
            "domain": "src.domain",
            "param_values": "src.param_values",
        },
    )
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Successfully inserted/updated the config details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
