# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d35b1ce6-d133-4623-9d58-8c88aba0654b",
# META       "default_lakehouse_name": "lh_platform_monitoring",
# META       "default_lakehouse_workspace_id": "b0bd8a9b-9bdf-4a90-99cf-fed9e6800f1a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         },
# META         {
# META           "id": "b229f4c0-e27f-4fb4-92f4-5d4edc0a773c"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d6e9c4b7-fdc8-4966-afb3-443827650df9",
# META       "workspaceId": "088bba09-dd86-49a0-87f3-ad9331892945"
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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_hr_lib_name = "var_hr_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "xpath_details"
notebook_name = notebookutils.runtime.context["currentNotebookName"]


config_workspace_id = get_variable(var_hr_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_hr_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def workday_cs_generate_data(table_name: str, xpath: dict, primary_key: str,explode_and_pivot_required: dict | None = None, partition_by: str | None = None):
    """Generate configuration rows for a given table/xpath mapping.

    For each (alias_name, path) pair in the `xpath` mapping, this function
    builds a tuple containing:
      - a shared UUID `id` (same for all rows in this batch)
      - static `source` and `domain` values
      - the provided `table_name`
      - the alias name and xpath
      - a JSON string describing explode/pivot details (if configured)

    Args:
        table_name (str): Target logical table name (e.g., bronze table).
        xpath (dict): Mapping of alias_name -> xpath string.
        explode_and_pivot_required (dict | None): Optional mapping where the
            key is alias_name and the value is a dict describing which columns
            should be excluded from explode/pivot or any other required
            metadata.

    Returns:
        list[tuple]: List of configuration tuples ready to be turned into a
        DataFrame.
    """

    # Guard against None to avoid AttributeError when accessing .keys()
    if explode_and_pivot_required is None:
        explode_and_pivot_required = {}

    # Generate a single UUID for this batch so all related rows share the same id
    batch_id = str(uuid.uuid4())
    source = "workday"
    domain = "hr"

    data_lst = []

    try:
        # Precompute a case-insensitive set of keys from explode_and_pivot_required
        explode_keys_lower = {k.lower() for k in explode_and_pivot_required.keys()}

        for alias_name, path in xpath.items():
            # Determine if this alias has explode/pivot metadata configured
            if alias_name.lower() in explode_keys_lower:
                # Use the original key lookup (case-sensitive) to fetch details
                # We assume the exact alias_name exists; if not, this will
                # raise a KeyError, which we allow to propagate.
                explode_details = {
                    alias_name: explode_and_pivot_required[alias_name]
                }

                # Serialize explode/pivot configuration to a compact JSON string
                explode_details_str = json.dumps(explode_details, separators=(",", ":"))
            else:
                explode_details_str = None

            # Append a configuration tuple for this alias/xpath pair
            data_lst.append(
                (batch_id, source, domain, table_name, alias_name, path, explode_details_str, primary_key, partition_by)
            )

        return data_lst

    except Exception:
        # Re-raise the original exception so the caller keeps full type and
        # traceback. This allows upstream orchestration code to decide how
        # to handle configuration or schema issues.
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

academic_unit_xpath = {
    # ------------------------------------------------------------------
    # Top-level academic unit
    # ------------------------------------------------------------------
    "id": "Academic_Unit_Data>ID",
    "is_institution": "Academic_Unit_Data>Is_Institution",

    # Academic organization subtype (struct)
    # Keep parent struct plus one ID column
    "academic_organization_subtype_reference":
        "Academic_Unit_Data>Academic_Organization_Subtype_Reference",
    "academic_organization_subtype_id":
        "Academic_Unit_Data>Academic_Organization_Subtype_Reference",

    # ------------------------------------------------------------------
    # Snapshot data (array) – usually exploded later
    # ------------------------------------------------------------------
    "academic_unit_snapshot_data":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data",

    # Fields inside each snapshot row
    "snapshot_inactive":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Inactive",
    "snapshot_name":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Name",
    "snapshot_code":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Code",

    # Company reference inside snapshot (struct)
    # Parent struct + one ID column
    "snapshot_company_reference":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Company_Reference",
    "snapshot_company_reference_id":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Company_Reference",

    "snapshot_effective_date":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Effective_Date",
    "snapshot_include_incumbent_leader_in_name":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Include_Incumbent_Leader_in_Name",
    "snapshot_include_subtype_in_name":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Include_Subtype_in_Name",
    "snapshot_include_code_in_name":
        "Academic_Unit_Data>Academic_Unit_Snapshot_Data>Include_Code_in_Name",

    # Audit
    "modified_on": "modified_on",
}

academic_unit_tbl = "lh_hr_bronze.workday_hr.academic_unit"

academic_unit = workday_cs_generate_data(
    table_name=academic_unit_tbl,
    xpath=academic_unit_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


academic_appointee_xpath = {
    # ------------------------------------------------------------------
    # Person Photo
    # ------------------------------------------------------------------
    "photo_file":
        "academic_appointee_data>Person_Photo_Data>File",
    "photo_filename":
        "academic_appointee_data>Person_Photo_Data>Filename",

    # ------------------------------------------------------------------
    # Personal Information
    # ------------------------------------------------------------------
    "uses_tobacco":
        "academic_appointee_data>Personal_Information_Data>Uses_Tobacco",
    "date_of_birth":
        "academic_appointee_data>Personal_Information_Data>Date_of_Birth",
    "citizenship_reference":
        "academic_appointee_data>Personal_Information_Data>Citizenship_Reference",   # []
    "ethnicity_reference":
        "academic_appointee_data>Personal_Information_Data>Ethnicity_Reference",     # array of {ID: [...]}
    "disability_information_data":
        "academic_appointee_data>Personal_Information_Data>Disability_Information_Data",
    "gender_id":
        "academic_appointee_data>Personal_Information_Data>Gender_Reference",
    "military_information_data":
        "academic_appointee_data>Personal_Information_Data>Military_Information_Data",

    # ------------------------------------------------------------------
    # Person Identification
    # ------------------------------------------------------------------
    "visa_id":
        "academic_appointee_data>Person_Identification_Data>Visa_ID",
    "government_id":
        "academic_appointee_data>Person_Identification_Data>Government_ID",
    "license_id":
        "academic_appointee_data>Person_Identification_Data>License_ID",
    "custom_id":
        "academic_appointee_data>Person_Identification_Data>Custom_ID",


    "national_id_country_id": "academic_appointee_data>Person_Identification_Data>National_ID>National_ID_Data>Country_Reference",
    # "national_id_value":      "academic_appointee_data>Person_Identification_Data>National_ID>National_ID_Data",
    "national_id_type_id":    "academic_appointee_data>Person_Identification_Data>National_ID>National_ID_Data>ID_Type_Reference",
    "national_id_reference_id": "academic_appointee_data>Person_Identification_Data>National_ID>National_ID_Reference",
    "national_id_shared_reference_id": "academic_appointee_data>Person_Identification_Data>National_ID>National_ID_Shared_Reference",
    # ------------------------------------------------------------------
    # Person Data – names
    # ------------------------------------------------------------------
    "additional_name_data":
        "academic_appointee_data>Person_Data>Additional_Name_Data",

    # Legal name
    "legal_first_name":
        "academic_appointee_data>Person_Data>Legal_Name_Data>Name_Detail_Data>First_Name",
    "legal_formatted_name":
        "academic_appointee_data>Person_Data>Legal_Name_Data>Name_Detail_Data>Formatted_Name",
    "legal_reporting_name":
        "academic_appointee_data>Person_Data>Legal_Name_Data>Name_Detail_Data>Reporting_Name",
    "legal_last_name":
        "academic_appointee_data>Person_Data>Legal_Name_Data>Name_Detail_Data>Last_Name",
    "legal_middle_name":
        "academic_appointee_data>Person_Data>Legal_Name_Data>Name_Detail_Data>Middle_Name",
    "legal_country_id":
        "academic_appointee_data>Person_Data>Legal_Name_Data>Name_Detail_Data>Country_Reference",

    # Preferred name
    "preferred_first_name":
        "academic_appointee_data>Person_Data>Preferred_Name_Data>Name_Detail_Data>First_Name",
    "preferred_formatted_name":
        "academic_appointee_data>Person_Data>Preferred_Name_Data>Name_Detail_Data>Formatted_Name",
    "preferred_reporting_name":
        "academic_appointee_data>Person_Data>Preferred_Name_Data>Name_Detail_Data>Reporting_Name",
    "preferred_last_name":
        "academic_appointee_data>Person_Data>Preferred_Name_Data>Name_Detail_Data>Last_Name",
    "preferred_middle_name":
        "academic_appointee_data>Person_Data>Preferred_Name_Data>Name_Detail_Data>Middle_Name",
    "preferred_country_id":
        "academic_appointee_data>Person_Data>Preferred_Name_Data>Name_Detail_Data>Country_Reference",

    # ------------------------------------------------------------------
    # Appointment (array) – you will typically explode this later
    # ------------------------------------------------------------------
    "roster_percent":
        "academic_appointee_data>Appointment_Data>Roster_Percent",
    "rank_id":
        "academic_appointee_data>Appointment_Data>Rank_Reference",
    "track_type_id":
        "academic_appointee_data>Appointment_Data>Track_Type_Reference",
    "academic_unit_id":
        "academic_appointee_data>Appointment_Data>Academic_Unit_Reference",
    "appointment_title":
        "academic_appointee_data>Appointment_Data>Appointment_Title",
    "academic_appointment_track_id":
        "academic_appointee_data>Appointment_Data>Academic_Appointment_Track_Reference",
    "appointment_identifier_id":
        "academic_appointee_data>Appointment_Data>Appointment_Identifier_Reference",
    "position_id":
        "academic_appointee_data>Appointment_Data>Position_Reference",
    "constructed_title":
        "academic_appointee_data>Appointment_Data>Constructed_Title",
    "appointment_track_id":
        "academic_appointee_data>Appointment_Data>Appointment_Track_ID",
    "appointment_track_inactive":
        "academic_appointee_data>Appointment_Data>Appointment_Track_Inactive",
    "appointment_start_date":
        "academic_appointee_data>Appointment_Data>Appointment_Start_Date",

    # Audit
    "modified_on": "modified_on",
}

academic_appointee_tbl = "lh_hr_bronze.workday_hr.academic_appointee"

academic_appointee = workday_cs_generate_data(
    table_name=academic_appointee_tbl,
    xpath=academic_appointee_xpath,
    primary_key="appointment_identifier_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

compensation_plan_xpath = {
    # Top-level fields
    "inactive": "compensation_plan_data>Inactive",
    "name": "compensation_plan_data>Name",
    "compensation_plan_id": "compensation_plan_data>Compensation_Plan_ID",
    "effective_date": "compensation_plan_data>Effective_Date",

    # Allowance Plan Data - Currency Reference
    "currency_reference_id": "compensation_plan_data>Allowance_Plan_Data>Currency_Reference",

    # Allowance Plan Data - Flags & Settings
    "no_individual_override": "compensation_plan_data>Allowance_Plan_Data>No_Individual_Override",
    "frequency_reference_id": "compensation_plan_data>Allowance_Plan_Data>Frequency_Reference",
    "compensation_element_reference_id": "compensation_plan_data>Allowance_Plan_Data>Compensation_Element_Reference",
    "exclude_from_merit": "compensation_plan_data>Allowance_Plan_Data>Exclude_from_Merit",
    "amount_data": "compensation_plan_data>Allowance_Plan_Data>Amount_Data",
    "apply_full_time_equivalent": "compensation_plan_data>Allowance_Plan_Data>Apply_Full_Time_Equivalent",
    "use_assigned_value_in_100_percent_calculation": "compensation_plan_data>Allowance_Plan_Data>Use_Assigned_Value_in_100_Percent_Calculation",

    # Eligibility Rule Reference (array)
    "eligibility_rule_reference_id": "compensation_plan_data>Eligibility_Rule_Reference",

    # Audit
    "modified_on": "modified_on",
}

compensation_plan_tbl = "lh_hr_bronze.workday_hr.compensation_plan"

compensation_plan = workday_cs_generate_data(
    table_name=compensation_plan_tbl,
    xpath=compensation_plan_xpath,
    primary_key="compensation_plan_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

job_family_xpath = {
    # Top-level fields
    "inactive": "job_family_data>Inactive",
    "id": "job_family_data>ID",
    "include_in_display_name": "job_family_data>Include_in_Display_Name",
    "name": "job_family_data>Name",
    # "integration_field_override_data": "job_family_data>Integration_Field_Override_Data",
    "effective_date": "job_family_data>Effective_Date",
    "summary": "job_family_data>Summary",

    # Job Profile Data (array)
    "job_profile_data": "job_family_data>Job_Profile_Data",
    "job_profile_reference_id": "job_family_data>Job_Profile_Data>Job_Profile_Reference",

    # Audit
    "modified_on": "modified_on",
}

job_family_tbl = "lh_hr_bronze.workday_hr.job_family"

job_family = workday_cs_generate_data(
    table_name=job_family_tbl,
    xpath=job_family_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

job_profile_xpath = {
    # Top-level
    "job_profile_reference": "job_profile_data>Job_Profile_Reference",

    # Request data (container)
    "job_profile_request_data": "job_profile_data>Job_Profile_Request_Data",

    # Exempt data
    "job_profile_exempt_data": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Exempt_Data",
    "job_exempt": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Exempt_Data>Job_Exempt",
    "location_context_reference_id": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Exempt_Data>Location_Context_Reference",

    # Integration / unions
    "integration_field_override_data": "job_profile_data>Job_Profile_Request_Data>Integration_Field_Override_Data",
    "unions_data": "job_profile_data>Job_Profile_Request_Data>Unions_Data",

    # Job classification
    "job_classification_data": "job_profile_data>Job_Profile_Request_Data>Job_Classification_Data",
    "job_classifications_reference_id": "job_profile_data>Job_Profile_Request_Data>Job_Classification_Data>Job_Classifications_Reference",

    # Effective date
    "effective_date": "job_profile_data>Job_Profile_Request_Data>Effective_Date",

    # Basic data (array)
    "job_profile_basic_data": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data",
    "inactive": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Inactive",
    "restrict_to_organization_reference": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Restrict_to_Organization_Reference",
    "company_insider_data": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Company_Insider_Data",
    "job_title": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Job_Title",
    "restrict_to_country_reference": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Restrict_to_Country_Reference",
    "public_job": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Public_Job",
    "critical_job": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Critical_Job",
    "work_shift_required": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Work_Shift_Required",

    # Basic data – Job family
    "job_family_data": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Job_Family_Data",
    "job_family_reference_id": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Job_Family_Data>Job_Family_Reference",

    # Basic data – misc
    "include_job_code_in_name": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Include_Job_Code_in_Name",
    "management_level_reference_id": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Basic_Data>Management_Level_Reference",

    # Pay rate data
    
    "country_reference_id": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Pay_Rate_Data>Country_Reference",
    "pay_rate_type_reference_id": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Pay_Rate_Data>Pay_Rate_Type_Reference",

    # Job code
    "job_code": "job_profile_data>Job_Profile_Request_Data>Job_Code",

    # Work-study
    "work_study_data": "job_profile_data>Job_Profile_Request_Data>Work-Study_Data",

    # Workers’ comp
    "workers_compensation_code_replacement_data": "job_profile_data>Job_Profile_Request_Data>Workers_Compensation_Code_Replacement_Data",
    "workers_compensation_code_reference_id": "job_profile_data>Job_Profile_Request_Data>Workers_Compensation_Code_Replacement_Data>Workers_Compensation_Code_Reference",

    # Compensation data
    "job_profile_compensation_data": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Compensation_Data",
    "compensation_grade_profile_reference": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Compensation_Data>Compensation_Grade_Profile_Reference",
    "compensation_grade_reference_id": "job_profile_data>Job_Profile_Request_Data>Job_Profile_Compensation_Data>Compensation_Grade_Reference",

    # Audit
    "modified_on": "modified_on",
}

job_profile_tbl = "lh_hr_bronze.workday_hr.job_profile"

job_profile = workday_cs_generate_data(
    table_name=job_profile_tbl,
    xpath=job_profile_xpath,
    primary_key="job_code",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


organization_xpath = {
    # Top-level fields
    "inactive": "organization_data>Inactive",
    "include_manager_in_name": "organization_data>Include_Manager_in_Name",
    "name": "organization_data>Name",
    "integration_field_override_data": "organization_data>Integration_Field_Override_Data",
    "organization_code": "organization_data>Organization_Code",
    "leadership_reference": "organization_data>Leadership_Reference",

    # Organization subtype
    "organization_subtype_reference_id": "organization_data>Organization_Subtype_Reference",

    # Availability / visibility
    "availability_date": "organization_data>Availibility_Date",
    "organization_type_reference_id": "organization_data>Organization_Type_Reference",
    "include_organization_code_in_name": "organization_data>Include_Organization_Code_in_Name",
    "reference_id": "organization_data>Reference_ID",

    # External IDs
    "external_ids_data": "organization_data>External_IDs_Data",
    

    # Audit/last updated
    "last_updated_datetime": "organization_data>Last_Updated_DateTime",
    "organization_visibility_reference_id": "organization_data>Organization_Visibility_Reference",

    # Hierarchy
    
    "included_in_organization_reference": "organization_data>Hierarchy_Data>Included_In_Organization_Reference",
    "included_organization_reference": "organization_data>Hierarchy_Data>Included_Organization_Reference",
    "subordinate_organization_reference": "organization_data>Hierarchy_Data>Subordinate_Organization_Reference",
    "top_level_organization_reference_id": "organization_data>Hierarchy_Data>Top-Level_Organization_Reference",

    # Config table audit
    "modified_on": "modified_on",
}

organization_tbl = "lh_hr_bronze.workday_hr.organization"

organization = workday_cs_generate_data(
    table_name=organization_tbl,
    xpath=organization_xpath,
    primary_key="reference_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

worker_xpath = {
    # Top-level IDs
    "user_id": "worker_data>User_ID",
    "worker_id": "worker_data>Worker_ID",

    # ------------------------------------------------------------------
    # Personal data – Names
    # ------------------------------------------------------------------
    # Legal name
    "legal_first_name": "worker_data>Personal_Data>Name_Data>Legal_Name_Data>Name_Detail_Data>First_Name",
    "legal_formatted_name": "worker_data>Personal_Data>Name_Data>Legal_Name_Data>Name_Detail_Data>Formatted_Name",
    "legal_reporting_name": "worker_data>Personal_Data>Name_Data>Legal_Name_Data>Name_Detail_Data>Reporting_Name",
    "legal_last_name": "worker_data>Personal_Data>Name_Data>Legal_Name_Data>Name_Detail_Data>Last_Name",
    "legal_middle_name": "worker_data>Personal_Data>Name_Data>Legal_Name_Data>Name_Detail_Data>Middle_Name",
    "legal_country_reference_id": "worker_data>Personal_Data>Name_Data>Legal_Name_Data>Name_Detail_Data>Country_Reference",

    # Preferred name
    "preferred_first_name": "worker_data>Personal_Data>Name_Data>Preferred_Name_Data>Name_Detail_Data>First_Name",
    "preferred_formatted_name": "worker_data>Personal_Data>Name_Data>Preferred_Name_Data>Name_Detail_Data>Formatted_Name",
    "preferred_reporting_name": "worker_data>Personal_Data>Name_Data>Preferred_Name_Data>Name_Detail_Data>Reporting_Name",
    "preferred_last_name": "worker_data>Personal_Data>Name_Data>Preferred_Name_Data>Name_Detail_Data>Last_Name",
    "preferred_middle_name": "worker_data>Personal_Data>Name_Data>Preferred_Name_Data>Name_Detail_Data>Middle_Name",
    "preferred_country_reference_id": "worker_data>Personal_Data>Name_Data>Preferred_Name_Data>Name_Detail_Data>Country_Reference",

    # Tobacco use
    "tobacco_use": "worker_data>Personal_Data>Tobacco_Use",

    # ------------------------------------------------------------------
    # Personal Information (array)
    # ------------------------------------------------------------------
    "personal_information_data": "worker_data>Personal_Data>Personal_Information_Data",
    "birth_date": "worker_data>Personal_Data>Personal_Information_Data>Birth_Date",

    # Country-level personal info (fixed to match JSON structure)
    "country_personal_information_data":
        "worker_data>Personal_Data>Personal_Information_Data>Personal_Information_For_Country_Data>Country_Personal_Information_Data",
    "country_personal_information_country_reference_id":
        "worker_data>Personal_Data>Personal_Information_Data>Personal_Information_For_Country_Data>Country_Reference",

    # Ethnicity / gender inside Country_Personal_Information_Data
    "ethnicity_reference_id":
        "worker_data>Personal_Data>Personal_Information_Data>Personal_Information_For_Country_Data>Country_Personal_Information_Data>Ethnicity_Reference",
    "gender_reference_id":
        "worker_data>Personal_Data>Personal_Information_Data>Personal_Information_For_Country_Data>Country_Personal_Information_Data>Gender_Reference",

    # ------------------------------------------------------------------
    # Compensation
    # ------------------------------------------------------------------
    "compensation_data": "worker_data>Compensation_Data",
    "compensation_effective_date": "worker_data>Compensation_Data>Compensation_Effective_Date",
    "compensation_reason_reference_id": "worker_data>Compensation_Data>Reason_Reference",

    # Guidelines
    "compensation_guidelines_data": "worker_data>Compensation_Data>Compensation_Guidelines_Data",
    "compensation_grade_reference_id":
        "worker_data>Compensation_Data>Compensation_Guidelines_Data>Compensation_Grade_Reference",
    "compensation_package_reference_id":
        "worker_data>Compensation_Data>Compensation_Guidelines_Data>Compensation_Package_Reference",

    # Salary & hourly
    "salary_and_hourly_data": "worker_data>Compensation_Data>Salary_and_Hourly_Data",
    "salary_currency_reference_id":
        "worker_data>Compensation_Data>Salary_and_Hourly_Data>Currency_Reference",
    "salary_frequency_reference_id":
        "worker_data>Compensation_Data>Salary_and_Hourly_Data>Frequency_Reference",
    "salary_compensation_plan_reference_id":
        "worker_data>Compensation_Data>Salary_and_Hourly_Data>Compensation_Plan_Reference",
    "salary_compensation_element_reference_id":
        "worker_data>Compensation_Data>Salary_and_Hourly_Data>Compensation_Element_Reference",
    "salary_amount": "worker_data>Compensation_Data>Salary_and_Hourly_Data>Amount",
    "salary_assignment_effective_date":
        "worker_data>Compensation_Data>Salary_and_Hourly_Data>Assignment_Effective_Date",

    # Compensation summary – Annualized in reporting currency
    "summary_annualized_reporting_currency":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_in_Reporting_Currency_Summary_Data",
    "summary_annualized_reporting_currency_currency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_in_Reporting_Currency_Summary_Data>Currency_Reference",
    "summary_annualized_reporting_currency_frequency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_in_Reporting_Currency_Summary_Data>Frequency_Reference",
    "summary_annualized_reporting_currency_total_base_pay":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_in_Reporting_Currency_Summary_Data>Total_Base_Pay",
    "summary_annualized_reporting_currency_total_salary_and_allowances":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_in_Reporting_Currency_Summary_Data>Total_Salary_and_Allowances",

    # Summary – Hourly frequency
    "summary_hourly":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Hourly_Frequency",
    "summary_hourly_currency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Hourly_Frequency>Currency_Reference",
    "summary_hourly_frequency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Hourly_Frequency>Frequency_Reference",
    "summary_hourly_total_base_pay":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Hourly_Frequency>Total_Base_Pay",

    # Summary – Pay group frequency
    "summary_pay_group":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Pay_Group_Frequency",
    "summary_pay_group_currency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Pay_Group_Frequency>Currency_Reference",
    "summary_pay_group_frequency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Pay_Group_Frequency>Frequency_Reference",
    "summary_pay_group_total_base_pay":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Pay_Group_Frequency>Total_Base_Pay",
    "summary_pay_group_total_salary_and_allowances":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Summary_Data_in_Pay_Group_Frequency>Total_Salary_and_Allowances",

    # Summary – Annualized summary
    
    "summary_annualized_currency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_Summary_Data>Currency_Reference",
    "summary_annualized_frequency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_Summary_Data>Frequency_Reference",
    "summary_annualized_total_base_pay":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_Summary_Data>Total_Base_Pay",
    "summary_annualized_total_salary_and_allowances":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Annualized_Summary_Data>Total_Salary_and_Allowances",

    # Summary – Employee compensation summary (hourly)

    "summary_employee_compensation_currency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Employee_Compensation_Summary_Data>Currency_Reference",
    "summary_employee_compensation_frequency_reference_id":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Employee_Compensation_Summary_Data>Frequency_Reference",
    "summary_employee_compensation_total_base_pay":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Employee_Compensation_Summary_Data>Total_Base_Pay",
    "summary_employee_compensation_total_salary_and_allowances":
        "worker_data>Compensation_Data>Employee_Compensation_Summary_Data>Employee_Compensation_Summary_Data>Total_Salary_and_Allowances",

    # ------------------------------------------------------------------
    # Employment data – Worker job / position
    # ------------------------------------------------------------------
    
   

    # Position level
   
    "position_id": "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Position_ID",
    "position_reference_id": "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Position_Reference",
    "business_title": "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Business_Title",
    "position_title": "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Position_Title",
    "start_date": "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Start_Date",
    "position_time_type_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Position_Time_Type_Reference",
    "worker_type_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Worker_Type_Reference",
    "scheduled_weekly_hours":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Scheduled_Weekly_Hours",
    "default_weekly_hours":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Default_Weekly_Hours",
    "working_fte":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Working_FTE",
    "full_time_equivalent_percentage":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Full_Time_Equivalent_Percentage",
    "pay_rate_type_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Pay_Rate_Type_Reference",
    "federal_withholding_fein":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Federal_Withholding_FEIN",
    "job_exempt":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Job_Exempt",

    # Workers’ compensation

    "workers_compensation_code":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Worker_Compensation_Code_Data>Workers_Compensation_Code",
    "workers_compensation_code_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Worker_Compensation_Code_Data>Workers_Compensation_Code_Reference",

    # Business site
   
    "business_site_name":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Business_Site_Summary_Data>Name",
    "business_site_location_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Business_Site_Summary_Data>Location_Reference",

    # Job profile summary
    
    "job_profile_name":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Job_Profile_Summary_Data>Job_Profile_Name",
    "job_profile_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Job_Profile_Summary_Data>Job_Profile_Reference",
    "job_family_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Job_Profile_Summary_Data>Job_Family_Reference",
    "management_level_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Job_Profile_Summary_Data>Management_Level_Reference",

    # Job classification summary
    
    "job_classification_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Job_Classification_Summary_Data>Job_Classification_Reference",
    "job_group_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Job_Classification_Summary_Data>Job_Group_Reference",

    # Manager reference
    "manager_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Manager_as_of_last_detected_manager_change_Reference",

    # Payroll interface processing
    "payroll_interface_frequency_reference_id":
        "worker_data>Employment_Data>Worker_Job_Data>Position_Data>Payroll_Interface_Processing_Data>Frequency_Reference",

    # Primary job flag
    "primary_job": "worker_data>Employment_Data>Worker_Job_Data>Primary_Job",

    # ------------------------------------------------------------------
    # Worker status
    # ------------------------------------------------------------------
   
    "hire_date": "worker_data>Employment_Data>Worker_Status_Data>Hire_Date",
    "original_hire_date": "worker_data>Employment_Data>Worker_Status_Data>Original_Hire_Date",
    "continuous_service_date": "worker_data>Employment_Data>Worker_Status_Data>Continuous_Service_Date",
    "seniority_date": "worker_data>Employment_Data>Worker_Status_Data>Seniority_Date",
    "first_day_of_work": "worker_data>Employment_Data>Worker_Status_Data>First_Day_of_Work",
    "active_status_date": "worker_data>Employment_Data>Worker_Status_Data>Active_Status_Date",
    "active": "worker_data>Employment_Data>Worker_Status_Data>Active",
    "terminated": "worker_data>Employment_Data>Worker_Status_Data>Terminated",
    "not_returning": "worker_data>Employment_Data>Worker_Status_Data>Not_Returning",
    "retired": "worker_data>Employment_Data>Worker_Status_Data>Retired",
    "rehire": "worker_data>Employment_Data>Worker_Status_Data>Rehire",
    "hire_reason_reference_id":
        "worker_data>Employment_Data>Worker_Status_Data>Hire_Reason_Reference",

    # ------------------------------------------------------------------
    # Integration / misc
    # ------------------------------------------------------------------
    "integration_field_override_data": "worker_data>Integration_Field_Override_Data",
    "background_check_data": "worker_data>Background_Check_Data",

    # Audit
    "modified_on": "modified_on",
}

worker_tbl = "lh_hr_bronze.workday_hr.worker"

worker = workday_cs_generate_data(
    table_name=worker_tbl,
    xpath=worker_xpath,
    primary_key="worker_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

faculty_pay_fca_xpath = {
    "academic_period": "Academic_Period",
    "fringe": "Fringe",
    "academic_period_id": "academicPeriodId",
    "activity_code": "activityCode",
    "assigned_amount": "assignedAmount",
    "assigned_quantity": "assignedQuantity",
    "assigned_unit_rate": "assignedUnitRate",
    "cost_center_id": "costCenterId",
    "course": "course",
    "employee_id": "employeeId",
    "fund_id": "fundId",
    "instructor": "instructor",
    "period_activity_end_date": "periodActivityEndDate",
    "period_activity_start_date": "periodActivityStartDate",
    "position": "position",
    # Audit column for config table
    "modified_on": "modified_on",
}
faculty_pay_fca_tbl = "lh_hr_bronze.workday_hr.faculty_pay_fca"

faculty_pay_fca = workday_cs_generate_data(
    table_name=faculty_pay_fca_tbl,
    xpath=faculty_pay_fca_xpath,
    primary_key="employee_id,academic_period_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

faculty_pay_npa_xpath = {
    "fringe": "Fringe",
    "academic_period_id": "academicPeriodId",
    "activity_code": "activityCode",
    "assigned_amount": "assignedAmount",
    "assigned_quantity": "assignedQuantity",
    "assigned_unit_rate": "assignedUnitRate",
    "cost_center_id": "costCenterId",
    "employee_id": "employeeId",
    "fund_id": "fundId",
    "instructor": "instructor",
    "period_activity_end_date": "periodActivityEndDate",
    "period_activity_start_date": "periodActivityStartDate",
    "position": "position",
    # Audit column for config table
    "modified_on": "modified_on",
}

faculty_pay_npa_tbl = "lh_hr_bronze.workday_hr.faculty_pay_npa"

faculty_pay_npa = workday_cs_generate_data(
    table_name=faculty_pay_npa_tbl,
    xpath=faculty_pay_npa_xpath,
    primary_key="employee_id,academic_period_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    *academic_unit,
    *academic_appointee,
    *compensation_plan,
    *job_family,
    *job_profile,
    *organization,
    *worker,
    *faculty_pay_fca,
    *faculty_pay_npa     
]

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
    StructField("table_name", StringType(), True),
    StructField("alias_name", StringType(), True),
    StructField("xpath", StringType(), True),
    StructField("explode_and_pivot_required", StringType(), True),
    StructField("primary_keys", StringType(), True),
    StructField("partition_by", StringType(), True),

])

# ------------------------------------------------------------------
# 3. Create a single-row DataFrame
# ------------------------------------------------------------------

df_config = spark.createDataFrame(data, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_config = df_config.withColumns({
    "created_on":  current_timestamp(),
    "created_by":  lit(notebook_name),         # or your username / job name
    "modified_on": current_timestamp(),
    "modified_by": lit(notebook_name),
})

df_delete = df_config.select("source", "domain", "table_name").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if table_exists(config_abfss_path):
    # Staged data (new/changed records)
    staged_df = df_config

    # Reference the target Delta table by path
    delta_table = DeltaTable.forPath(spark, config_abfss_path)

    (
        delta_table.alias("tgt")
        .merge(
            df_config.alias("src"),
            "tgt.source = src.source AND tgt.domain = src.domain AND tgt.table_name = src.table_name"   # join condition (business key)
        )
        .whenMatchedDelete()
        .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_config.write.format("delta")\
        .mode("append")\
        .option("mergeSchema", True)\
        .save(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta")\
        .load(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
