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

import uuid
import json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "xpath_details"
notebook_name = notebookutils.runtime.context["currentNotebookName"]


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def workday_cs_generate_data(table_name: str, xpath: dict, primary_key: str, explode_and_pivot_required: dict | None = None, anonymization: list | None = None, partition_by: str | None = None):
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
        anonymization (list | None): Optional list of alias names that should
            be flagged for anonymization.
        partition_by (str | None): Optional partitioning column(s) information.

    Returns:
        list[tuple]: List of configuration tuples ready to be turned into a
        DataFrame.
    """

    # Guard against None for optional parameters
    if explode_and_pivot_required is None:
        explode_and_pivot_required = {}
    if anonymization is None:
        anonymization = []

    # Generate a single UUID for this batch so all related rows share the same id
    batch_id = str(uuid.uuid4())
    source = "workday"
    domain = "cs"

    data_lst = []

    try:
        # Precompute a case-insensitive set of keys from explode_and_pivot_required
        explode_keys_lower = {k.lower() for k in explode_and_pivot_required.keys()}

        # Precompute a case-insensitive list from anonymization
        anonymization_lower = [x.lower() for x in anonymization]

        for alias_name, path in xpath.items():
            is_anonym = 0

            # Determine if this alias has explode/pivot metadata configured
            if alias_name.lower() in explode_keys_lower:
                # Use the original key lookup (case-sensitive) to fetch details
                explode_details = {
                    alias_name: explode_and_pivot_required[alias_name]
                }

                # Serialize explode/pivot configuration to a compact JSON string
                explode_details_str = json.dumps(explode_details, separators=(",", ":"))
            else:
                explode_details_str = None

            # Determine if this alias should be anonymized
            if alias_name.lower() in anonymization_lower:
                is_anonym = 1

            # Append a configuration tuple for this alias/xpath pair
            data_lst.append(
                (batch_id, source, domain, table_name, alias_name, path, explode_details_str, primary_key, partition_by, is_anonym)
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

student_course_tags_xpath = {
            "student_course_tag_reference": "Student_Course_Tag_Reference",
            "student_course_tag_category_reference": "Student_Course_Tag_Data>Student_Course_Tag_Category_Reference",
            "student_course_tag_name": "Student_Course_Tag_Data>Student_Course_Tag_Name",
            "id": "Student_Course_Tag_Data>ID",
            "academic_unit_reference" : "Student_Course_Tag_Data>Has_Institution_Academic_Unit_Data>Academic_Unit_Reference",
            "modified_on": "modified_on"
}

student_course_tags_tbl = "lh_cs_bronze.workday_cs.student_course_tags"

student_course_tags = workday_cs_generate_data(table_name=student_course_tags_tbl,
                                    xpath = student_course_tags_xpath,
                                    primary_key= "id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_course_tag_cat_xpath = {
            "student_course_tag_category_reference": "Student_Course_Tag_Category_Reference",
            "academic_unit_reference": "Student_Course_Tag_Category_Data>Has_Institution_Academic_Unit_Data>Academic_Unit_Reference",
            "id": "Student_Course_Tag_Category_Data>ID",
            "private_course_tag_category" : "Student_Course_Tag_Category_Data>Private_Course_Tag_Category",
            "student_course_tag_category_name" : "Student_Course_Tag_Category_Data>Student_Course_Tag_Category_Name",
            "modified_on": "modified_on"
}

student_course_tag_cat_tbl = "lh_cs_bronze.workday_cs.student_course_tag_categories"

student_course_tag_cat = workday_cs_generate_data(table_name=student_course_tag_cat_tbl,
                                    xpath = student_course_tag_cat_xpath,
                                    primary_key= "id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

course_sections_xpath = {
    "course_reference": "Course_Section_Data>Course_Reference",
    "projected_enrollment": "Course_Section_Data>Course_Section_Data_Data>Projected_Enrollment",
    "manually_manage_waitlist_promotion": "Course_Section_Data>Course_Section_Data_Data>Manually_Manage_Waitlist_Promotion",
    "has_reserved_capacity": "Course_Section_Data>Course_Section_Data_Data>Has_Reserved_Capacity",
    "graded_anonymously": "Course_Section_Data>Course_Section_Data_Data>Graded_Anonymously",
    "learning_outcome_reference": "Course_Section_Data>Course_Section_Data_Data>Learning_Outcome_Reference",
    "delivery_mode_reference": "Course_Section_Data>Course_Section_Data_Data>Delivery_Mode_Reference",
    "course_section_combination_instructional_format_contact_hours_data": "Course_Section_Data>Course_Section_Data_Data>Course_Section_Combination_Instructional_Format_Contact_Hours_Data",
    "title": "Course_Section_Data>Course_Section_Data_Data>Title",
    "campus_location_reference": "Course_Section_Data>Course_Section_Data_Data>Campus_Location_Reference",
    "instructional_format_reference": "Course_Section_Data>Course_Section_Data_Data>Instructional_Format_Reference",
    "partial_unit_increment": "Course_Section_Data>Course_Section_Data_Data>Partial_Unit_Increment",
    "end_date": "Course_Section_Data>Course_Section_Data_Data>End_Date",
    "educational_taxonomy_code_reference": "Course_Section_Data>Course_Section_Data_Data>Educational_Taxonomy_Code_Reference",
    "published_instructor_roles_reference": "Course_Section_Data>Course_Section_Data_Data>Published_Instructor_Roles_Reference",
    "course_section_academic_unit_data": "Course_Section_Data>Course_Section_Data_Data>Course_Section_Academic_Unit_Data",
    "description": "Course_Section_Data>Course_Section_Data_Data>Description",
    "abbreviated_title": "Course_Section_Data>Course_Section_Data_Data>Abbreviated_Title",
    "override_meeting_pattern": "Course_Section_Data>Course_Section_Data_Data>Override_Meeting_Pattern",
    "other_credit_values": "Course_Section_Data>Course_Section_Data_Data>Other_Credit_Values",
    "section_component_data": "Course_Section_Data>Course_Section_Data_Data>Section_Component_Data",
    "co_requisite_courses_reference": "Course_Section_Data>Course_Section_Data_Data>Co_Requisite_Courses_Reference",
    "academic_period_reference": "Course_Section_Data>Course_Section_Data_Data>Academic_Period_Reference",
    "course_material_section_assignment_data": "Course_Section_Data>Course_Section_Data_Data>Course_Material_Section_Assignment_Data",
    "unit_type_reference": "Course_Section_Data>Course_Section_Data_Data>Unit_Type_Reference",
    "unlimited_capacity": "Course_Section_Data>Course_Section_Data_Data>Unlimited_Capacity",
    "grading_basis_reference": "Course_Section_Data>Course_Section_Data_Data>Grading_Basis_Reference",
    "minimum_units": "Course_Section_Data>Course_Section_Data_Data>Minimum_Units",
    "contact_hours": "Course_Section_Data>Course_Section_Data_Data>Contact_Hours",
    "reserved_capacity_line_data": "Course_Section_Data>Course_Section_Data_Data>Reserved_Capacity_Line_Data",
    "maximum_units": "Course_Section_Data>Course_Section_Data_Data>Maximum_Units",
    "section_listing_data": "Course_Section_Data>Course_Section_Data_Data>Section_Listing_Data",
    "other_credit_type_value_data": "Course_Section_Data>Course_Section_Data_Data>Other_Credit_Type_Value_Data",
    "competency_reference": "Course_Section_Data>Course_Section_Data_Data>Competency_Reference",
    "wait_list_capacity": "Course_Section_Data>Course_Section_Data_Data>Wait_List_Capacity",
    "course_tag_reference": "Course_Section_Data>Course_Section_Data_Data>Course_Tag_Reference",
    "start_date": "Course_Section_Data>Course_Section_Data_Data>Start_Date",
    "not_visible": "Course_Section_Data>Course_Section_Data_Data>Not_Visible",
    "instructor_role_assignment_data": "Course_Section_Data>Course_Section_Data_Data>Instructor_Role_Assignment_Data",
    "section_capacity": "Course_Section_Data>Course_Section_Data_Data>Section_Capacity",
    "id": "Course_Section_Data>ID",
    "section_status_reference": "Course_Section_Data>Section_Status_Reference",
    "course_section_reference": "Course_Section_Reference",
    "modified_on": "modified_on"
}

course_sections_tbl = "lh_cs_bronze.workday_cs.course_sections"

course_sections = workday_cs_generate_data(
    table_name=course_sections_tbl,
    xpath=course_sections_xpath,
    primary_key="id"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_schedule_for_reg_xpath = {
    "id": "Student_Schedule_for_Registration_Data>ID",
    "schedule_name": "Student_Schedule_for_Registration_Data>Schedule_Name",
    "academic_period_reference": "Student_Schedule_for_Registration_Data>Academic_Period_Reference",
    "student_schedule_for_registration_item": "Student_Schedule_for_Registration_Data>Student_Schedule_for_Registration_Item",
    "academic_record_reference": "Student_Schedule_for_Registration_Data>Academic_Record_Reference",
    "modified_on": "modified_on"
}

student_schedule_for_reg_tbl = "lh_cs_bronze.workday_cs.student_schedules_for_registration"

student_schedule_for_reg = workday_cs_generate_data(
    table_name= student_schedule_for_reg_tbl,
    xpath= student_schedule_for_reg_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

std_location_response_xpath = {
            "student_location_assignment_snapshot_data": "Student_Location_Assignment_Data>Student_Location_Assignment_Snapshot_Data",
            "academic_record_reference": "Academic_Record_Reference",
            "modified_on": "modified_on"

}

std_location_response_ids = {
        "academic_record_reference": {
                "col_not_req": ["wid"],
                "col_rename": {}
        }
    }

std_location_response_tbl = "lh_cs_bronze.workday_cs.student_location_assignment_snapshots"

std_location_response = workday_cs_generate_data(
    table_name= std_location_response_tbl,
    xpath= std_location_response_xpath,
    primary_key="student_record_id",
    explode_and_pivot_required= std_location_response_ids,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

students_xpath = {
        "id": "Student_Data>ID",
        "student_wid": "Student_Reference>WID",
        "account_user_name" : "Student_Data>Account_User_Name",
        "universal_id" : "Student_Data>Universal_ID",
        "academic_record_for_student_data": "Student_Data>Financial_Aid_Record_for_Student_Data>Academic_Record_for_Student_Data",
        "financial_aid_record_secured_reference": "Student_Data>Financial_Aid_Record_for_Student_Data>Financial_Aid_Record_Secured_Reference",
        "additional_name_data" : "Student_Data>Student_Person_Data>Person_Name_Data>Additional_Name_Data",
        "legal_country_reference" : "Student_Data>Student_Person_Data>Person_Name_Data>Legal_Name_Data>Name_Detail_Data>Country_Reference",
        "legal_first_name" : "Student_Data>Student_Person_Data>Person_Name_Data>Legal_Name_Data>Name_Detail_Data>First_Name",
        "legal_last_name" : "Student_Data>Student_Person_Data>Person_Name_Data>Legal_Name_Data>Name_Detail_Data>Last_Name",
        "legal_middle_name" : "Student_Data>Student_Person_Data>Person_Name_Data>Legal_Name_Data>Name_Detail_Data>Middle_Name",
        "preferred_country_reference" : "Student_Data>Student_Person_Data>Person_Name_Data>Preferred_Name_Data>Name_Detail_Data>Country_Reference",
        "preferred_first_name" : "Student_Data>Student_Person_Data>Person_Name_Data>Preferred_Name_Data>Name_Detail_Data>First_Name",
        "preferred_last_name" : "Student_Data>Student_Person_Data>Person_Name_Data>Preferred_Name_Data>Name_Detail_Data>Last_Name",
        "preferred_middle_name" : "Student_Data>Student_Person_Data>Person_Name_Data>Preferred_Name_Data>Name_Detail_Data>Middle_Name",
        "date_of_birth" : "Student_Data>Student_Person_Data>Personal_Profile_Data>Date_of_Birth",
        "ethnicity_reference" : "Student_Data>Student_Person_Data>Personal_Profile_Data>Ethnicity_Reference",
        "gender_reference" : "Student_Data>Student_Person_Data>Personal_Profile_Data>Gender_Reference",
        "hispanic_or_latino" : "Student_Data>Student_Person_Data>Personal_Profile_Data>Hispanic_or_Latino",
        "student_contact_information" : "Student_Data>Student_Person_Data>Student_Contact_Information_Data",
        "do_not_purge_settings_data" : "Student_Data>Do_Not_Purge_Settings_Data",
        "student_id": "Student_Data>Student_ID",
        "other_student_information_data": "Student_Data>Other_Student_Information_Data",
        "modified_on": "modified_on",
        "student_additional_data":"student_additional_data"}

students_tbl = "lh_cs_bronze.workday_cs.students"

anonymization_lst = ["legal_first_name", "legal_last_name", "legal_middle_name", 
                    "preferred_first_name", "preferred_last_name", "preferred_middle_name"]

students = workday_cs_generate_data(
    table_name= students_tbl,
    xpath= students_xpath,
    primary_key="student_id",
    anonymization = anonymization_lst
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

country_reference_xpath = {
            "referenced_object_descriptor": "Reference_ID_Data>Referenced_Object_Descriptor",
            "iso_3166_1_alpha_2_Code": "Reference_ID_Reference>ISO_3166-1_Alpha-2_Code",
            "iso_3166_1_alpha_3_Code": "Reference_ID_Reference>ISO_3166-1_Alpha-3_Code",
            "iso_3166_1_numeric_3_Code": "Reference_ID_Reference>ISO_3166-1_Numeric-3_Code",
            "opm_country_code": "Reference_ID_Reference>OPM_Country_Code",
            "modified_on": "modified_on"

}

country_reference_tbl = "lh_cs_bronze.workday_cs.country_references"

country_references = workday_cs_generate_data(
    table_name= country_reference_tbl,
    xpath= country_reference_xpath,
    primary_key="iso_3166_1_alpha_2_Code",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

country_region_reference_xpath = {
            "referenced_object_descriptor": "Reference_ID_Data>Referenced_Object_Descriptor",
            "iso_3166_2_code": "Reference_ID_Reference>ISO_3166-2_Code",
            "iso_3166_1_numeric_3_code": "Reference_ID_Reference>ISO_3166-2_Country-Region_Code",
            "opm_country_subdivision_code": "Reference_ID_Reference>OPM_Country_Subdivision_Code",
            "country_region_id": "Reference_ID_Reference>Country_Region_ID",
            "modified_on": "modified_on"

}

country_region_reference_tbl = "lh_cs_bronze.workday_cs.country_region_references"

country_region_references = workday_cs_generate_data(
    table_name= country_region_reference_tbl,
    xpath= country_region_reference_xpath,
    primary_key="country_region_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ethnicity_reference_xpath = {
            "ethnicity_id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

ethnicity_reference_tbl = "lh_cs_bronze.workday_cs.ethnicity_references"

ethnicity_references = workday_cs_generate_data(
    table_name= ethnicity_reference_tbl,
    xpath= ethnicity_reference_xpath,
    primary_key="ethnicity_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_veteran_xpath = {
            "student_veteran_benefits_reference": "Student_Veteran_Benefits_Reference",
            "student_veteran_benefits_data": "Student_Veteran_Benefits_Data",
            "modified_on": "modified_on"
}

student_veteran_ids = {
        "student_veteran_benefits_reference": {
                "col_not_req": [],
                "col_rename": {}
        }
    }

student_veteran_tbl = "lh_cs_bronze.workday_cs.student_veterans_benefits"

student_veteran = workday_cs_generate_data(
    table_name= student_veteran_tbl,
    xpath= student_veteran_xpath,
    primary_key= "wid,student_veteran_benefits_id",
    explode_and_pivot_required= student_veteran_ids,
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

historical_academic_records_xpath= {"historical_academic_record_reference":"Historical_Academic_Record_Reference",
                                    "historical_academic_record_Data":"Historical_Academic_Record_Data",
                                    "modified_on": "modified_on"
                                                    }

student_historical_ids = {
        
        "historical_academic_record_reference": {
                "col_not_req": [],
                "col_rename": {
            
                }
        }
    }
student_historical = "lh_cs_bronze.workday_cs.historical_academic_records"

historical_academic_records = workday_cs_generate_data(
    table_name= student_historical,
    xpath= historical_academic_records_xpath,
    primary_key= "historical_student_record_id",
    explode_and_pivot_required= student_historical_ids,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

program_of_study_assignment_snapshots_xpath = {
                                "program_of_study_assignment_snapshot_data":"Program_of_Study_Assignment_Snapshot_Data",
                                "academic_record_reference":"Academic_Record_Reference",
                                "student_reference":"Student_Reference",
                                "program_of_study_Conferral_Data":"Program_of_Study_Conferral_Data",
                                 "modified_on": "modified_on"
                        }

study_assignment_ids = {
        
        "student_reference": {
                "col_not_req": [],
                "col_rename": {
                   "wid": "student_wid",
                   "student_academic_person_id":"student_academic_person_id",
                   "student_id":"student_id",
                   "universal_identifier_id":"universal_identifier_id"
                }
        },
        "academic_record_reference": {
                "col_not_req": [],
                "col_rename": {}
        }
    }
student_snpshot = "lh_cs_bronze.workday_cs.program_of_study_assignment_snapshots"

program_of_study_assignment_snapshots = workday_cs_generate_data(
    table_name= student_snpshot,
    xpath= program_of_study_assignment_snapshots_xpath,
    primary_key= "student_wid,student_id",
    explode_and_pivot_required= study_assignment_ids,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_residencies_xpath ={"student_residency_id":"Student_Residency_Data>ID",
                           "institutional_academic_unit_reference":"Student_Residency_Data>Institutional_Academic_Unit_Reference",
                           "effective_date":"Student_Residency_Data>Effective_Date",
                           "residency_reason_reference":"Student_Residency_Data>Residency_Reason_Reference",
                           "student_residency_status_reference":"Student_Residency_Data>Student_Residency_Status_Reference",
                           "student_and_universal_id_data":"Student_Residency_Data>Student_and_Universal_ID_Data",
                           "student_residency_student_prospect_stage_reference":"Student_Residency_Data>Student_Prospect_Stage_Reference",
                           "modified_on": "modified_on"
                        }


student_residencies_tbl = "lh_cs_bronze.workday_cs.student_residencies"


student_residencies = workday_cs_generate_data(
    table_name= student_residencies_tbl,
    xpath= student_residencies_xpath,
    primary_key= "student_residency_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_cohorts_xpath = {
            "student_cohorts_id": "Student_Cohort_Data>ID",
            "student_cohort_name": "Student_Cohort_Data>Student_Cohort_Name",
            "student_cohort_description": "Student_Cohort_Data>Student_Cohort_Description",
            "student_cohort_is_inactive":"Student_Cohort_Data>Student_Cohort_is_Inactive",
            "evaluate_security_by_cohort":"Student_Cohort_Data>Evaluate_Security_by_Cohort",
            "student_cohort_type_reference": "Student_Cohort_Data>Student_Cohort_Type_Reference",
            "academic_unit_reference": "Student_Cohort_Data>Academic_Unit_Data>Academic_Unit_Reference",
            "academic_level_reference": "Student_Cohort_Data>Academic_Unit_Data>Academic_Level_Reference",
            "student_cohort_reference": "Student_Cohort_Reference",
            "modified_on": "modified_on"
}

student_cohorts_ids = {
        "student_cohort_reference": {
                "col_not_req": [],
                "col_rename": { 
                    "wid" : "student_cohort_reference_wid"                   
                }
        }
    }

student_cohorts_tbl = "lh_cs_bronze.workday_cs.student_cohorts"

student_cohorts = workday_cs_generate_data(
    table_name=student_cohorts_tbl,
    xpath=student_cohorts_xpath,
    primary_key="student_cohorts_id,student_cohort_name",
    explode_and_pivot_required=student_cohorts_ids
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_cohort_memberships_xpath = {
            "student_cohort_membership_data_id": "Student_Cohort_Membership_Data>ID",
            "student_cohort_assignment_data": "Student_Cohort_Membership_Data>Student_Cohort_Assignment_Data",
            "student_cohort_reference_wid": "Student_Cohort_Reference>WID",
            "student_cohort_id": "Student_Cohort_Reference>Student_Cohort_ID",
            "modified_on": "modified_on"

}

student_cohort_memberships_tbl = "lh_cs_bronze.workday_cs.student_cohort_memberships"

student_cohort_memberships = workday_cs_generate_data(
    table_name=student_cohort_memberships_tbl,
    xpath=student_cohort_memberships_xpath,
    primary_key="student_cohort_membership_data_id,Student_Cohort_ID",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

academic_period_date_controls_xpath = {
            "academic_level_reference": "Academic_Period_Date_Controls_Data>Academic_Period_Date_Control_Configuration_Data",
            "academic_period_id": "Academic_Period_Date_Controls_Data>Academic_Period_ID",
            "academic_period_reference" : "Academic_Period_Reference",
            "modified_on": "modified_on"
}

academic_period_date_controls_ids = {
        "academic_period_reference": {
                "col_not_req": [],
                "col_rename": {
                    "academic_period_id":"academic_period_ref_id"
                }
        }
    }

academic_period_date_controls_tbl = "lh_cs_bronze.workday_cs.academic_period_date_controls"

academic_period_date_controls = workday_cs_generate_data(
    table_name=academic_period_date_controls_tbl,
    xpath=academic_period_date_controls_xpath,
    primary_key="academic_period_id,academic_period_ref_id",
    explode_and_pivot_required=academic_period_date_controls_ids
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_application_admission_responses_xpath = {
            "student_application_admission_responses_id": "Student_Application_Admission_Response_Data>ID",
            "admission_response_reference": "Student_Application_Admission_Response_Data>Admission_Response_Reference",
            "admission_response_reasons_reference": "Student_Application_Admission_Response_Data>Admission_Response_Reasons_Reference",
            "student_application_admission_response_reference":"Student_Application_Admission_Response_Reference",
            "modified_on": "modified_on"
}

student_application_admission_responses_ids = {
        "student_application_admission_response_reference": {
                "col_not_req": [],
                "col_rename": {
                    "wid": "student_application_admission_response_reference_wid"
                }
        }
    }

student_application_admission_responses_tbl = "lh_cs_bronze.workday_cs.student_application_admission_responses"

student_application_admission = workday_cs_generate_data(
    table_name=student_application_admission_responses_tbl,
    xpath=student_application_admission_responses_xpath,
    primary_key="student_application_admission_responses_id,student_application_id",
    explode_and_pivot_required=student_application_admission_responses_ids
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_applications_xpath = {
        "student_application_reference_id" : "Student_Application_Reference>Student_Application_ID",
        "student_application_reference_wid": "Student_Application_Reference>WID",
        "student_application_id": "Student_Application_Data>ID",
        "student_application_active": "Student_Application_Data>Active",
        "student_prospect_stage_reference": "Student_Application_Data>Student_Prospect_Stage_Reference",
        "student_applicant_type_reference": "Student_Application_Data>Student_Applicant_Type_Reference",
        "application_date": "Student_Application_Data>Application_Date",
        "student_application_program_of_study_line_data_id" : "Student_Application_Data>Student_Application_Program_of_Study_Line_Data>ID",
        "program_of_study_reference" : "Student_Application_Data>Student_Application_Program_of_Study_Line_Data>Program_of_Study_Reference",
        "academic_period_data": "Student_Application_Data>Academic_Period_Data",
        "anticipated_load_status_reference": "Student_Application_Data>Anticipated_Load_Status_Reference",
        "location_reference": "Student_Application_Data>Location_Reference",
        "learning_mode_reference": "Student_Application_Data>Learning_Mode_Reference",
        "student_tags_reference": "Student_Application_Data>Student_Tags_Reference",
        "payment_amount": "Student_Application_Data>Payment_Amount",
        "student_document_data": "Student_Application_Data>Student_Document_Data",
        "student_application_source_reference": "Student_Application_Data>Student_Application_Source_Reference",
        "previous_college_attendance": "Student_Application_Data>Previous_College_Attendance",
        "first_generation_college_student": "Student_Application_Data>First_Generation_College_Student",
        "application_identifier": "Student_Application_Data>Application_Identifier",
        "attended_this_institution": "Student_Application_Data>Attended_this_Institution",
        "legacy": "Student_Application_Data>Legacy",
        "academic_disciplinary_history": "Student_Application_Data>Academic_Disciplinary_History",
        "homeschooled": "Student_Application_Data>Homeschooled",
        "graduation_or_equivalency_reference": "Student_Application_Data>Graduation_or_Equivalency_Reference",
        "student_application_details_secondary_education_data": "Student_Application_Data>Student_Application_Details_Secondary_Education_Data",
        "student_application_details_post_secondary_education_data": "Student_Application_Data>Student_Application_Details_Post_Secondary_Education_Data",
        "selective_service": "Student_Application_Data>Selective_Service",
        "student_prospect_data": "Student_Application_Data>Student_Prospect_Data",
        "questionnaire_response_data": "Student_Application_Data>Questionnaire_Response_Data",
        "criminal_history": "Student_Application_Data>Criminal_History",
        "application_fee_waived": "Student_Application_Data>Application_Fee_Waived",
        "modified_on": "modified_on"

}

student_applications_tbl = "lh_cs_bronze.workday_cs.student_applications"

student_applications = workday_cs_generate_data(
    table_name= student_applications_tbl,
    xpath= student_applications_xpath,
    primary_key="student_application_reference_id,student_application_reference_wid,student_application_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reporting_records_xpath = {
            "financial_aid_record_reference": "Financial_Aid_Record_Reference",
            "reporting_record_snapshot_data": "Reporting_Record_Data>Reporting_Record_Snapshot_Data",
            "modified_on": "modified_on"

}

reporting_records_ids = {
        "financial_aid_record_reference": {
                "col_not_req": [],
                "col_rename": {
                    "wid": "financial_aid_record_reference_wid"
                }
        }
    }

reporting_records_tbl = "lh_cs_bronze.workday_cs.reporting_records"

reporting_record = workday_cs_generate_data(
    table_name= reporting_records_tbl,
    xpath= reporting_records_xpath,
    primary_key="financial_aid_record_reference_wid, financial_aid_record_id",
    explode_and_pivot_required= reporting_records_ids,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

academic_period_reference_xpath = {
            "academic_period_id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "wid" : "Reference_ID_Reference>WID",
            "modified_on": "modified_on"

}

academic_period_tbl = "lh_cs_bronze.workday_cs.academic_period_references"

academic_period_references = workday_cs_generate_data(
    table_name= academic_period_tbl,
    xpath= academic_period_reference_xpath,
    primary_key="academic_period_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

delivery_mode_reference_xpath = {
            "delivery_mode_id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

delivery_mode_tbl = "lh_cs_bronze.workday_cs.delivery_mode_references"

delivery_mode_references = workday_cs_generate_data(
    table_name= delivery_mode_tbl,
    xpath= delivery_mode_reference_xpath,
    primary_key="delivery_mode_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

instr_format_reference_xpath = {
            "instructional_format_id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

instr_format_tbl = "lh_cs_bronze.workday_cs.instructional_format_references"

instr_format_references = workday_cs_generate_data(
    table_name= instr_format_tbl,
    xpath= instr_format_reference_xpath,
    primary_key="instructional_format_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

maintained_academic_periods_xpath = {
            "academic_period_reference": "Academic_Period_Reference",
            "maintained_academic_period_data": "Maintained_Academic_Period_Data",
            "modified_on": "modified_on"
}

maintained_academic_periods_ids = {
        "academic_period_reference": {
                "col_not_req": [],
                "col_rename": {}
        }
    }


maintained_academic_periods_tbl = "lh_cs_bronze.workday_cs.maintained_academic_periods"

maintained_academic_periods = workday_cs_generate_data(table_name= maintained_academic_periods_tbl,
                                    xpath = maintained_academic_periods_xpath,
                                    primary_key= "wid,academic_period_id",
                                    explode_and_pivot_required= maintained_academic_periods_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

programs_of_study_xpath = {
            "program_of_study_reference": "Program_of_Study_Reference",
            "id": "Program_of_Study_Data>ID",
            "include_type_in_name": "Program_of_Study_Data>Include_Type_in_Name",
            "program_of_study_type_reference": "Program_of_Study_Data>Program_of_Study_Type_Reference",
            "first_entry_date": "Program_of_Study_Data>First_Entry_Date",
            "program_of_study_data_snapshot_data": "Program_of_Study_Data>Program_of_Study_Data_Snapshot_Data",
            "modified_on": "modified_on"
}

programs_of_study_ids = {
        "program_of_study_reference": {
                "col_not_req": [],
                "col_rename": {}
        }
    }


programs_of_study_tbl = "lh_cs_bronze.workday_cs.programs_of_study"

programs_of_study = workday_cs_generate_data(table_name= programs_of_study_tbl,
                                    xpath = programs_of_study_xpath,
                                    primary_key= "wid,program_of_study_id",
                                    explode_and_pivot_required= programs_of_study_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_tags_xpath = {
            "student_tag_reference": "Student_Tag_Reference",
            "student_tag_data": "Student_Tag_Data",
            "modified_on": "modified_on"
}

student_tags_ids = {
        "student_tag_reference": {
                "col_not_req": [],
                "col_rename": {}
        }
    }


student_tags_tbl = "lh_cs_bronze.workday_cs.student_tags"

student_tags = workday_cs_generate_data(table_name= student_tags_tbl,
                                    xpath = student_tags_xpath,
                                    primary_key= "wid,student_tag_id",
                                    explode_and_pivot_required= student_tags_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_educational_institution_xpath = {
            "educational_institution_reference": "Educational_Institution_Reference",
            "student_educational_institution_data": "Student_Educational_Institution_Data",
            "modified_on": "modified_on"
}

student_educational_institution_ids = {
        "educational_institution_reference": {
                "col_not_req": [],
                "col_rename": {}
        }
    }


student_educational_institution_tbl = "lh_cs_bronze.workday_cs.student_educational_institution"

student_educational_institution = workday_cs_generate_data(table_name= student_educational_institution_tbl,
                                    xpath = student_educational_institution_xpath,
                                    primary_key= "wid,educational_institution_id,educational_institution_postsecondary_other_id",
                                    explode_and_pivot_required= student_educational_institution_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_course_registration_xpath = {
    "attempted_credits": "Attempted_Credits",
    "cf_lrv_course_section_id": "Cf_Lrv_Course_Section_ID",
    "cf_lrv_academic_period_into_student_course_section_record": "CF_LRV_Academic_Period_into_Student_Course_section_Record",
    "cf_lrv_student_academic_level": "CF_LRV_Student_Academic_Level",
    "cf_lrv_studnet_academic_record_status": "CF_LRV_Studnet_Academic_Record_Status",
    "cf_rlv_student_id": "CF_RLV_Student_ID",
    "course_listing": "Course_Listing",
    "course_section": "Course_Section",
    "course_section_definition": "Course_Section_Definition",
    "credits": "Credits",
    "drop_date": "Drop_Date",
    "earned_credits": "Earned_Credits",
    "gpa_credits": "GPA_Credits",
    "grade": "Grade",
    "grade_due_date": "Grade_Due_Date",
    "grade_points": "Grade_Points",
    "graded_date": "Graded_Date",
    "grading_basis": "Grading_Basis",
    "grading_scheme": "Grading_Scheme",
    "instructional_format": "Instructional_Format",
    "original_credits": "Original_Credits",
    "registered_date": "Registered_Date",
    "registration_status": "Registration_Status",
    "repeating": "Repeating",
    "student": "Student",
    "primary_program_of_study": "Primary_Program_of_Study_from_Student_Record_on_Student_Course_Registration_Record",
    "student_record": "Student_Record",
    "waitlist_date": "Waitlist_Date",
    "withdrawal_date": "Withdrawal_Date",
    "program_of_study" : "Program_of_Study",
    "modified_on": "modified_on"
}
 
student_course_registration_tbl = "lh_cs_bronze.workday_cs.student_course_registration"
 
student_course_registration = workday_cs_generate_data(
    table_name=student_course_registration_tbl,
    xpath=student_course_registration_xpath,
    primary_key="cf_rlv_student_id,cf_lrv_course_section_id"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_enrollment_xpath = {

    "future_discontinued_program_of_study": "Student_Program_of_Study_Record_group>Future_Discontinued_Programs_of_Study",
    "student_id": "Student_ID",
    "modified_on": "modified_on"
}
student_enrollment_tbl = "lh_cs_bronze.workday_cs.student_enrollment"
 
student_enrollment = workday_cs_generate_data(
    table_name=student_enrollment_tbl,
    xpath=student_enrollment_xpath,
    primary_key="student_id,future_discontinued_program_of_study",
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_educational_institution_xpath = {
            "educational_institution_reference": "Educational_Institution_Reference",
            "student_educational_institution_data": "Student_Educational_Institution_Data",
            "modified_on": "modified_on"
}

student_educational_institution_ids = {
        "educational_institution_reference": {
                "col_not_req": [],
                "col_rename": {}
        }
    }


student_educational_institution_tbl = "lh_cs_bronze.workday_cs.student_educational_institution"

student_educational_institution = workday_cs_generate_data(table_name= student_educational_institution_tbl,
                                    xpath = student_educational_institution_xpath,
                                    primary_key= "wid,educational_institution_id,educational_institution_postsecondary_other_id",
                                    explode_and_pivot_required= student_educational_institution_ids)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

course_tag_reference_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

course_tag_reference_tbl = "lh_cs_bronze.workday_cs.student_course_tag_references"

course_tag_references = workday_cs_generate_data(
    table_name= course_tag_reference_tbl,
    xpath= course_tag_reference_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

academic_unit_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

academic_unit_references_reference_tbl = "lh_cs_bronze.workday_cs.academic_unit_references"

academic_unit_references_references = workday_cs_generate_data(
    table_name= academic_unit_references_reference_tbl,
    xpath= academic_unit_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

country_subregion_internal_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

country_subregion_internal_references_tbl = "lh_cs_bronze.workday_cs.country_subregion_internal_references"

country_subregion_internal_references = workday_cs_generate_data(
    table_name= country_subregion_internal_references_tbl,
    xpath= country_subregion_internal_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

communication_usage_type_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

communication_usage_type_references_tbl = "lh_cs_bronze.workday_cs.communication_usage_type_references"

communication_usage_type_references = workday_cs_generate_data(
    table_name= communication_usage_type_references_tbl,
    xpath= communication_usage_type_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_record_status_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

student_record_status_references_tbl = "lh_cs_bronze.workday_cs.student_record_status_references"

student_record_status_references = workday_cs_generate_data(
    table_name= student_record_status_references_tbl,
    xpath= student_record_status_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_load_status_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

student_load_status_references_tbl = "lh_cs_bronze.workday_cs.student_load_status_references"

student_load_status_references = workday_cs_generate_data(
    table_name= student_load_status_references_tbl,
    xpath= student_load_status_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_grade_element_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

student_grade_element_references_tbl = "lh_cs_bronze.workday_cs.student_grade_element_references"

student_grade_element_references = workday_cs_generate_data(
    table_name= student_grade_element_references_tbl,
    xpath= student_grade_element_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

educational_taxonomy_code_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

educational_taxonomy_code_references_tbl = "lh_cs_bronze.workday_cs.educational_taxonomy_code_references"

educational_taxonomy_code_references = workday_cs_generate_data(
    table_name= educational_taxonomy_code_references_tbl,
    xpath= educational_taxonomy_code_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_grading_scheme_xpath = {
    "student_grading_scheme_reference": "Student_Grading_Scheme_Reference",
    "institution_reference": "Student_Grading_Scheme_Data>Institution_Reference",
    "effective_date": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Effective_Date",
    "order": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Order",
    "id": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>ID",
    "student_grade_element_reference": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Student_Grade_Element_Reference",
    "grade_points": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Grade_Points",
    "earn_credits": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Earn_Credits",
    "include_in_gpa": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Include_in_GPA",
    "repeated_for_grade_improvement": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Repeated_for_Grade_Improvement",
    "student_grade_category_type_reference": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Student_Grade_Category_Type_Reference",
    "grade_note_required": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Grade_Note_Required",
    "last_date_of_attendance_required": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Last_Date_of_Attendance_Required",
    "display_grade_notes_to_student": "Student_Grading_Scheme_Data>Student_Grading_Scheme_Snapshot_Data>Student_Grade_Data>Display_Grade_Notes_to_Student",
    "modified_on": "modified_on"
}
 
student_grading_scheme_tbl = "lh_cs_bronze.workday_cs.student_grading_schemes" 
student_grading_scheme = workday_cs_generate_data(
    table_name=student_grading_scheme_tbl,
    xpath=student_grading_scheme_xpath,
    primary_key="id"
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


educational_credential_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

educational_credential_references_tbl = "lh_cs_bronze.workday_cs.educational_credential_references"

educational_credential_references = workday_cs_generate_data(
    table_name= educational_credential_references_tbl,
    xpath= educational_credential_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

academic_level_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

academic_level_references_tbl = "lh_cs_bronze.workday_cs.academic_level_references"

academic_level_references = workday_cs_generate_data(
    table_name= academic_level_references_tbl,
    xpath= academic_level_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cip_code_references_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

cip_code_references_tbl = "lh_cs_bronze.workday_cs.cip_code_references"

cip_code_references = workday_cs_generate_data(
    table_name= cip_code_references_tbl,
    xpath= cip_code_references_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

isir_transactions_request_xpath = {
    "isir_caa_transaction_id": "ISIR_CAA_Transaction_Data>ID",
    "student_id": "ISIR_CAA_Transaction_Data>ISIR_Student_Data>ID",
    "financial_aid_award_year_reference": "ISIR_CAA_Transaction_Data>Financial_Aid_Award_Year_Reference",
    "academic_year_begin_date": "ISIR_CAA_Transaction_Data>NSLDS_Transaction_Data>NSLDS_Loan_Data>Academic_Year_Begin_Date"      , 
    "academic_year_end_date": "ISIR_CAA_Transaction_Data>NSLDS_Transaction_Data>NSLDS_Loan_Data>Academic_Year_End_Date"   ,    
    "pell_grant_eligibility_flag": "ISIR_CAA_Transaction_Data>Pell_Grant_Eligibility_Flag",
    "modified_on": "modified_on"
}

isir_transactions_request_tbl = "lh_cs_bronze.workday_cs.isir_transactions_request"
isir_transactions_request = workday_cs_generate_data(
    table_name=isir_transactions_request_tbl,
    xpath=isir_transactions_request_xpath,
    primary_key="isir_caa_transaction_id"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

organizations_request_xpath = {
            "location_hierarchy": "Request_References>Organization_Reference",
            "subordinate": "Organization_Data>Hierarchy_Data>Subordinate_Organization_Reference",
            "modified_on": "modified_on"      

}

organizations_references_tbl = "lh_cs_bronze.workday_cs.organizations"

organizations_references = workday_cs_generate_data(
    table_name= organizations_references_tbl,
    xpath= organizations_request_xpath,
    primary_key="location_hierarchy,subordinate",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

educational_taxonomy_schemes_xpath = {
            "educational_taxonomy_code_group": "Educational_Taxonomy_Code_Group",
            "name": "Name",
            "modified_on": "modified_on"      

}

educational_taxonomy_schemes_tbl = "lh_cs_bronze.workday_cs.educational_taxonomy_schemes"

educational_taxonomy_schemes = workday_cs_generate_data(
    table_name= educational_taxonomy_schemes_tbl,
    xpath= educational_taxonomy_schemes_xpath,
    primary_key="name",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

campus_location_xpath = {
            "id": "ID",
            "academic_period": "Academic_Period",
            "cf_esi_location_code_course_tag": "CF_ESI_Location_Code_Course_Tag",
            "campus_locations": "Campus_Locations",
            "course_definition": "Course_Definition",
            "course_tags": "Course_Tags",
            "locations": "Locations",
            "modified_on": "modified_on"
        
}

campus_location_schemes_tbl = "lh_cs_bronze.workday_cs.campuslocations"

campus_location = workday_cs_generate_data(
    table_name=campus_location_schemes_tbl,
    xpath=campus_location_xpath,
    primary_key="id",
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pos_all_xpath = {
            "academic_unit": "Academic_Unit",
            "academic_status": "Acadmic_status",
            "earned_credential": "Earned_Creden",
            "effective_date": "Eff_Date",
            "expected_completion_date": "Expd_compl_dt",
            "is_primary_program_of_study": "Is_Primary",
            "completion_date" : "PrgCompleDate",
            "program_type": "ProgramType",
            "program_of_study": "Program_of_Study",
            "status": "Status",
            "student_id": "Student_ID",
            "conferral_date": "Conferral_Date",
            "conferral_academic_period": "Conferral_Academic_Period",
            "modified_on": "modified_on"


}

pos_all_tbl = "lh_cs_bronze.workday_cs.program_of_study_all"

pos_all = workday_cs_generate_data(
    table_name= pos_all_tbl,
    xpath= pos_all_xpath,
    primary_key="student_id,program_of_study,effective_date,status",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prospect_xpath = {
            "academic_level": "Academic_Level",
            "primary_program_of_study": "Primary_Program_of_Study",
            "student_id": "Student_ID",
            "prospect": "Student_Prospect",
            "prospect_stage": "Student_Prospect_Stage",
            "created_moment": "Created_Moment",
            "std_created_on": "std_Created_ON",
            "modified_on": "modified_on"


}

prospect_tbl = "lh_cs_bronze.workday_cs.prospect"

prospect = workday_cs_generate_data(
    table_name= prospect_tbl,
    xpath= prospect_xpath,
    primary_key="academic_level,primary_program_of_study,prospect,prospect_stage,student_id,created_moment",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

transfer_credit_xpath = {
            "course": "course",
            "course_number": "course_number",
            "created_moment": "created_moment",
            "created_on_date": "created_on_date",
            "credit_type": "credit_type",
            "credits": "credits",
            "standard_academic_period": "standard_academic_period",
            "student_id": "student_id",
            "student_record": "student_record",
            "transfer_credit_registration" : "transfer_credit_registration",
            "external_articulated_registration_date": "External_Articulated_Registration_Date",
            "modified_on": "modified_on"
}

transfer_credit_tbl = "lh_cs_bronze.workday_cs.student_transfer_credit"

transfer_credit = workday_cs_generate_data(
    table_name= transfer_credit_tbl,
    xpath= transfer_credit_xpath,
    primary_key="course,student_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pell_eligible_xpath = {
            "pell_eligible": "Pell_Eligible",
            "student": "Student",
            "student_id": "Student_ID",
            "academic_period": "academicPeriod",
            "modified_on": "modified_on"
}

pell_eligible_tbl = "lh_cs_bronze.workday_cs.student_pellgrant"

pell_eligible = workday_cs_generate_data(
    table_name= pell_eligible_tbl,
    xpath= pell_eligible_xpath,
    primary_key="student_id,academic_period",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

std_app_decision_xpath = {
            "admission_decision": "admission_decision",
            "admission_response": "admission_response",
            "application_identifier": "application_identifier",
            "educ_institution": "educ_institution",
            "graduation_date": "graduation_date",
            "student_application": "student_application",
            "student_id": "student_id",
            "workdayid": "workdayid",
            "modified_on": "modified_on"
}

std_app_decision_tbl = "lh_cs_bronze.workday_cs.student_application_decision"

std_app_decision = workday_cs_generate_data(
    table_name= std_app_decision_tbl,
    xpath= std_app_decision_xpath,
    primary_key="workdayid",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tenant_application_type_xpath = {
            "id": "Reference_ID_Data>ID",
            "description": "Reference_ID_Data>Referenced_Object_Descriptor",
            "modified_on": "modified_on"

}

tenant_application_type_tbl = "lh_cs_bronze.workday_cs.tenanted_student_applicant_type_references"

tenant_application_type = workday_cs_generate_data(
    table_name= tenant_application_type_tbl,
    xpath= tenant_application_type_xpath,
    primary_key="id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_edu_credential_xpath={
    "educational_institution_attendances":"educational_institution_attendances",
    "educational_institution_attendances_group":"educational_institution_attendances_group",
    "active_student_applications":"active_student_applications",
    "cf_emi_eagle1_student_external_transcript___official_group":"cf_emi_eagle1_student_external_transcript___official_group",
    "active_student_applications_group":"active_student_applications_group",
    "student_external_transcript":"student_external_transcript",
    "student_id": "student_id",
    "modified_on": "modified_on"

}

student_edu_credential_tbl = "lh_cs_bronze.workday_cs.student_edu_credential"

student_edu_credential = workday_cs_generate_data(
    table_name= student_edu_credential_tbl,
    xpath= student_edu_credential_xpath,
    primary_key="student_id",
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_stage_progression_funnel_xpath = {
    "cf_emi_eagle1_history_items_type_stage_progression": "CF_EMI_Eagle1_History_items_type_Stage_progression",
    "primary_academic_level": "Primary_Academic_Level",
    "stage_progression_group": "Stage_progression_group",
    "student": "Student",
    "student_history_items": "Student_History_Items",
    "student_id": "Student_ID",
    "student_prospect_records": "Student_Prospect_Records",
    "student_prospect_records_group": "Student_Prospect_Records_group",
    "student_recruitment": "Student_Recruitment",
    "student_recruitment_group": "Student_Recruitment_group",
    "academic_records_group": "academicRecords_group",
    "modified_on": "modified_on"
}

student_stage_progression_funnel_tbl = (
    "lh_cs_bronze.workday_cs.student_stage_progress_funnel"
)

student_stage_progression_funnel = workday_cs_generate_data(
    table_name=student_stage_progression_funnel_tbl,
    xpath=student_stage_progression_funnel_xpath,
    primary_key="student_id",
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

student_stage_progress_xpath={
    
    "action": "action",
    "category": "category",
    "date": "date",
    "details": "details",
    "student": "student",
    "student_id": "student_id",
    "student_recruitment": "student_recruitment",
    "reference_id": "referenceId"

}

student_stage_progress_tbl = "lh_cs_bronze.workday_cs.student_stage_progress"

student_stage_progress = workday_cs_generate_data(
    table_name= student_stage_progress_tbl,
    xpath= student_stage_progress_xpath,
    primary_key="student_id",
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    *student_course_tags,
    *student_course_tag_cat,
    *course_sections,
    *student_schedule_for_reg,
    *std_location_response,
    *students,
    *country_references,
    *country_region_references,
    *student_veteran,
    *historical_academic_records,
    *program_of_study_assignment_snapshots,
    *student_residencies,
    *student_cohorts,
    *student_cohort_memberships,
    *academic_period_date_controls,
    *student_application_admission,
    *student_applications,
    *reporting_record,
    *ethnicity_references,
    *academic_period_references,
    *delivery_mode_references,
    *instr_format_references,
    *maintained_academic_periods,
    *programs_of_study,
    *student_tags,
    *student_educational_institution,
    *student_enrollment,
    *student_course_registration,
    *course_tag_references,
    *academic_unit_references_references,
    *country_subregion_internal_references,
    *communication_usage_type_references,
    *student_record_status_references,
    *student_load_status_references,
    *student_grade_element_references,
    *student_grading_scheme,
    *educational_taxonomy_code_references,
    *educational_credential_references,
    *academic_level_references,
    *cip_code_references,
    *isir_transactions_request,
    *pos_all,
    *prospect,
    *organizations_references,
    *educational_taxonomy_schemes,
    *campus_location,
    *transfer_credit,
    *pell_eligible,
    *std_app_decision,
    *tenant_application_type,
    *student_edu_credential,
    *student_stage_progress,
    *student_stage_progression_funnel
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
    StructField("is_anonymization", IntegerType(), True)

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

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.sql("SELECT * FROM `EAGLEI-CS-STORE-DEV`.lh_cs_landing.workday_cs.isir_transactions_request LIMIT 10")
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
