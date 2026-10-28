# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5c59c66e-6492-4023-804b-71322eda6c41",
# META       "default_lakehouse_name": "lh_hr_silver",
# META       "default_lakehouse_workspace_id": "fc434332-df9b-42bf-9f6d-2e6f2ab3fca9",
# META       "known_lakehouses": [
# META         {
# META           "id": "5c59c66e-6492-4023-804b-71322eda6c41"
# META         },
# META         {
# META           "id": "863239a1-544f-4d86-83b2-80ab40b759eb"
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
SERVICE_NAME = "human_resources"
OPERATION_NAME = "Get_Academic_Appointee"
SOURCE_NAME = ""
VAR_HR_LIB_NAME = "var_hr_process"
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
DOMAIN = "hr"
SERVICE_NAME = "Human_Resources"
OPERATION_NAME = "Get_Academic_Appointee"
SOURCE_NAME = "workday"
VAR_HR_LIB_NAME = "var_hr_process"
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
    config_workspace_id = get_variable(VAR_HR_LIB_NAME, "platform_monitoring_workspace_id")
    config_lakehouse_id = get_lakehouse_id(VAR_HR_LIB_NAME, AUDIT_LAKEHOUSE_NAME)
    config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, CONFIG_SCHEMA_NAME, CONFIG_TABLE_NAME)

    ## Fetching Config object values
    config_df = spark.read.format("delta").load(config_abfss_path)\
                .filter((F.col("source") == SOURCE_NAME) & (F.col("domain") == DOMAIN))


    bronze_schema_name = get_param_value(config_df,SOURCE_NAME,"bronze_schema_name")
    bronze_lh_name = get_param_value(config_df,SOURCE_NAME,"bronze_lakehouse_name")
    silver_schema_name = get_param_value(config_df,SOURCE_NAME,"silver_schema_name")
    silver_lh_name = get_param_value(config_df,SOURCE_NAME,"silver_lakehouse_name")
    parent_object = get_param_value(config_df,SOURCE_NAME, SERVICE_NAME.lower())
    bronze_table_name = parent_object[OPERATION_NAME.lower()]["bronze_table_name"]
    workspace_id = get_workspace_id(VAR_HR_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_HR_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_HR_LIB_NAME, silver_lh_name)

    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.academic_appointee"

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

    academic_appointe_df = bronze_df.withColumn("ethnicity_reference",F.explode_outer(F.col("ethnicity_reference")))\
    .selectExpr("legal_reporting_name",
                                                "legal_last_name",
                                                "legal_middle_name",
                                                "legal_country_id",
                                                "preferred_first_name",
                                                "preferred_formatted_name",
                                                "preferred_reporting_name",
                                                "preferred_last_name",
                                                "preferred_middle_name",
                                                "preferred_country_id",
                                                "roster_percent",
                                                "rank_id",
                                                "track_type_id",
                                                "academic_unit_id",
                                                "appointment_title",
                                                "academic_appointment_track_id",
                                                "appointment_identifier_id",
                                                "position_id",
                                                "constructed_title",
                                                "appointment_track_id",
                                                "appointment_track_inactive",
                                                "appointment_start_date",
                                                "photo_file",
                                                "photo_filename",
                                                "uses_tobacco",
                                                "date_of_birth",
                                                # "citizenship_reference",
                                                "ethnicity_reference",
                                                # "disability_information_data",
                                                "gender_id",
                                                # "military_information_data",
                                                # "visa_id",
                                                # "government_id",
                                                # "license_id",
                                                # "custom_id",
                                                "national_id_country_id",
                                                "national_id_type_id",
                                                "national_id_reference_id",
                                                "national_id_shared_reference_id",
                                                # "additional_name_data",
                                                "legal_first_name",
                                                "legal_formatted_name"
                                                
                                            )


    explode_id_list = [
            {
                            "col_name": "preferred_country_id",
                            "col_not_req": ["wid"],
                            "col_rename": {"ISO_3166-1_Alpha-2_Code":"person_alpha2_code",
                            "ISO_3166-1_Alpha-3_Code":"person_alph3_code",
                            "OPM_Country_Code":"opm_cntry_code",
                            "ISO_3166-1_Numeric-3_Code":"person_numeric2_code"}
            },
            {
                            "col_name": "rank_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "academic_unit_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "academic_appointment_track_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "appointment_identifier_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "position_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "gender_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "national_id_country_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "national_id_type_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "national_id_reference_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "national_id_shared_reference_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "track_type_id",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "legal_country_id",
                            "col_not_req": ["wid"],
                            "col_rename": {"ISO_3166-1_Alpha-2_Code":"legal_alpha2_code",
                            "ISO_3166-1_Alpha-3_Code":"legal_alph3_code",
                            "OPM_Country_Code":"legal_opm_cntry_code",
                            "ISO_3166-1_Numeric-3_Code":"legal_numeric2_code"}
            },
            {
                            "col_name": "ethnicity_reference",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },

            
        ]
    academic_ante_df = explode_ids(academic_appointe_df, explode_id_list)
    # display(academic_ante_df)

    bronze_df = to_lowercase_columns(academic_ante_df)
    # display(bronze_df)
    # column_list = list(bronze_df.columns)
    # print(column_list)

    flat_bronze_df = to_lowercase_columns(ac_bronze_df)
    audit_df = add_audit_column(flat_bronze_df, PIPELINE_NAME)
      

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_appointee")
    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["appointment_identifier_id"],
    )
   
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0,
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

# CELL ********************

df = spark.sql("SELECT * FROM lh_hr_bronze.workday_hr.academic_unit LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_hr_bronze.workday_hr.job_family LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_hr_bronze.workday_hr.job_profile LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_hr_bronze.workday_hr.worker LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
