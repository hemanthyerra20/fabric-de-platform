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
SERVICE_NAME = ""
OPERATION_NAME = ""
SOURCE_NAME = ""
VAR_HR_LIB_NAME = ""
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
SERVICE_NAME = "human_resources"
OPERATION_NAME = "get_workers"
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.worker"

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

# MAGIC %%sql
# MAGIC describe table `EAGLEI-HR-STORE-DEV`.lh_hr_bronze.workday_hr.worker

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM `EAGLEI-HR-STORE-DEV`.lh_hr_landing.workday_hr.worker LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)
    bronze_df = spark.read.format("delta").load(bronze_abfss_path)
    job_family_df =bronze_df.withColumn("job_family_reference_id", F.explode_outer(F.col("job_family_reference_id"))) \
    .withColumn("manager_reference_id", F.explode_outer(F.col("manager_reference_id"))) \
    .selectExpr("scheduled_weekly_hours",
                                        "default_weekly_hours",
                                        "working_fte",
                                        "full_time_equivalent_percentage",
                                        "pay_rate_type_reference_id",
                                        "federal_withholding_fein",
                                        "job_exempt",
                                        "workers_compensation_code",
                                        "workers_compensation_code_reference_id",
                                        "business_site_name",
                                        "business_site_location_reference_id",
                                        "job_profile_name",
                                        "job_profile_reference_id",
                                    
                                        "management_level_reference_id",
                                        "job_classification_reference_id",
                                        "job_group_reference_id",
                                       
                                        "payroll_interface_frequency_reference_id",
                                        "primary_job",
                                        "hire_date",
                                        "original_hire_date",
                                        "continuous_service_date",
                                        "seniority_date",
                                        "first_day_of_work",
                                        "active_status_date",
                                        "active",
                                        "terminated",
                                        "not_returning",
                                        "retired",
                                        "rehire",
                                        "hire_reason_reference_id",
                                        "integration_field_override_data",
                                        "background_check_data",
                                        "user_id",
                                        "worker_id",
                                        "legal_first_name",
                                        "legal_formatted_name",
                                        "legal_reporting_name",
                                        "legal_last_name",
                                        "legal_middle_name",
                                        "summary_annualized_reporting_currency_currency_reference_id",
                                        "summary_annualized_reporting_currency_frequency_reference_id",
                                        "summary_annualized_reporting_currency_total_base_pay",
                                        "summary_annualized_reporting_currency_total_salary_and_allowances",
                                        
                                        "summary_hourly_frequency_reference_id",
                                        "summary_hourly_total_base_pay",
                                       
                                        "summary_pay_group_currency_reference_id",
                                        "summary_pay_group_frequency_reference_id",
                                        "summary_pay_group_total_base_pay",
                                        "summary_pay_group_total_salary_and_allowances",
                                        "summary_annualized_currency_reference_id",
                                        "summary_annualized_frequency_reference_id",
                                        "summary_annualized_total_base_pay",
                                        "summary_annualized_total_salary_and_allowances",
                                        "summary_employee_compensation_currency_reference_id",
                                        "summary_employee_compensation_frequency_reference_id",
                                        "summary_employee_compensation_total_base_pay",
                                        "summary_employee_compensation_total_salary_and_allowances",
                                        "position_id",
                                        "position_reference_id",
                                        "business_title",
                                        "position_title",
                                        "start_date",
                                        "position_time_type_reference_id",
                                        "worker_type_reference_id",
                                        
                                        "preferred_first_name",
                                        "preferred_formatted_name",
                                        "preferred_reporting_name",
                                        "preferred_last_name",
                                        "preferred_middle_name",
                                        
                                        "tobacco_use",
                                        
                                        "birth_date",
                                        
                                        "country_personal_information_country_reference_id",
                                        
                                        "gender_reference_id",
                                        
                                        "compensation_reason_reference_id",
                                        
                                        "compensation_grade_reference_id",
                                        "compensation_package_reference_id",
                                        
                                       
                                        
                                        "salary_amount",
                                        "salary_assignment_effective_date",
                                        
                                        "modified_on",
                                        )

  
    explode_id_list = [
        
                    { "col_name": "pay_rate_type_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "workers_compensation_code_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "business_site_location_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "job_profile_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "management_level_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "job_classification_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "job_group_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "payroll_interface_frequency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}}, 
                    { "col_name": "hire_reason_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},
                    { "col_name": "summary_annualized_reporting_currency_currency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"Currency_ID":"anualized_currency_id"}},

                   


                    { "col_name": "summary_annualized_reporting_currency_frequency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"currency_numeric_code":"summaryreport_currency_numeric_code",
                    "Frequency_ID":"currency_Frequency_ID",
                    "Currency_ID":"anualized_summary_currency_id"}},

                     
                    { "col_name": "summary_pay_group_currency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"currency_numeric_code":"summary_currency_numeric_code"
                    ,"Currency_ID":"anual_summary_currency_id"}},

                    { "col_name": "summary_annualized_currency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"currency_numeric_code":"summarypay_currency_numeric_code",
                    "Currency_ID":"anaulized_currency_id"}},

                    { "col_name": "summary_annualized_frequency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"Frequency_ID":"Summary_Frequency_ID"}},
                                        
                    { "col_name": "summary_employee_compensation_currency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"currency_numeric_code":"summaryh_currency_numeric_code"}},

                    { "col_name": "summary_employee_compensation_frequency_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"currency_numeric_code":"sum_emp_currency_numeric_code",
                    "frequency_id":"summ_frequency_id"}},

                    { "col_name": "position_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"position_id":"ref_position_id"}},

                    { "col_name": "position_time_type_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"frequency_id":"position_frequency_id",
                    "position_id":"position_position_id"}},

                    { "col_name": "worker_type_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},

                    { "col_name": "country_personal_information_country_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},

                    # { "col_name": "ethnicity_reference_id",
                    # "col_not_req": ["WID"],
                    # "col_rename": {}},

                    { "col_name": "gender_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},

                    { "col_name": "compensation_reason_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {"currency_numeric_code":"sumhourly_currency_numeric_code",
                    "event_classification_subcategory_id":"comp_event_classification_subcategory_id",
                    "general_event_subcategory_id":"comp_general_event_subcategory_id"}},

                    { "col_name": "compensation_grade_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},

                    { "col_name": "compensation_package_reference_id",
                    "col_not_req": ["WID"],
                    "col_rename": {}},                
                ] 


    bronze_df = explode_ids(job_family_df, explode_id_list)
    # display(bronze_df)
    # column_list = list(bronze_df.columns)
    # print(column_list)


    bronze_df=bronze_df\
                        .select(
                            F.sha2(F.coalesce(F.trim(F.col("position_id").cast("string")), F.lit('')), 256).alias("position_key"),
                            

                        F.col("scheduled_weekly_hours"),
                        F.col("default_weekly_hours"),
                        F.col("working_fte"),
                        F.col("full_time_equivalent_percentage"),
                        F.col("federal_withholding_fein"),
                        F.col("job_exempt"),
                        F.col("workers_compensation_code"),
                        F.col("business_site_name"),
                        F.col("job_profile_name"),
                        F.col("primary_job"),
                        F.col("hire_date"),
                        F.col("original_hire_date"),
                        F.col("continuous_service_date"),
                        F.col("seniority_date"),
                        F.col("first_day_of_work"),
                        F.col("active_status_date"),
                        F.col("active"),
                        F.col("terminated"),
                        F.col("not_returning"),
                        F.col("retired"),
                        F.col("rehire"),
                        F.col("integration_field_override_data"),
                        F.col("background_check_data"),
                        F.col("user_id"),
                        F.col("worker_id"),
                        F.col("legal_first_name"),
                        F.col("legal_formatted_name"),
                        F.col("legal_reporting_name"),
                        F.col("legal_last_name"),
                        F.col("legal_middle_name"),
                        F.col("summary_annualized_reporting_currency_total_base_pay"),
                        F.col("summary_annualized_reporting_currency_total_salary_and_allowances"),
                        F.col("summary_hourly_frequency_reference_id"),
                        F.col("summary_hourly_total_base_pay"),
                        F.col("summary_pay_group_frequency_reference_id"),
                        F.col("summary_pay_group_total_base_pay"),
                        F.col("summary_pay_group_total_salary_and_allowances"),
                        F.col("summary_annualized_total_base_pay"),
                        F.col("summary_annualized_total_salary_and_allowances"),
                        F.col("summary_employee_compensation_total_base_pay"),
                        F.col("summary_employee_compensation_total_salary_and_allowances"),
                        F.col("position_id"),
                        F.col("business_title"),
                        F.col("position_title"),
                        F.col("start_date"),
                        F.col("preferred_first_name"),
                        F.col("preferred_formatted_name"),
                        F.col("preferred_reporting_name"),
                        F.col("preferred_last_name"),
                        F.col("preferred_middle_name"),
                        F.col("tobacco_use"),
                        F.col("birth_date"),
                        F.col("salary_amount"),
                        F.col("salary_assignment_effective_date"),
                        F.col("modified_on"),
                        F.col("pay_rate_type_id"),
                        F.col("workers_compensation_id"),
                        F.col("location_id"),
                        F.col("job_profile_id"),
                        F.col("management_level_id"),
                        F.col("job_classification_reference_id"),
                        F.col("job_classification_group_id"),
                        F.col("frequency_id"),
                        F.col("event_classification_subcategory_id"),
                        F.col("general_event_subcategory_id"),
                        F.col("anualized_currency_id"),
                        F.col("currency_numeric_code"),
                        F.col("currency_Frequency_ID"),
                        F.col("anual_summary_currency_id"),
                        F.col("summary_currency_numeric_code"),
                        F.col("anaulized_currency_id"),
                        F.col("summarypay_currency_numeric_code"),
                        F.col("Summary_Frequency_ID"),
                        F.col("currency_id"),
                        F.col("summaryh_currency_numeric_code"),
                        F.col("summ_frequency_id"),
                        F.col("ref_position_id"),
                        F.col("position_time_type_id"),
                        F.col("contingent_worker_type_id"),
                        F.col("employee_type_id"),
                        F.col("iso_3166-1_alpha-2_code"),
                        F.col("iso_3166-1_alpha-3_code"),
                        F.col("iso_3166-1_numeric-3_code"),
                        F.col("opm_country_code"),
                        F.col("gender_code"),
                        F.col("change_job_subcategory_id"),
                        F.col("comp_event_classification_subcategory_id"),
                        F.col("comp_general_event_subcategory_id"),
                        F.col("compensation_grade_id"),
                        F.col("compensation_package_id"),
                       )            
    

    bronze_df = to_lowercase_columns(bronze_df)
    flat_bronze_df = bronze_df
    flat_bronze_df=to_lowercase_columns(flat_bronze_df)
    
    audit_df = add_audit_column(flat_bronze_df, PIPELINE_NAME)
    audit_df2=audit_df.distinct()

    final_count = audit_df2.count()
    rows_read = final_count
    rows_written = final_count

    distinct_row_count = audit_df2.distinct().count()
    distinct_key_count = (audit_df2.select("position_id").distinct().count())
    counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_isir_caa_count"])

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "worker")
    write_delta_tbl(source_df = audit_df2
                    ,target_table_path = silver_path
                    ,keys = ["position_id"])
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
