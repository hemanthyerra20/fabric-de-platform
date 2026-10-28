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
# META         },
# META         {
# META           "id": "08846ef1-977d-4698-b06f-84fdbd834bc6"
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
OPERATION_NAME = "get_academic_units"
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.academic_unit"

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

    # Project required columns from the bronze table, including nested "academic_unit_snapshot_data" fields
    academic_unit_df = bronze_df.selectExpr(
        "id",
        "is_institution",
        "academic_organization_subtype_id",
        "academic_unit_snapshot_data.*",
        "snapshot_inactive",
        "snapshot_name",
        "snapshot_code",
        "snapshot_company_reference_id",
        "snapshot_effective_date",
        "snapshot_include_incumbent_leader_in_name",
        "snapshot_include_subtype_in_name",
        "snapshot_include_code_in_name",
        "modified_on"
    )

    # Configure IDs to explode/normalize
    explode_id_list = [
        {
            "col_name": "snapshot_company_reference_id",
            "col_not_req": ["WID"],
            "col_rename": {},
        },
        {
            "col_name": "academic_organization_subtype_id",
            "col_not_req": ["WID"],
            "col_rename": {},
        },
     ]

    # Apply custom explode/normalization helper
    bronze_df = explode_ids(academic_unit_df, explode_id_list)

    # display(bronze_df)
    # column_list = list(bronze_df.columns)
    # print(column_list)

    # Build silver path and write data
    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_units")
    audit_df = add_audit_column(bronze_df, PIPELINE_NAME)
  

    

    bronze_df=bronze_df\
                        .select(
                            F.sha2(F.coalesce(F.trim(F.col("id").cast("string")), F.lit('')), 256).alias("academic_unit_key"),
                            F.col("id"),
                            F.col("is_institution"),
                            F.col("Code"),
                           
                            F.col("Effective_Date"),
                            F.col("Inactive"),
                            F.col("Include_Code_in_Name"),
                            F.col("Include_Incumbent_Leader_in_Name"),
                            F.col("Include_Subtype_in_Name"),
                            F.col("Name"),
                            F.col("snapshot_inactive"),
                            F.col("snapshot_name"),
                            F.col("snapshot_code"),
                            F.col("snapshot_effective_date"),
                            F.col("snapshot_include_incumbent_leader_in_name"),
                            F.col("snapshot_include_subtype_in_name"),
                            F.col("snapshot_include_code_in_name"),
                            F.col("modified_on"),
                            F.col("company_reference_id"),
                            F.col("organization_reference_id"),
                            F.col("academic_unit_subtype_id"),
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
    distinct_key_count = (audit_df2.select("id").distinct().count())
    counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_isir_caa_count"])

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_units")
    write_delta_tbl(source_df = audit_df2
                    ,target_table_path = silver_path
                    ,keys = ["id"])
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
