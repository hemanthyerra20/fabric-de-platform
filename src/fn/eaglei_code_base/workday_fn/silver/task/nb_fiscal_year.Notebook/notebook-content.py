# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
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

PIPELINE_NAME = "testing"
DOMAIN = ""
SERVICE_NAME = "Financial_Management"
OPERATION_NAME = "Get_Fiscal_Years"
SOURCE_NAME = ""
VAR_FN_LIB_NAME = "var_fn_process"
AUDIT_LAKEHOUSE_NAME = ""
CONFIG_SCHEMA_NAME = ""
CONFIG_TABLE_NAME = ""
MASTER_RUN_ID = ""
RUN_ID = ""
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

# DOMAIN = "fn"
# SOURCE_NAME = "workday"
# AUDIT_LAKEHOUSE_NAME = "lh_platform_monitoring"
# CONFIG_SCHEMA_NAME = "config"
# CONFIG_TABLE_NAME = "config_param"
# ACTIVITY_NAME = "notebook"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    start_time = datetime.now()
    config_workspace_id = get_variable(VAR_FN_LIB_NAME, "platform_monitoring_workspace_id")
    config_lakehouse_id = get_lakehouse_id(VAR_FN_LIB_NAME, AUDIT_LAKEHOUSE_NAME)
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

    workspace_id = get_workspace_id(VAR_FN_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_FN_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_FN_LIB_NAME, silver_lh_name)


    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.fiscal_year"

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
    bronze_df = spark.read.format("delta").load(bronze_abfss_path)\
                    .selectExpr(
                        'fiscal_year_number as year',
                        'fiscal_schedule_reference',
                        'fiscal_year_name as name',
                        'fiscal_year_start_date AS year_start_date',
                        'fiscal_year_end_date AS year_end_date',
                        'fiscal_period_data',
                    )
    
    explode_df = bronze_df\
                    .withColumn("fiscal_period_data", F.explode(F.col("fiscal_period_data")))\
                    .select("*", "fiscal_period_data.*")\
                    .drop("fiscal_period_data")
    
    col_converted_df = to_lowercase_columns(explode_df)

    explode_id_list = [
        {
                        "col_name": "fiscal_schedule_reference",
                        "col_not_req": ["wid"],
                        "col_rename": {}
        },
        {
                        "col_name": "fiscal_posting_interval_reference",
                        "col_not_req": ["wid"],
                        "col_rename": {}
        }

    ]

    exploded_ids_df = explode_ids(col_converted_df, explode_id_list)

    selected_df = exploded_ids_df.select(
        F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("year").cast("string"), F.lit("")),
                    F.coalesce(F.col("fiscal_posting_interval_id").cast("string"), F.lit(""))
                ),
                256
            ).alias("fiscal_year_key"),
        F.col('year'),
        F.col('name'),
        F.col('year_start_date').cast('date'),
        F.col('year_end_date').cast('date'),
        F.col('fiscal_period_start_date').cast('date').alias('period_start_date'),
        F.col('fiscal_period_end_date').cast('date').alias('period_end_date'),
        F.col('fiscal_schedule_id').alias('schedule_id'),
        F.col('fiscal_posting_interval_id').alias('posting_interval_id')
    )

    audit_df = add_audit_column(selected_df, PIPELINE_NAME)

    audit_df.cache()

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "fiscal_year"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["fiscal_year_key"]
    )

    audit_df.unpersist()
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
