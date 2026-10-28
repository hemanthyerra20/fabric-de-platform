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
OPERATION_NAME = "Get_Ledger_Account_Summaries"
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

    bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)
    bronze_df = spark.read.format("delta").load(bronze_abfss_path)
    bronze_df.cache()


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
    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.ledger_account_summary"

    ledger_account_summary_df = bronze_df.selectExpr(
                                    "ledger_account_summary_id as id",
                                    "ledger_account_summary_name AS name",
                                    "account_set_reference",
                                    "hierarchy_level_reference",
                                    "effective_date"
                                )

    explode_id_list = [
            {
                            "col_name": "account_set_reference",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "hierarchy_level_reference",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            }
        ]

    summary_exploded_ids_df = explode_ids(ledger_account_summary_df, explode_id_list)
    summary_flat_bronze_df = to_lowercase_columns(summary_exploded_ids_df)

    summary_df = summary_flat_bronze_df.select(
            F.sha2(F.coalesce(F.trim(F.col("id").cast("string")), F.lit('')), 256).alias("ledger_account_summary_key"),
            'id',
            'name',
            'account_set_id',
            'hierarchy_level_id',
            'effective_date',
        )
    audit_df = add_audit_column(summary_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "ledger_account_summary"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["ledger_account_summary_key"]
    )

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

# CELL ********************

try:
    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.ledger_account"

    ledger_account_df = bronze_df\
                                .withColumn("included_ledger_account_reference", F.explode(F.col("included_ledger_account_reference")))\
                                .selectExpr(
                                    "ledger_account_summary_id",
                                    "included_ledger_account_reference"
                                )

    flattened_df = ledger_account_df.select(
        "ledger_account_summary_id",
        F.explode("included_ledger_account_reference.ID").alias("id_rec")
    ).filter(F.col("id_rec.type") == "Ledger_Account_ID")

    flattened_df = flattened_df.select(
        "ledger_account_summary_id",
        F.col("id_rec.parent_id").alias("parent_id"),
        F.col("id_rec.parent_type").alias("parent_type"),
        F.col("id_rec.type").alias("type"),
        F.col("id_rec._value_1").alias("value")
    )

    result_df = (
        flattened_df
        .groupBy(
            "ledger_account_summary_id",
            "parent_id",
            "parent_type"
        )
        .pivot("type")
        .agg(F.first("value"))
    )
    ledger_account_flat_df = to_lowercase_columns(result_df)

    ledger_account_df = ledger_account_flat_df.select(
            F.sha2(
                F.concat_ws(
                    "||",                                   
                    F.coalesce(F.trim(F.col("ledger_account_summary_id").cast("string")), F.lit("")),
                    F.coalesce(F.trim(F.col("ledger_account_id").cast("string")), F.lit(""))
                ),
                256
            ).alias("ledger_account_key"),
            "ledger_account_id",
            "ledger_account_summary_id",
            "parent_id",
            "parent_type"
        )
    audit_df = add_audit_column(ledger_account_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "ledger_account"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["ledger_account_key"]
    )

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

# CELL ********************

try:
    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.summary_account_range"

    ledger_account_df = bronze_df\
                                .withColumn("included_ledger_account_range_data", F.explode(F.col("included_ledger_account_range_data")))\
                                .selectExpr(
                                    "ledger_account_summary_id",
                                    "included_ledger_account_range_data.*"
                                )

    ledger_account_flat_df = to_lowercase_columns(ledger_account_df)

    ledger_account_df = ledger_account_flat_df.select(
            F.sha2(
                F.concat_ws(
                    "||",                                   
                    F.coalesce(F.trim(F.col("ledger_account_summary_id").cast("string")), F.lit("")),
                    F.coalesce(F.trim(F.col("from_value").cast("string")), F.lit("")),
                    F.coalesce(F.trim(F.col("to_value").cast("string")), F.lit(""))
                ),
                256
            ).alias("summary_account_range_key"),
            "ledger_account_summary_id",
            "from_value",
            "to_value"
        )
    audit_df = add_audit_column(ledger_account_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "summary_account_range"
    )

    audit_df.write.format("delta").mode("overwrite").save(silver_path)

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

# CELL ********************

try:
    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.ledger_account_summary_relation"

    ledger_account_df = bronze_df\
                                .withColumn("ledger_account_summary_reference_data", F.explode(F.col("ledger_account_summary_reference_data")))\
                                .selectExpr(
                                    "ledger_account_summary_id AS ledger_parent_id",
                                    "ledger_account_summary_reference_data"
                                )

    flattened_df = ledger_account_df.select(
        "ledger_parent_id",
        F.explode("ledger_account_summary_reference_data.ID").alias("id_rec")
    ).filter(F.col("id_rec.type") == "Ledger_Account_Summary_ID")

    flattened_df = flattened_df.select(
        "ledger_parent_id",
        F.col("id_rec.parent_id").alias("parent_id"),
        F.col("id_rec.parent_type").alias("parent_type"),
        F.col("id_rec.type").alias("type"),
        F.col("id_rec._value_1").alias("value")
    )

    result_df = (
        flattened_df
        .groupBy(
            "ledger_parent_id",
            "parent_id",
            "parent_type"
        )
        .pivot("type")
        .agg(F.first("value"))
    )
    ledger_account_flat_df = to_lowercase_columns(result_df)

    ledger_account_df = ledger_account_flat_df.select(
            F.sha2(
                F.concat_ws(
                    "||",                                   
                    F.coalesce(F.trim(F.col("ledger_parent_id").cast("string")), F.lit("")),
                    F.coalesce(F.trim(F.col("ledger_account_summary_id").cast("string")), F.lit(""))
                ),
                256
            ).alias("ledger_account_summary_relation_key"),
            F.col("ledger_parent_id").alias("parent_id"),
            F.col("ledger_account_summary_id").alias("child_id")
        )
    audit_df = add_audit_column(ledger_account_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "ledger_account_summary_relation"
    )


    audit_df.write.format("delta").mode("overwrite").save(silver_path)

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

# CELL ********************

bronze_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
