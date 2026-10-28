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
OPERATION_NAME = "Get_Cost_centers"
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.cost_center"

    cost_center_df = bronze_df.selectExpr(
                                    "cost_center_reference", 
                                    "include_organization_id_in_name", 
                                    "organization_name",
                                    "phonetic_name",
                                    "organization_code",
                                    "include_organization_code_in_name",
                                    "organization_active",
                                    "availability_date",
                                    "effective_date",
                                    "external_url_reference",
                                    "organization_subtype_reference",
                                    "organization_visibility_reference"
                                )
    explode_id_list = [
            {
                            "col_name": "cost_center_reference",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "organization_subtype_reference",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "organization_visibility_reference",
                            "col_not_req": [],
                            "col_rename": {
                                "wid": "visibility_wid"
                            }
            }
        ]

    cost_center_ids_df = explode_ids(cost_center_df, explode_id_list)

    cc_bronze_df = to_lowercase_columns(cost_center_ids_df)

    cc_selected_df = cc_bronze_df\
                        .select(
                            F.sha2(F.coalesce(F.trim(F.col("cost_center_reference_id").cast("string")), F.lit('')), 256).alias("cost_center_key"),
                            F.col('cost_center_reference_id').alias("cost_center_id"),
                            F.col('organization_reference_id').alias("organization_id"),
                            F.col('organization_name'),
                            F.col('phonetic_name'),
                            F.col('organization_code'),
                            F.col('organization_subtype_id'),
                            F.col('include_organization_id_in_name'),
                            F.col('include_organization_code_in_name'),
                            F.col('organization_active'),
                            F.col('availability_date').cast("date"),
                            F.col("effective_date").cast("date"),
                            F.col('external_url_reference'),
                            F.col('visibility_wid')
                        )

    audit_df = add_audit_column(cc_selected_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "cost_center"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["cost_center_key"]
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.cost_center_container"

    container_df = bronze_df\
                                .withColumn("organization_container_reference", F.explode(F.col("organization_container_reference")))\
                                .selectExpr(
                                    "organization_container_reference",
                                    "cost_center_reference"
                                )

    explode_id_list = [
            {
                            "col_name": "organization_container_reference",
                            "col_not_req": ["wid"],
                            "col_rename": {}
            },
            {
                            "col_name": "cost_center_reference",
                            "col_not_req": ["wid", "organization_reference_id"],
                            "col_rename": {}
            }
        ]

    container_ids_df = explode_ids(container_df, explode_id_list)

    container_bronze_df = to_lowercase_columns(container_ids_df)

    container_select_df = container_bronze_df.select(
            F.sha2(
                F.concat_ws(
                    "||",                                   
                    F.coalesce(F.trim(F.col("cost_center_reference_id").cast("string")), F.lit("")),
                    F.coalesce(F.trim(F.col("organization_reference_id").cast("string")), F.lit(""))
                ),
                256
            ).alias("cost_center_container_key"),
            F.sha2(
                F.concat_ws(
                    "||",                                   
                    F.coalesce(F.trim(F.col("cost_center_reference_id").cast("string")), F.lit(""))
                ),
                256
            ).alias("cost_center_key"),
            F.col("cost_center_reference_id").alias("cost_center_id"),
            "organization_reference_id",
            "custom_organization_reference_id"
        )
    audit_df = add_audit_column(container_select_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "cost_center_container"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["cost_center_container_key"]
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.cost_center_worktag"

    worktag_df = bronze_df\
                    .selectExpr(
                            "related_worktags_data.*",
                            "cost_center_reference"
                        )

    select_df = worktag_df\
                        .withColumn("Related_Worktags_by_Type_Data", F.explode(F.col("Related_Worktags_by_Type_Data")))\
                        .withColumn("Allowed_Worktag_Data", F.explode(F.col("Related_Worktags_by_Type_Data.Allowed_Worktag_Data")))\
                        .select(
                            F.col("Allowed_Worktag_Data.Allowed_Worktag_Reference"),
                            F.col("Related_Worktags_by_Type_Data.Default_Worktag_Data.Default_Worktag_Reference"),
                            F.col("Related_Worktags_by_Type_Data.Replace_All_Allowed_Values"),
                            F.col("Related_Worktags_by_Type_Data.Required_On_Transaction"),
                            F.col("Related_Worktags_by_Type_Data.Required_On_Transaction_For_Validation"),
                            F.col("Related_Worktags_by_Type_Data.Worktag_Type_Reference"),
                            F.col("cost_center_reference")
    )

    explode_id_list = [
            {
                            "col_name": "Default_Worktag_Reference",
                            "col_not_req": ["wid"],
                            "col_rename": {"fund_id": "default_worktag_fund_id"}
            },
            {
                            "col_name": "cost_center_reference",
                            "col_not_req": ["wid", "organization_reference_id"],
                            "col_rename": {"cost_center_reference_id" : "cost_center_id"}
            },
            {
                            "col_name": "Worktag_Type_Reference",
                            "col_not_req": ["wid"],
                            "col_rename": {"cost_center_reference_id" : "cost_center_id"}
            }, 
            {
                            "col_name": "Allowed_Worktag_Reference",
                            "col_not_req": ["wid"],
                            "col_rename": {"fund_id" : "allowed_worktag_fund_id"}
            }, 
        ]

    wrktag_ids_df = explode_ids(select_df, explode_id_list)

    wrktag_bronze_df = to_lowercase_columns(wrktag_ids_df)

    select_df = wrktag_bronze_df\
                    .select(
                        F.sha2(
                            F.concat_ws(
                                "||",                                   
                                F.coalesce(F.trim(F.col("cost_center_id").cast("string")), F.lit("")),
                                F.coalesce(F.trim(F.col("worktag_type_id").cast("string")), F.lit(""))
                            ),
                            256
                        ).alias("cost_center_worktag_key"),
                    F.sha2(
                        F.concat_ws(
                            "||",                                   
                            F.coalesce(F.trim(F.col("cost_center_id").cast("string")), F.lit(""))
                        ),
                        256
                    ).alias("cost_center_key"),
                    'replace_all_allowed_values', 
                    'required_on_transaction', 
                    'required_on_transaction_for_validation', 
                    'default_worktag_fund_id', 
                    'cost_center_id', 
                    'accounting_worktag_type_id', 
                    'worktag_type_id', 
                    'fund_hierarchy_id', 
                    'allowed_worktag_fund_id'
            )

    audit_df = add_audit_column(select_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "cost_center_worktag"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["cost_center_worktag_key"]
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.cost_center_integration_id"

    intg_df = bronze_df\
                    .withColumn("integration_id_data", F.explode(F.col("integration_id_data")))\
                    .selectExpr(
                            "integration_id_data",
                            "cost_center_reference"
                        )

    explode_id_list = [
            {
                            "col_name": "cost_center_reference",
                            "col_not_req": ["wid", "organization_reference_id"],
                            "col_rename": {"cost_center_reference_id" : "cost_center_id"}
            }
        ]

    ids_df = explode_ids(intg_df, explode_id_list)

    flattened_df = ids_df.select(
        "cost_center_id",
        F.explode("integration_id_data.ID").alias("id_rec")
    ).filter(F.col("id_rec.System_ID").isin(["WD-I", "WD-WID"]))


    flattened_df = flattened_df.select(
        "cost_center_id",
        F.col("id_rec.System_ID").alias("system_id"),
        F.col("id_rec._value_1").alias("value")
    )

    result_df = (
        flattened_df
        .groupBy(
            "cost_center_id"
        )
        .pivot("system_id")
        .agg(F.first("value"))
    )

    select_df = result_df.selectExpr(
                "cost_center_id",
                "`WD-I` AS wd_i",
                "`WD-WID` AS wd_wid"
    )

    ints_df = to_lowercase_columns(select_df)

    select_df = ints_df\
                    .select(
                        F.sha2(
                            F.concat_ws(
                                "||",                                   
                                F.coalesce(F.trim(F.col("cost_center_id").cast("string")), F.lit("")),
                                F.coalesce(F.trim(F.col("wd_i").cast("string")), F.lit("")),
                                F.coalesce(F.trim(F.col("wd_wid").cast("string")), F.lit(""))
                            ),
                            256
                        ).alias("integration_key"),
                    F.sha2(
                        F.concat_ws(
                            "||",                                   
                            F.coalesce(F.trim(F.col("cost_center_id").cast("string")), F.lit(""))
                        ),
                        256
                    ).alias("cost_center_key"),
                    'wd_i', 
                    'wd_wid'
            )

    audit_df = add_audit_column(select_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "cost_center_integration_id"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["integration_key"]
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

bronze_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
