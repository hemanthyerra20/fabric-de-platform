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
VAR_CS_LIB_NAME = ""
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

# PIPELINE_NAME = "ingest_silver_layer"
# DOMAIN = "cs"
# SERVICE_NAME = "Academic_Foundation"
# OPERATION_NAME = "Get_Academic_Period_Date_Controls"
# SOURCE_NAME = "workday"
# VAR_CS_LIB_NAME = "var_cs_process"
# AUDIT_LAKEHOUSE_NAME = "lh_platform_monitoring"
# CONFIG_SCHEMA_NAME = "config"
# CONFIG_TABLE_NAME = "config_param"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    start_time = datetime.now()
    config_workspace_id = get_variable(VAR_CS_LIB_NAME, "platform_monitoring_workspace_id")
    config_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, AUDIT_LAKEHOUSE_NAME)
    config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, CONFIG_SCHEMA_NAME, CONFIG_TABLE_NAME)

    ## Fetching Config object values
    config_df = spark.read.format("delta").load(config_abfss_path)


    bronze_schema_name = get_param_value(config_df,SOURCE_NAME,"bronze_schema_name")
    bronze_lh_name = get_param_value(config_df,SOURCE_NAME,"bronze_lakehouse_name")
    silver_schema_name = get_param_value(config_df,SOURCE_NAME,"silver_schema_name")
    silver_lh_name = get_param_value(config_df,SOURCE_NAME,"silver_lakehouse_name")
    parent_object = get_param_value(config_df,SOURCE_NAME, SERVICE_NAME.lower())
    bronze_table_name = parent_object[OPERATION_NAME.lower()]["bronze_table_name"]
    workspace_id = get_workspace_id(VAR_CS_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_CS_LIB_NAME, silver_lh_name)

    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.academic_period_date_controls"

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
    explode_id_list = [
    {
                    "col_name": "academic_level_reference",
                    "col_not_req": [],
                    "col_rename": {
                     "wid": "academic_level_reference_wid"
                    }
    },
    {
                    "col_name": "academic_unit_reference",
                    "col_not_req": [],
                    "col_rename": {
                     "wid": "academic_unit_reference_wid"
                    }
    },
    {
                    "col_name": "date_control_reference",
                    "col_not_req": [],
                    "col_rename": {
                     "wid": "date_control_reference_wid"
                    }
    }
]

    flat_bronze_df = bronze_df\
                            .select(
                                F.col("academic_period_id"),
                                F.col("academic_period_ref_id"),
                                F.col("wid").alias("academic_period_reference_wid"),
                                F.explode_outer(F.col("academic_level_reference")).alias("academic_level_reference")
                            )
    #display(flat_bronze_df)

    flat_bronze_df_stage2 = flat_bronze_df\
                            .select(
                                F.col("academic_period_id"),
                                F.col("academic_period_ref_id"),
                                F.col("academic_period_reference_wid"),
                                F.col("academic_level_reference.Academic_Level_Reference").alias("academic_level_reference"),
                                F.col("academic_level_reference.Academic_Unit_Reference").alias("academic_unit_reference"),
                                F.explode_outer(F.col("academic_level_reference.Academic_Period_Date_Control")).alias("academic_period_date_control")
                            )
    #9d6bb666f2391001fa31e211e9180001
    flat_bronze_df_stage3 = flat_bronze_df_stage2\
     .select(
                                F.col("academic_period_id"),
                                F.col("academic_period_ref_id"),
                                F.col("academic_period_reference_wid"),
                                F.col("academic_level_reference"),
                                #F.col("academic_level_reference.Academic_Level_Reference").alias("academic_level_reference"),
                                F.col("academic_unit_reference"),
                                F.col("academic_period_date_control.ID").alias("academic_period_date_control_id"),
                                F.col("academic_period_date_control.Use_Course_Section_Start").alias("use_course_section_start"),
                                F.col("academic_period_date_control.Control_Date").alias("control_date"),
                                F.col("academic_period_date_control.None").alias("none"),
                                F.col("academic_period_date_control.Date_Control_Reference").alias("date_control_reference")
                            )
    exploded_ids_df = explode_ids(flat_bronze_df_stage3, explode_id_list)
    #display(exploded_ids_df.filter(F.col("academic_period_reference_wid") == "9d6bb666f2391001fa31e211e9180001"))
    audit_df = add_audit_column(exploded_ids_df, PIPELINE_NAME)
    audit_df2=audit_df.distinct()

    final_count = audit_df2.count()
    rows_read = final_count
    rows_written = final_count

    distinct_row_count = audit_df2.distinct().count()

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_period_date_controls")
    write_delta_tbl(
        source_df=audit_df2,
        target_table_path=silver_path,
        keys=["academic_period_id", "academic_period_ref_id", "academic_period_reference_wid", "academic_period_date_control_id"]
    )
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
