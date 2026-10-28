# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08846ef1-977d-4698-b06f-84fdbd834bc6",
# META       "default_lakehouse_name": "lh_cs_silver",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "08846ef1-977d-4698-b06f-84fdbd834bc6"
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
# OPERATION_NAME = "Get_Programs_of_Study"
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.program_of_study"

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
    ref_schema = StructType([
    StructField(
        "ID",
        ArrayType(
            StructType([
                StructField("_value_1", StringType(), True),
                StructField("type", StringType(), True)
            ]),
            True
        ),
        True
    )
])

    bronze_df = spark.read.format("delta").load(bronze_abfss_path)
    flat_df = bronze_df\
            .withColumn("program_of_study_data_snapshot_data", F.explode_outer(F.col("program_of_study_data_snapshot_data")))\
            .select("*",
                    "program_of_study_data_snapshot_data.*"
                    )\
            .drop("program_of_study_data_snapshot_data")

    select_df = flat_df\
                .withColumn(
                    "Program_Area_Reference",
                    F.transform(
                        F.col("Program_Area_Reference"),
                        lambda x: F.from_json(x, ref_schema)
                    )
                )\
                .selectExpr(
                    "sha2(concat_ws('||', coalesce(cast(program_of_study_id as string), ''), coalesce(cast(Effective_Date as string), '')), 256) as program_of_study_key",
                    "program_of_study_id",
                    "program_of_study_type_reference",
                    "Academic_Unit_Reference",
                    "Academic_Level_Reference",
                    "explode_outer(Program_Area_Reference) AS Program_Area_Reference",
                    "CIP_Code_Reference",
                    "explode_outer(Default_Credentials_Reference) AS Default_Credentials_Reference",
                    "explode_outer(Educational_Taxonomy_Codes_Reference) AS Educational_Taxonomy_Codes_Reference",
                    "Effective_Date",
                    "Evaluate_Academic_Requirements_Prior_to_Program_Change",
                    "Exclude_Program_of_Study_from_National_Student_Clearinghouse_Reporting",
                    "Exclude_Students_in_Program_of_Study_from_National_Student_Clearinghouse_Reporting",
                    "Fast_Path_Enabled",
                    "Grant_Single_Credential",
                    "Program_Duration_Months",
                    "Program_Duration_Weeks",
                    "Program_Duration_Years",
                    "Program_Focus_Required",
                    "Program_Minimum_Coursework_Clock",
                    "Program_Minimum_Coursework_Credit",
                    "Program_Name",
                    "Program_of_Study_Code",
                    "Route_by_Coordinating_Academic_Unit",
                    "Route_by_Owning_Academic_Unit",
                    )


    explode_id_list = [
    {
                    "col_name": "program_of_study_type_reference",
                    "col_not_req": ["wid"],
                    "col_rename": {}
    },
    {
                    "col_name": "Academic_Unit_Reference",
                    "col_not_req": ["wid"],
                    "col_rename": {}
    },
    {
                    "col_name": "Academic_Level_Reference",
                    "col_not_req": ["wid"],
                    "col_rename": {}
    },
    {
                    "col_name": "CIP_Code_Reference",
                    "col_not_req": ["wid", "cip_code_id_2020"],
                    "col_rename": {}
    },
    {
                    "col_name": "Default_Credentials_Reference",
                    "col_not_req": ["wid"],
                    "col_rename": {}
    },
    {
                    "col_name": "Educational_Taxonomy_Codes_Reference",
                    "col_not_req": ["wid"],
                    "col_rename": {}
    },
    {
                    "col_name": "Program_Area_Reference",
                    "col_not_req": ["wid"],
                    "col_rename": {}
    },
]

    exploded_ids_df = explode_ids(select_df, explode_id_list)
    pos_col_convt_df = to_lowercase_columns(exploded_ids_df)

    if "program_area_id" not in pos_col_convt_df.columns:
        pos_col_convt_df = pos_col_convt_df.withColumn("program_area_id", F.lit(""))
    audit_df = add_audit_column(pos_col_convt_df, PIPELINE_NAME)
    audit_df2=audit_df.distinct()

    final_count = audit_df2.count()
    rows_read = final_count
    rows_written = final_count

    distinct_row_count = audit_df2.distinct().count()
    distinct_key_count = (audit_df2.select("program_of_study_key").distinct().count())
    counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_isir_caa_count"])

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "program_of_study")
    write_delta_tbl(source_df = audit_df2
                    ,target_table_path = silver_path
                    ,keys = ["program_of_study_key"])
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
