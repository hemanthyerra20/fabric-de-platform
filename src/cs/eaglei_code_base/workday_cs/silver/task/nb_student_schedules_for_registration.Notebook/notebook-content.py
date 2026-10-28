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
# SERVICE_NAME = "Student_Records"
# OPERATION_NAME = "Get_Student_Schedules_for_Registration"
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
    target_table = f"{silver_lh_name}.{silver_schema_name}.student_schedule_for_registration"

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
    student_schd_reg_df = bronze_df\
                        .select(
                            F.col("id"),
                            F.col("schedule_name"),
                            F.col("academic_period_reference"),
                            F.col("academic_record_reference"),
                        )
    explode_id_list = [
    {
                    "col_name": "academic_record_reference",
                    "col_not_req": ["wid"],
                    "col_rename": {
                        "Student_Record_ID": "student_record_id"
                    }
    },
     {
                    "col_name": "academic_period_reference",
                    "col_not_req": ["wid"],
                    "col_rename": {
                        "Academic_Period_ID": "academic_period_id"
                    }
    },

]

    exploded_ids_df = explode_ids(student_schd_reg_df, explode_id_list)
    flat_bronze_df=to_lowercase_columns(exploded_ids_df)
    audit_df = add_audit_column(flat_bronze_df, PIPELINE_NAME)
    audit_df2=audit_df.distinct()

    final_count = audit_df2.count()
    rows_read = final_count
    rows_written = final_count

    distinct_row_count = audit_df2.distinct().count()
    distinct_key_count = (audit_df2.select("id").distinct().count())
    counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_registration_count"])

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_schedules_for_registration")
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

student_schd_reg_item_df = bronze_df\
                        .select(
                            F.col("id"),
                            F.explode_outer(F.col("student_schedule_for_registration_item")).alias("student_schedule_for_registration_item"),
                        )

std_item_selected_df = student_schd_reg_item_df\
                        .select(
                            F.col("id").alias("student_schedules_for_registration_id"),
                            F.col("student_schedule_for_registration_item.Academic_Period_Reference"),
                            F.explode_outer(F.col("student_schedule_for_registration_item.Course_Section_Reference")).alias("Course_Section_Reference"),
                            F.col("student_schedule_for_registration_item.Grading_Basis_Reference"),
                            F.col("student_schedule_for_registration_item.Units").alias("units")
                        )

explode_id_list = [
    {
                    "col_name": "Course_Section_Reference",
                    "col_not_req": ["wid"],
                    "col_rename": {
                        "Section_Listing_ID": "section_listing_id"
                    }
    },
     {
                    "col_name": "Academic_Period_Reference",
                    "col_not_req": ["wid"],
                    "col_rename": {
                        "Academic_Period_ID": "academic_period_id"
                    }
    },
]
exploded_ids_df = explode_ids(std_item_selected_df, explode_id_list)


custom_pivot_df = exploded_ids_df\
                    .withColumn("grading_basis_reference", F.explode_outer("Grading_Basis_Reference.ID"))\
                    .select("grading_basis_reference.*",
                            "student_schedules_for_registration_id",
                            "section_listing_id",
                            "academic_period_id",
                            "units"
                            )\
                    .filter(F.col("type") == "Grading_Basis_ID")

pivoted_df = custom_pivot_df\
                .groupBy(*[x for x in custom_pivot_df.columns if x not in ["_value_1", "type"]])\
                .pivot("type")\
                .agg(F.first("_value_1"))

col_selected_df = pivoted_df\
                    .select(
                        F.sha2(
                                F.concat_ws(
                                    "||",
                                    F.coalesce(F.col("student_schedules_for_registration_id").cast("string"), F.lit("")),
                                    F.coalesce(F.col("section_listing_id").cast("string"), F.lit("")),
                                    F.coalesce(F.col("academic_period_id").cast("string"), F.lit(""))
                                ),
                                256
                            ).alias("id"),
                        F.col("student_schedules_for_registration_id"),
                        F.col("section_listing_id"),
                        F.col("academic_period_id"),
                        F.col("Grading_Basis_ID").alias("grading_basis_id"),
                        F.col("parent_id").alias("grading_basis_parent_id"),
                        F.col("parent_type").alias("grading_basis_parent_type"),
                        F.col("units"),
                    )

student_schd_item_audit_df = add_audit_column(col_selected_df, PIPELINE_NAME)

student_schd_item_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_schedule_for_registration_item")

write_delta_tbl(source_df = student_schd_item_audit_df
                ,target_table_path = student_schd_item_path
                ,keys = ["id"])

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
