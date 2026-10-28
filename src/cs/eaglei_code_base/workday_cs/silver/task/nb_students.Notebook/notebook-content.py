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

# MARKDOWN ********************

# - Source: students(bronze)
# - Target: student_geocoder,person_name_data,additional_name_data,financial_aid_record_for_student_data,academic_record_for_student_data,personal_profile_data,address_data,home_contact_address_line_data,email_data,phone_data,students


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

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_NAME = "ManualExecution"
DOMAIN = "cs"
SERVICE_NAME = "Academic_Foundation"
OPERATION_NAME = "Get_Students"
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

PIPELINE_NAME = "ingest_silver_layer"
DOMAIN = "cs"
SERVICE_NAME = "Academic_Foundation"
OPERATION_NAME = "Get_Students"
SOURCE_NAME = "workday"
VAR_CS_LIB_NAME = "var_cs_process"
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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.students"
    
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

bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)

students_bronze_df = spark.read.format("delta").load(bronze_abfss_path)\
                        .select("*",
                                F.sha2(
                                        F.concat_ws(
                                            "||",
                                            F.coalesce(F.col("student_id").cast("string"), F.lit("")),
                                            F.coalesce(F.col("universal_id").cast("string"), F.lit(""))
                                        ),
                                        256
                                    ).alias("student_key")
                        )

students_bronze_df.cache()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    geocoder_df = students_bronze_df\
                    .withColumn("geocoder", F.explode(F.col("student_additional_data.geocoder")))\
                    .select(
                            F.col("student_id"),
                            F.col("geocoder.*"),
                            F.col("universal_id"),
                            F.col("geocoderReference.WID").alias("geocoder_key"),
                            )\
                    .drop("geocoderReference")


    geo_audit_df = add_audit_column(geocoder_df, PIPELINE_NAME)

    geo_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "student_geocoder")

    final_count = geo_audit_df.count()
    rows_read = final_count
    rows_written = final_count
        
    write_delta_tbl(source_df = geo_audit_df
                    ,target_table_path = geo_data_path
                    ,keys = ["geocoder_key"])
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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.country_references"
    
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
    country_reference_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "country_references")

    country_reference_df = spark.read.format("delta")\
                            .load(country_reference_path)

    country_region_reference_path  = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "country_region_references")

    country_region_reference_df = spark.read.format("delta")\
                            .load(country_region_reference_path)


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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.person_name_data"
    
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
        person_name_data_df = students_bronze_df\
                            .select(
                                F.col("student_key").alias("person_name_data_key"),
                                F.col("student_key"),
                                F.col("legal_first_name"),
                                F.col("legal_middle_name"),
                                F.col("legal_last_name"),
                                F.col("legal_country_reference.ISO_3166-1_Alpha-2_Code").alias("legal_alpha2_code"),
                                F.col("legal_country_reference.ISO_3166-1_Alpha-3_Code").alias("legal_alpha3_code"),
                                F.col("legal_country_reference.ISO_3166-1_Numeric-3_Code").alias("legal_numeric3_code"),
                                F.col("preferred_first_name"),
                                F.col("preferred_middle_name"),
                                F.col("preferred_last_name"),
                                F.col("preferred_country_reference.ISO_3166-1_Alpha-2_Code").alias("preferred_alpha2_code"),
                                F.col("preferred_country_reference.ISO_3166-1_Alpha-3_Code").alias("preferred_alpha3_code"),
                                F.col("preferred_country_reference.ISO_3166-1_Numeric-3_Code").alias("preferred_numeric3_code")
                            )


        legal_country_df = person_name_data_df.alias("LH")\
                        .join(country_reference_df.alias("RH")
                            ,(
                                (F.lower(F.col("RH.iso_3166_1_alpha_2_code")) == F.lower(F.col("LH.legal_alpha2_code")))
                                & (F.lower(F.col("RH.iso_3166_1_alpha_3_code")) == F.lower(F.col("LH.legal_alpha3_code")))
                                & (F.lower(F.col("RH.iso_3166_1_numeric_3_code")) == F.lower(F.col("LH.legal_numeric3_code")))
                            ),
                            "left")\
                        .selectExpr("LH.*", "RH.country_key AS legal_country_reference_id")\
                        .drop("legal_alpha2_code", "legal_alpha3_code", "legal_numeric3_code")

        preferred_country_df = legal_country_df.alias("LH")\
                        .join(country_reference_df.alias("RH")
                            ,(
                                (F.lower(F.col("RH.iso_3166_1_alpha_2_code")) == F.lower(F.col("LH.preferred_alpha2_code")))
                                & (F.lower(F.col("RH.iso_3166_1_alpha_3_code")) == F.lower(F.col("LH.preferred_alpha3_code")))
                                & (F.lower(F.col("RH.iso_3166_1_numeric_3_code")) == F.lower(F.col("LH.preferred_numeric3_code")))
                            ),
                            "left")\
                        .selectExpr("LH.*", "RH.country_key AS preferred_country_reference_id")\
                        .drop("preferred_alpha2_code", "preferred_alpha3_code", "preferred_numeric3_code")


        person_name_audit_df = add_audit_column(preferred_country_df, PIPELINE_NAME)

        person_name_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "person_name_data")
        
        final_count = person_name_audit_df.count()
        rows_read = final_count
        rows_written = final_count
        
        write_delta_tbl(source_df = person_name_audit_df
                        ,target_table_path = person_name_data_path
                    ,keys = ["person_name_data_key"])
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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.additional_name_data"
    
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
    additional_name_data_df = students_bronze_df\
                            .select(
                                F.explode("additional_name_data").alias("additional_name_data"),
                                F.col("student_key"),
                            )

    expld_additional_name_df = additional_name_data_df\
                                .select(
                                F.col("student_key"),
                                F.col("additional_name_data.Name_Detail_Data.first_name"),
                                F.col("additional_name_data.Name_Detail_Data.middle_name"),
                                F.col("additional_name_data.Name_Detail_Data.last_name"),
                                F.col("additional_name_data.Name_Detail_Data.Country_Reference.ISO_3166-1_Alpha-2_Code").alias("alpha2_code"),
                                F.col("additional_name_data.Name_Detail_Data.Country_Reference.ISO_3166-1_Alpha-3_Code").alias("alpha3_code"),
                                F.col("additional_name_data.Name_Detail_Data.Country_Reference.ISO_3166-1_Numeric-3_Code").alias("numeric3_code"),
                                F.col("additional_name_data.Name_Type_Reference.Additional_Name_Type_ID").alias("name_reference_type")
                            )


    country_ref_joined_df = expld_additional_name_df.alias("LH")\
                        .join(country_reference_df.alias("RH")
                            ,(
                                (F.lower(F.col("RH.iso_3166_1_alpha_2_code")) == F.lower(F.col("LH.alpha2_code")))
                                & (F.lower(F.col("RH.iso_3166_1_alpha_3_code")) == F.lower(F.col("LH.alpha3_code")))
                                & (F.lower(F.col("RH.iso_3166_1_numeric_3_code")) == F.lower(F.col("LH.numeric3_code")))
                            ),
                            "left")\
                        .selectExpr("LH.*", "RH.country_key AS country_reference_id")\
                        .drop("alpha2_code", "alpha3_code", "numeric3_code")\
                        .drop_duplicates()

    added_id_df = country_ref_joined_df\
                    .select(
                        "*",
                        F.sha2(
                            F.concat_ws(
                                "||",
                                F.coalesce(F.col("first_name").cast("string"), F.lit("")),
                                F.coalesce(F.col("middle_name").cast("string"), F.lit("")),
                                F.coalesce(F.col("last_name").cast("string"), F.lit("")),
                                F.coalesce(F.col("name_reference_type").cast("string"), F.lit("")),
                                F.coalesce(F.col("student_key").cast("string"), F.lit("")),
                                F.coalesce(F.col("country_reference_id").cast("string"), F.lit(""))
                            ),
                            256
                        ).alias("additional_name_data_key")
                    )


    additional_name_audit_df = add_audit_column(added_id_df, PIPELINE_NAME)

    additional_name_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "additional_name_data")
    
    final_count = additional_name_audit_df.count()
    rows_read = final_count
    rows_written = final_count
        
    write_delta_tbl(source_df = additional_name_audit_df
                ,target_table_path = additional_name_data_path
                ,keys = ["additional_name_data_key"])

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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.financial_aid_record_for_student_data"
    
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
    financial_aid_df = students_bronze_df\
                            .select(
                                F.col("Financial_Aid_Record_Secured_Reference.Financial_Aid_Record_ID").alias("financial_aid_record_id"),
                                F.col("student_key")
                            )

    added_id_col_df = financial_aid_df\
                                .select(
                                "*",
                                F.sha2(
                                    F.concat_ws(
                                        "||",
                                        F.coalesce(F.col("student_key").cast("string"), F.lit("")),
                                        F.coalesce(F.col("financial_aid_record_id").cast("string"), F.lit(""))
                                    ),
                                    256
                                ).alias("financial_aid_record_for_student_data_key")
                            )


    financial_aid_audit_df = add_audit_column(added_id_col_df, PIPELINE_NAME)

    financial_aid_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "financial_aid_record_for_student_data")
    
    final_count = financial_aid_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    


    write_delta_tbl(source_df = financial_aid_audit_df
                ,target_table_path = financial_aid_data_path
                ,keys = ["financial_aid_record_for_student_data_key"])

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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.academic_record_for_student_data"
    academic_record_df = students_bronze_df\
                        .select(
                            F.explode("Academic_Record_for_Student_Data").alias("academic_record_student_data"),
                            F.col("student_key")
                        )

    added_id_col_df = academic_record_df\
                            .select(
                            F.sha2(
                                F.concat_ws(
                                    "||",
                                    F.coalesce(F.col("student_key").cast("string"), F.lit("")),
                                    F.coalesce(F.col("academic_record_student_data.Academic_Level_Reference.Academic_Level_ID").cast("string"), F.lit("")),
                                    F.coalesce(F.col("academic_record_student_data.Academic_Record_Secured_Reference.Student_Record_ID").cast("string"), F.lit("")),
                                    F.coalesce(F.col("academic_record_student_data.Academic_Unit_Reference.Academic_Unit_ID").cast("string"), F.lit("")),
                                    F.coalesce(F.col("academic_record_student_data.Student_Record_Status_Reference.Student_Record_Status_ID").cast("string"), F.lit("")),
                                ),
                                256
                            ).alias("academic_record_for_student_data_key"),
                            F.col("student_key"),
                            F.col("academic_record_student_data.Academic_Level_Reference.Academic_Level_ID"),
                            F.col("academic_record_student_data.Academic_Record_Secured_Reference.Student_Record_ID"),
                            F.col("academic_record_student_data.Academic_Unit_Reference.Academic_Unit_ID"),
                            F.col("academic_record_student_data.Student_Record_Status_Reference.Student_Record_Status_ID"),
                        )


    academic_record_audit_df = add_audit_column(added_id_col_df, PIPELINE_NAME)

    academic_record_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_record_for_student_data")

    write_delta_tbl(source_df = academic_record_audit_df
                    ,target_table_path = academic_record_data_path
                ,keys = ["academic_record_for_student_data_key"])
    
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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.personal_profile_data"
    
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
    personal_profile_df = students_bronze_df\
                        .select(
                            F.col("student_key"),
                            F.col("Date_of_Birth").alias("date_of_birth"),
                            F.explode(F.col("ethnicity_reference.Ethnicity_ID")).alias("ethnicity_id"),
                            F.col("Gender_Reference.Gender_Code").alias("gender_reference"),
                            F.col("Hispanic_or_Latino").alias("hispanic_or_latino")
                        )



    added_id_col_df = personal_profile_df\
                                .select(
                                F.col("student_key").alias("personal_profile_data_key"),
                                "*"
                            ).drop_duplicates()

    personal_profile_audit_df = add_audit_column(added_id_col_df, PIPELINE_NAME)

    personal_profile_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "personal_profile_data")

    final_count = personal_profile_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    drop_df = personal_profile_audit_df.select("personal_profile_data_key").drop_duplicates()
    
    if table_exists(personal_profile_data_path):
        # Existing Delta table at silver_path
        target = DeltaTable.forPath(spark, personal_profile_data_path)

        # audit_df must contain column: journal_key
        (
            target.alias("t")
            .merge(
                drop_df.alias("s"),
                "t.personal_profile_data_key = s.personal_profile_data_key"   # MERGE condition
            )
            .whenMatchedDelete()                  # When matched, delete from target
            .execute()
        )

    personal_profile_audit_df.write.format("delta").mode("append").option("mergeSchema", True).save(personal_profile_data_path)

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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.address_data"
    
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

def expand_array_column(df, col_name, prefix=None):
    prefix = prefix or col_name

    dtype = df.schema[col_name].dataType

    if not isinstance(dtype, ArrayType):
        df = df.withColumn(
            col_name,
            F.when(
                F.col(col_name).startswith("["),
                F.from_json(
                    F.col(col_name),
                    ArrayType(StringType())
                )
            ).otherwise(
                F.array(F.col(col_name))
            )
        )

    max_size = (
        df.select(F.max(F.size(F.col(col_name))).alias("max_size"))
        .first()["max_size"] or 0
    )

    for i in range(max_size):
        df = df.withColumn(
            f"{prefix}_{i+1}",
            F.col(col_name)[i]
        )

    if f"{prefix}_1" in df.columns:
        df = df.filter(F.col(f"{prefix}_1").isNotNull())
    
    df = df.drop(col_name)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    home_contact_df = students_bronze_df\
                        .withColumn("address_information_data", F.explode(F.col("student_contact_information.Home_Contact_Information_Data.Contact_Information_Data.Person_Address_Information_Data.Address_Information_Data")))\
                            .select(
                                F.col("student_key"),
                                F.col("address_information_data.*"),
                                F.sha2(
                                    F.concat_ws(
                                        "||",
                                        F.coalesce(F.col("address_information_data.address_id").cast("string"), F.lit(""))),
                                    256
                                ).alias("address_key"),
                            )

    ######################################
    ### Address Line Data
    ######################################
    address_line_data_df = home_contact_df\
                            .withColumn("Address_Line_Data", F.explode(F.col("Address_Data.Address_Line_Data")))\
                            .select(
                                    F.col("address_key").alias("home_contact_address_line_data_key"),
                                    F.col("address_key"),
                                    F.col("Address_Line_Data.Type").alias("type"),
                                    F.col("Address_Line_Data._value").alias("value")
                            )
    result_df = (
        address_line_data_df
        .groupBy(
            "home_contact_address_line_data_key",
            "address_key"
        )
        .pivot("type")
        .agg(F.first("value"))
    )

    col_converted_df = to_lowercase_columns(result_df)

    address_line_audit_df = add_audit_column(col_converted_df, PIPELINE_NAME)

    address_line_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "home_contact_address_line_data")
    
    final_count = address_line_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    
    address_line_audit_df.write.format("delta").mode("overwrite").option("mergeSchema", True).save(address_line_path)

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


######################################
### Subregion
######################################

try:
    subregion_data_df = home_contact_df\
                        .withColumn("Subregion_Data", F.explode(F.col("Address_Data.Subregion_Data")))\
                        .select(
                                F.col("address_key").alias("subregion_data_key"),
                                F.col("address_key"),
                                F.col("Subregion_Data.Type").alias("type"),
                                F.col("Subregion_Data._value").alias("value")
                        )

    result_df = (
        subregion_data_df
        .groupBy(
            "subregion_data_key",
            "address_key"
        )
        .pivot("type")
        .agg(F.first("value"))
    )

    subregion_audit_df = add_audit_column(result_df, PIPELINE_NAME)

    subregion_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "subregion_data")

    subregion_audit_df.write.format("delta").mode("overwrite").option("mergeSchema", True).save(subregion_path)

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

######################################
### Submunicipality Data
######################################

# submunicipality_data_df = home_contact_df\
#                         .select(
#                                 F.col("address_key").alias("submunicipality_data_key"),
#                                 F.col("address_key"),
#                                 F.col("Address_Data.Submunicipality_Data")
#                         )\
#                         .filter(F.col("Address_Data.Submunicipality_Data").isNotNull())

# submunicipality_df = expand_array_column(submunicipality_data_df, "Submunicipality_Data", prefix="submunicipality")
# submunicipality_audit_df = add_audit_column(submunicipality_df, PIPELINE_NAME)

# submunicipality_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "submunicipality_data")

# write_delta_tbl(source_df = submunicipality_audit_df
#                 ,target_table_path = submunicipality_path
#                 ,keys = ["submunicipality_data_key"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

######################################
### Address Data
######################################
try:
    address_data_df = home_contact_df\
                        .select(
                        F.col("address_key"),
                        F.col("Address_ID").alias("address_id"),
                        F.col("student_key"),
                        F.col("number_of_days"),
                        F.col("Address_Data.Country_City_Reference.Country_Subregion_Code_In_Country"),
                        F.col("Address_Data.Country_City_Reference.Country_Subregion_Internal_ID"),
                        F.col("Address_Data.Country_Region_Reference.ISO_3166-2_Code").alias("region_alpha2_code"),
                        F.col("Address_Data.Country_Region_Reference.ISO_3166-2_Country-Region_Code").alias("region_2_code"),
                        F.col("Address_Data.Country_Reference.ISO_3166-1_Alpha-2_Code").alias("country_alpha2_code"),
                        F.col("Address_Data.Country_Reference.ISO_3166-1_Alpha-3_Code").alias("country_alpha3_code"),
                        F.col("Address_Data.Country_Reference.ISO_3166-1_Numeric-3_Code").alias("country_numeric3_code"),
                        F.col("Address_Data.Postal_Code").alias("postal_code"),
                        F.col("Usage_Data.Type_Data.Type_Reference.Communication_Usage_Type_ID"),
                        F.col("Usage_Data.Public"),
                        F.col("Usage_Data.Type_Data.Primary")
                        )

    region_country_df = address_data_df.alias("LH")\
                    .join(country_region_reference_df.alias("RH")
                        ,(
                            (F.lower(F.col("LH.region_alpha2_code")) == F.lower(F.col("RH.iso_3166_2_code")))
                            & (F.lower(F.col("LH.region_2_code")) == F.lower(F.col("RH.iso_3166_1_numeric_3_code")))
                        ),
                        "left")\
                    .selectExpr("LH.*", "RH.country_region_key AS country_region_reference_id")\
                    .drop("region_2_code", "region_alpha2_code")

    country_df = region_country_df.alias("LH")\
                    .join(country_reference_df.alias("RH")
                        ,(
                            (F.lower(F.col("RH.iso_3166_1_alpha_2_code")) == F.lower(F.col("LH.country_alpha2_code")))
                            & (F.lower(F.col("RH.iso_3166_1_alpha_3_code")) == F.lower(F.col("LH.country_alpha3_code")))
                            & (F.lower(F.col("RH.iso_3166_1_numeric_3_code")) == F.lower(F.col("LH.country_numeric3_code")))
                        ),
                        "left")\
                    .selectExpr("LH.*", "RH.country_key AS country_reference_id")\
                    .drop("country_alpha2_code", "country_alpha3_code", "country_numeric3_code")


    convt_lower_case_df = to_lowercase_columns(country_df)

    address_data_audit_df = add_audit_column(convt_lower_case_df, PIPELINE_NAME)

    address_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "address_data")
    
    final_count = address_data_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    


    write_delta_tbl(source_df = address_data_audit_df
                    ,target_table_path = address_data_path
                    ,keys = ["address_key"])

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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.email_data"
    
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
    email_info_df = students_bronze_df\
                    .withColumn("email_information_data", F.explode(F.col("student_contact_information.Home_Contact_Information_Data.Contact_Information_Data.Person_Email_Information_Data.Email_Information_Data")))\
                        .select(
                            F.sha2(
                                F.concat_ws(
                                    "||",
                                    F.coalesce(F.col("email_information_data.Email_ID").cast("string"), F.lit(""))),
                                256
                            ).alias("email_data_key"),
                            F.col("student_key"),
                            F.col("email_information_data.Email_Data.Email_Address"),
                            F.col("email_information_data.Usage_Data.Type_Data.Type_Reference.Communication_Usage_Type_ID"),
                            F.col("email_information_data.Email_ID").alias("email_id"),
                        )


    convt_lower_case_df = to_lowercase_columns(email_info_df)

    email_info_audit_df = add_audit_column(convt_lower_case_df, PIPELINE_NAME)

    email_info_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "email_data")
    
    final_count = email_info_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    
    write_delta_tbl(source_df = email_info_audit_df
                    ,target_table_path = email_info_data_path
                    ,keys = ["email_data_key"])

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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.phone_data"
    
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
    phone_data_df = students_bronze_df\
                    .withColumn("phone_information_data", F.explode(F.col("student_contact_information.Home_Contact_Information_Data.Contact_Information_Data.Person_Phone_Information_Data.Phone_Information_Data")))\
                        .select(
                            F.sha2(
                                F.concat_ws(
                                    "||",
                                    F.coalesce(F.col("phone_information_data.Phone_ID").cast("string"), F.lit(""))),
                                256
                            ).alias("phone_data_key"),
                            F.col("student_key"),
                            F.col("phone_information_data.Phone_Data.Complete_Phone_Number"),
                            F.col("phone_information_data.Phone_Data.Country_Code_Reference.Country_Phone_Code_ID"),
                            F.col("phone_information_data.Phone_Data.Device_Type_Reference.Phone_Device_Type_ID"),
                            F.col("phone_information_data.Phone_ID").alias("phone_id"),
                            F.col("phone_information_data.Usage_Data.Type_Data.Type_Reference.Communication_Usage_Type_ID")
                        )


    convt_lower_case_df = to_lowercase_columns(phone_data_df)

    phone_data_audit_df = add_audit_column(convt_lower_case_df, PIPELINE_NAME)

    phone_data_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "phone_data")
    
    final_count = phone_data_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    
    write_delta_tbl(source_df = phone_data_audit_df
                    ,target_table_path = phone_data_path
                    ,keys = ["phone_data_key"])

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

    source_table = f"Silver Tables"
    target_table = f"{silver_lh_name}.{silver_schema_name}.students"
    
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
    students_df = students_bronze_df\
                        .select(
                            F.col("student_key"),
                            F.col("student_id"),
                            F.col("universal_id"),
                            F.col("Account_User_Name"),
                            F.col("Do_Not_Purge_Settings_Data.Do_Not_Purge"),
                            F.col("Other_Student_Information_Data.First_Generation_Data.First_Generation_College_Student"),
                            F.col("student_additional_data")
                        )\
                        .withColumn(
                            "has_veteran_tag",
                            F.expr(
                                "exists(student_additional_data.wtcsBiodemInformation, x -> x.veteran is not null)"
                            )
                        )


    students_with_veteran_flag_df = students_df.selectExpr(
    "*",
    "CASE WHEN has_veteran_tag IS NULL THEN false ELSE has_veteran_tag END AS is_veteran").drop("has_veteran_tag", "student_additional_data")


    flat_bronze_df=to_lowercase_columns(students_with_veteran_flag_df)
    audit_df = add_audit_column(flat_bronze_df, PIPELINE_NAME)
    audit_df2=audit_df.distinct()

    final_count = audit_df2.count()
    rows_read = final_count
    rows_written = final_count

    distinct_row_count = audit_df2.distinct().count()
    distinct_key_count = (audit_df2.select("student_key").distinct().count())
    counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_students_count"])

    silver_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "students")
    write_delta_tbl(source_df = audit_df2
                    ,target_table_path = silver_path
                    ,keys = ["student_key"])
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
