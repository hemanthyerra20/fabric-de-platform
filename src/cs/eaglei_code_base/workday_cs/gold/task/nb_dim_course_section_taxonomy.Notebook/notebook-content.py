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
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
# META         },
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

# PARAMETERS CELL ********************

VAR_CS_LIB_NAME = "var_cs_process"
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = "ManualRun"
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
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

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
bronze_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_bronze_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_course_section_taxonomy"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Course Sections Taxonomy

# CELL ********************

try:
    taxonomy_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "course_section_educational_taxonomy_code_reference"
    )
 
    taxonomy_ref_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "educational_taxonomy_code_references"
    )
 
    taxonomy_scheme_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "educational_taxonomy_schemes"
    )
 
    
    taxonomy_df = (
        spark.read.format("delta")
        .load(taxonomy_path)
    )
 
    taxonomy_ref_df = (
        spark.read.format("delta")
        .load(taxonomy_ref_path)
    )
 
    taxonomy_scheme_df = (
        spark.read.format("delta")
        .load(taxonomy_scheme_path)
    )
 
    taxonomy_joined_df = (
        taxonomy_df.alias("LH")
        .join(
            taxonomy_ref_df.alias("RH"),
            F.col("LH.educational_taxonomy_code_id") ==
            F.col("RH.educational_taxonomy_id"),
            "inner"
        )
        .join(
            taxonomy_scheme_df.alias("SCH"),
            F.col("LH.educational_taxonomy_code_id") ==
            F.col("SCH.reference_id"),
            "left"
        )
        .selectExpr(
            "sha2(concat_ws('||', coalesce(CAST(LH.course_section_key AS string), ''), coalesce(CAST(LH.educational_taxonomy_code_id AS string), '')),256) AS taxonomy_key",
            "LH.course_section_key AS course_section_key",
            "LH.course_section_id",
            "LH.educational_taxonomy_code_id AS id",
            "RH.code AS code",
            "RH.name AS name",
            "SCH.name AS scheme",
            "CASE WHEN RH.code IN ('28','32','43') THEN 'In-Contract' ELSE 'Non-Contract' END AS contract_status",
            "'workday' AS source"
        )
    )
 
    tax_audit_df = add_audit_column(
        taxonomy_joined_df,
        PIPELINE_NAME
    )

    final_count = tax_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    tax_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_course_section_taxonomy",
    )
    write_delta_tbl(
        tax_audit_df,
        tax_gold_path,
        ["taxonomy_key"]
    )

    tax_audit_df.unpersist()
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(
        f"Failed to create/load dim_course_section_taxonomy table: {e}"
    )

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
