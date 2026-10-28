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
PIPELINE_NAME = ""
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
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_course_sections"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Course Section

# CELL ********************

try:
    course_sections_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_sections")
    section_listing_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "section_listing_data")
    delivery_mode_ref_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "delivery_mode_references")
    instr_format_ref_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "instructional_format_references")
    acad_period_ref_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_period_references")
    acad_unit_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "course_section_academic_unit_data")
    maintained_aca_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "maintained_academic_periods")
    college_path = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "academic_unit_references")


    acad_unit_df = spark.read.format("delta").load(acad_unit_path)
    delivery_mode_df = spark.read.format("delta").load(delivery_mode_ref_path)
    instr_format_df = spark.read.format("delta").load(instr_format_ref_path)
    acad_period_df = spark.read.format("delta").load(acad_period_ref_path)
    section_listing_df = spark.read.format("delta").load(section_listing_path)
    maintained_aca_df = spark.read.format("delta").load(maintained_aca_path)\
                            .selectExpr("id", "academic_period_name AS academic_period")\
                            .drop_duplicates()

    college_df = (spark.read.format("delta").load(college_path)
        .selectExpr(
            "academic_unit_key AS college_key",
            "academic_unit_id AS college_id",
            "description AS name",
        )
        .drop_duplicates()
    )

    course_sections_df = (spark.read.format("delta").load(course_sections_path)
        .select(
            "course_section_key",
            "course_section_id",
            F.col("title").alias("section_name"),
            "delivery_mode_id",
            "instructional_format_id",
            F.col("academic_period_id"),
            F.col("start_date").cast("date").alias("section_start_date"),
            F.col("end_date").cast("date").alias("section_end_date"),
            "section_capacity",
            "minimum_units",
            "maximum_units",
            "contact_hours",
        )
    )

    delivery_joined_df = (
        course_sections_df.alias("lh")
        .join(
            delivery_mode_df.alias("rh"),
            F.col("lh.delivery_mode_id") == F.col("rh.delivery_mode_id"),
            "left",
        )
        .join(
            maintained_aca_df.alias("mn"),
            F.col("lh.academic_period_id") == F.col("mn.id"),
            "left"
        )
        .selectExpr(
            "lh.*",
            "rh.description AS delivery_mode",
            "mn.academic_period"
        )
        .drop("academic_period_id")
    )

    instr_joined_df = (
        delivery_joined_df.alias("lh")
        .join(
            instr_format_df.alias("rh"),
            F.col("lh.instructional_format_id") == F.col("rh.instructional_format_id"),
            "left",
        )
        .join(
            acad_unit_df.alias("au"),
            F.col("lh.course_section_key") == F.col("au.course_section_key"),
            "left",
        )
        .join(
            college_df.alias("cl"),
            F.col("au.academic_unit_id") == F.col("cl.college_id"),
            "left",
        )
        .join(
            section_listing_df.alias("sl"),
            F.col("lh.course_section_key") == F.col("sl.course_section_key"),
            "left")
        .selectExpr(
            "lh.*",
            "sl.course_listing_number AS course_code",
            "rh.description AS instructional_format"
        )
        .drop(
            "delivery_mode_id",
            "instructional_format_id"
        )
    )

    instr_joined_df = instr_joined_df.drop_duplicates()

    course_section_audit_df = add_audit_column(instr_joined_df, PIPELINE_NAME)

    course_section_audit_df.cache()

    final_count = course_section_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    course_sections_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_course_sections",
    )

    write_delta_tbl(course_section_audit_df, course_sections_gold_path, ["course_section_key"])

    course_section_audit_df.unpersist()
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_course_sections table: {e}")

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
