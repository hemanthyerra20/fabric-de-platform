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
PIPELINE_NAME = "ManualRun"
MASTER_RUN_ID = ""
RUN_ID = ""
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
target_table = f"lh_cs_gold.{gold_schema_name}.dim_campus_location"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Campus Location

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType

# Define schema
schema = StructType([
    StructField("id", StringType(), nullable=True),                    # Null
    StructField("name", StringType(), nullable=True),                  # (Online, Unknown)
    StructField("primary_campus_location", StringType(), nullable=False)  # (Online, Unknown)
])

# Example data
data = [
    ("Online", "Online",  "Online"),
    ("Unknown", "Unknown", "Unknown"),
]

# Create DataFrame
hardcode_df = spark.createDataFrame(data=data, schema=schema)

hardcode_df = hardcode_df.selectExpr(
          "sha2(concat_ws('||', "
          "coalesce(id, ''), "
          "coalesce(name, ''), "
          "coalesce(primary_campus_location, '')"
          "), 256) as campus_location_key",
          "*")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    campus_location_ref_path = abfss_path_builder( workspace_id, silver_lakehouse_id, silver_schema_name, "campuslocations")

    # Read campus location mapping
    silver_campuslocations_df = (spark.read.format("delta").load(campus_location_ref_path))


    calc_df = silver_campuslocations_df.withColumn(
        "location_code",
        F.trim(
            F.regexp_extract(
                F.col("cf_esi_location_code_course_tag"),
                r"Location Code\s*::\s*([^;]+)",
                1
            )
        )
    ).select("location_code", "campus_locations")\
    .filter((F.col("location_code").isNotNull()) & (F.col("campus_locations").isNotNull()))\
    .drop_duplicates()

    df = (
        calc_df
        .withColumn("campus_locations", F.explode(F.split(F.col("campus_locations"), ";")))
        .withColumn("campus_locations", F.trim(F.col("campus_locations")))
        .filter(F.col("campus_locations") != "")
        .drop_duplicates()
    )

    df_with_id = (
    df.withColumn("id", F.trim(F.split(F.col("location_code"), "-")[0]))
      .selectExpr(
          "sha2(concat_ws('||', "
          "coalesce(id, ''), "
          "coalesce(location_code, ''), "
          "coalesce(campus_locations, '')"
          "), 256) as campus_location_key",
          "id",
          "location_code as name",
          "campus_locations as primary_campus_location"
      )
    )

    union_df = df_with_id.unionByName(hardcode_df, allowMissingColumns=True)

    # Add audit columns
    camp_audit_df = add_audit_column(union_df, PIPELINE_NAME)

    camp_audit_df.cache()

    final_count = camp_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    # Gold path
    camp_gold_path = abfss_path_builder(
        workspace_id, gold_lakehouse_id, gold_schema_name, "dim_campus_location"
    )

    # Write to Gold
    write_delta_tbl(camp_audit_df, camp_gold_path, ["campus_location_key"])

    camp_audit_df.unpersist()

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_campus_location table: {e}")

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
