# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0277ffad-3784-4551-8d7d-66d1b8ff633a",
# META       "default_lakehouse_name": "lh_cs_bronze",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "0277ffad-3784-4551-8d7d-66d1b8ff633a"
# META         },
# META         {
# META           "id": "5bd544a5-340c-493c-aa88-61dc1992c614"
# META         },
# META         {
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
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
from pyspark.sql.types import StructType, ArrayType
import json
from datetime import date, datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

JOB_NAME = "canvas_testing"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
landing_schema_name = 'canvas'
bronze_schema_name = 'canvas'
landing_lakehouse_name = 'lh_cs_landing'
bronze_lakehouse_name='lh_cs_bronze'
notebook_name = notebookutils.runtime.context["currentNotebookName"]
config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
#config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
store_workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name,landing_lakehouse_name)
bronze_lakehouse_id = get_lakehouse_id(var_cs_lib_name,bronze_lakehouse_name)
landing_table_name ="canv_submissions"
table_names=["canv_accounts",
"canv_assignments",
"canv_content_tags",
"canv_course_sections",
"canv_courses",
"canv_enrollment_terms",
"canv_enrollments",
"canv_learning_outcome_groups",
"canv_learning_outcome_results",
"canv_learning_outcomes",
"canv_outcome_proficiencies",
"canv_outcome_proficiency_ratings",
"canv_pseudonyms",
"canv_rubric_assessments",
"canv_rubric_associations",
"canv_rubrics",
"canv_submissions",
"canv_users"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

landing_path = f"abfss://{store_workspace_id}@onelake.dfs.fabric.microsoft.com/{landing_lakehouse_id}/Tables/{landing_schema_name}"
for table in table_names:
    print(table)
 
for table_name in mssparkutils.fs.ls(landing_path):
    print(table_name.name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, ArrayType

# -------------------------------------------------------------------
# Build Landing & Bronze Base Paths
# -------------------------------------------------------------------

landing_base_path = (
    f"abfss://{store_workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com/"
    f"{landing_lakehouse_id}/Tables/{landing_schema_name}"
)

bronze_base_path = (
    f"abfss://{store_workspace_id}"
    f"@onelake.dfs.fabric.microsoft.com/"
    f"{bronze_lakehouse_id}/Tables/{bronze_schema_name}"
)

# -------------------------------------------------------------------
# Get all landing tables
# -------------------------------------------------------------------

landing_tables = [
    table.name.rstrip("/")
    for table in mssparkutils.fs.ls(landing_base_path)
]

print(f"Total Landing Tables: {len(landing_tables)}")
print(landing_tables)

# -------------------------------------------------------------------
# Process each table
# -------------------------------------------------------------------

def process_table(table_name):

    try:
        print(f"\nProcessing Table: {table_name}")

        landing_path = f"{landing_base_path}/{table_name}"
        bronze_path = f"{bronze_base_path}/{table_name}"

        # Read Landing Table
        landing_df = (
            spark.read
            .format("delta")
            .load(landing_path)
        )

        if landing_df.rdd.isEmpty():
            print(f"{table_name} is empty.")
            return

        # Keep only hashkey and rowdata
        landing_df = landing_df.select("hashkey", "rowdata")

        # Infer schema from all JSON records
        schema = spark.read.json(
            landing_df.select("rowdata").rdd.map(lambda r: r.rowdata)
        ).schema

        # Parse JSON
        parsed_df = landing_df.withColumn(
            "json_data",
            from_json(col("rowdata"), schema)
        )

        
        select_exprs = [col("hashkey")]  # always keep hashkey

        for field in schema.fields:
            dt = field.dataType

            # Special handling for top-level `key` struct that contains `id`
            if field.name == "key" and isinstance(dt, StructType):
                nested_field_names = {f.name for f in dt.fields}
                if "id" in nested_field_names:
                    select_exprs.append(col("json_data.key.id").alias("key_id"))
                    continue  # do not keep full `key` JSON as a column

            if isinstance(dt, StructType):
                # Flatten one level for struct fields, but alias only by the
                # nested field name (no parent prefix like value_ or meta_)
                for nested in dt.fields:
                    nested_col = col(f"json_data.{field.name}.{nested.name}")
                    alias_name = nested.name  # remove value_ / key_ style prefixes
                    select_exprs.append(nested_col.alias(alias_name))

            elif isinstance(dt, ArrayType) and isinstance(dt.elementType, StructType):
                # Keep arrays of structs as-is for now; do not explode rows to
                # avoid changing the grain. You can later explode specific
                # columns in downstream logic if needed.
                select_exprs.append(col(f"json_data.{field.name}"))

            else:
                # Primitive (or other) fields: project directly
                select_exprs.append(col(f"json_data.{field.name}"))

        bronze_df = parsed_df.select(*select_exprs)

        print(f"Writing Bronze Table: {table_name}")

        # Use standard Delta writer helper with hashkey as the merge key
        write_delta_tbl(
            bronze_df,
            bronze_path,
            ["hashkey"],
        )

        print(f"{table_name} loaded successfully.")

    except Exception as e:
        print(f"Failed to process {table_name}")
        print(e)

# -------------------------------------------------------------------
# Process all tables in parallel
# -------------------------------------------------------------------

MAX_WORKERS = 8

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    executor.map(process_table, landing_tables)

print("\nAll Canvas Landing tables have been loaded into the Bronze layer successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
