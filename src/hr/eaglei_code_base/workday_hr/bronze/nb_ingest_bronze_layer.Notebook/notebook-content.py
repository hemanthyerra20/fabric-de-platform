# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "863239a1-544f-4d86-83b2-80ab40b759eb",
# META       "default_lakehouse_name": "lh_hr_bronze",
# META       "default_lakehouse_workspace_id": "fc434332-df9b-42bf-9f6d-2e6f2ab3fca9",
# META       "known_lakehouses": [
# META         {
# META           "id": "863239a1-544f-4d86-83b2-80ab40b759eb"
# META         },
# META         {
# META           "id": "b229f4c0-e27f-4fb4-92f4-5d4edc0a773c"
# META         }
# META       ]
# META     },
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
from pyspark.sql.types import StructType, ArrayType
import json
from datetime import date, datetime, timedelta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

MASTER_RUN_ID = "test123"
RUN_ID = "test123"
PIPELINE_ID = "test123"
PIPELINE_NAME = "test123"
DOMAIN = "hr"
SOURCE_NAME = "workday"
ACTIVITY_NAME = "notebook activity"
LEVEL="notebook"
LAYER="bronze"
SERVICE_NAME = "Ras_Report"
OPERATION_NAME = "Get_Faculty_Pay_NPA"
ENVIRONMENT = "dev"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_landing_name = "lh_hr_landing"
lh_bronze_name = "lh_hr_bronze"

var_hr_lib_name = "var_hr_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"
xpath_details_table_name = "xpath_details"


config_workspace_id = get_variable(var_hr_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_hr_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
xpath_abfss_apth = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, xpath_details_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

t_minus_1 = datetime.today() - timedelta(days=40)
year = t_minus_1.year
month = t_minus_1.month
day = t_minus_1.day
print("loading data for the date:",day,"/",month,"/",year) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Fetching Config object values
config_df = spark.read.format("delta").load(config_abfss_path)\
                .filter((F.col("source") == SOURCE_NAME) & (F.col("domain") == DOMAIN))
display(config_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

landing_schema_name = get_param_value(config_df,SOURCE_NAME,"landing_schema_name")
bronze_schema_name = get_param_value(config_df,SOURCE_NAME,"bronze_schema_name")
landing_lh_name = get_param_value(config_df,SOURCE_NAME,"landing_lakehouse_name")
bronze_lh_name = get_param_value(config_df,SOURCE_NAME,"bronze_lakehouse_name")


parent_object = get_param_value(config_df,SOURCE_NAME, SERVICE_NAME.lower())
landing_table_name = parent_object[OPERATION_NAME.lower()]["landing_table_name"]
bronze_table_name = parent_object[OPERATION_NAME.lower()]["bronze_table_name"]
landing_full_table_name = f"{landing_lh_name}.{landing_schema_name}.{landing_table_name}"
bronze_full_table_name = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_workspace_id(var_hr_lib_name, DOMAIN, "store")
landing_lakehouse_id = get_lakehouse_id(var_hr_lib_name, lh_landing_name)
bronze_lakehouse_id = get_lakehouse_id(var_hr_lib_name, lh_bronze_name)

landing_abfss_path = abfss_path_builder(workspace_id, landing_lakehouse_id, landing_schema_name, landing_table_name)
bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

xpath_df = spark.read.format("delta").load(xpath_abfss_apth)\
                .filter(F.col("table_name") == bronze_full_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(xpath_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Fetching Config object values
xpath_df = spark.read.format("delta").load(xpath_abfss_apth)\
                .filter(F.col("table_name") == bronze_full_table_name)

xpath_df = xpath_df.withColumn(
    "xpath",
    F.regexp_replace(
        F.col("xpath"),
        r"^[^>]+",
        F.lower(F.regexp_extract(F.col("xpath"), r"^([^>]+)", 1))
    )
)

xpath_df.cache()

if xpath_df.count() == 0:
    raise Exception(f"Xpath config not found for {bronze_full_table_name} table")


xpath_col_details = {x.xpath : x.alias_name for x in xpath_df.select("alias_name", "xpath").collect()}

xpath = list(xpath_col_details.keys())

primary_keys_value = xpath_df.select("primary_keys").distinct().first()[0]

primary_keys = (
    [c.strip() for c in primary_keys_value.split(",")]
    if primary_keys_value
    else []
)

partition_by_value = xpath_df.select("partition_by").distinct().first()[0]

partition_by = (
    [c.strip() for c in partition_by_value.split(",")]
    if partition_by_value
    else []
)
xpath_df.unpersist()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#######################
# Update this cell when implementing the incremental logic.
#######################
landing_df = spark.read.format("delta")\
                .load(landing_abfss_path)
print("landing count:", landing_df.count())
filter_date = date(year, month, day)

filtered_df = (
    landing_df.filter(
        F.to_date(F.col("modified_on")) >= F.lit(filter_date))
)
print("filtered_df count:",filtered_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def xpath_rename(df, cols_mapping):
    col_lst = []
    for col in df.columns:
        col_nm = cols_mapping.get(col, None)
        if col_nm:
            col_lst.append(f"`{col}` AS {col_nm}")
        else:
            col_lst.append(f"`{col}` AS {col}")
    
    return df.selectExpr(*col_lst)
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_dtype(schema, path):
    parts = path.split(">")
    current = schema
    for part in parts:
        if isinstance(current, StructType):
            current = current[part].dataType
        elif isinstance(current, ArrayType):
            current = current.elementType
            if isinstance(current, StructType):
                current = current[part].dataType
    return current


def selective_flatten(df, xpath_list):
    exploded = set()
    for full_path in xpath_list:
        parts = full_path.split(">")
        for i in range(len(parts)):
            current_path = ">".join(parts[: i + 1])
            dtype = get_dtype(df.schema, current_path)
            if isinstance(dtype, ArrayType):
                # explode only if not final xpath
                if current_path != full_path:
                    if current_path not in exploded:
                        df = df.withColumn(
                            current_path,
                            F.explode_outer(F.col(f"`{current_path}`"))
                        )
                        exploded.add(current_path)
            
            #flatten next child
            if i < len(parts) - 1:
                next_field = parts[i + 1]
                new_col = f"{current_path}>{next_field}"
                if new_col not in df.columns:
                    df = df.withColumn(
                        new_col,
                        F.col(f"`{current_path}`.`{next_field}`")
                    )

    # KEEP ONLY XPATH COLUMNS
    final_cols = [f"`{col}`" for col in xpath_list]
    df = df.selectExpr(*final_cols)
    return df


selective_flatten_df = selective_flatten(filtered_df, xpath)
renamed_xpath_df = xpath_rename(selective_flatten_df, xpath_col_details)
print("count:",filtered_df.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build explode_and_pivot_lst for the current bronze table based on the
# configuration stored in the xpath_details table. Each entry in the list
# describes a column that needs special explode/pivot processing in the
# downstream transformation step.

xpath_df = spark.read.format("delta").load(xpath_abfss_apth)\
                .filter(F.col("table_name") == bronze_full_table_name)

# Final configuration list consumed by explode_ids(). Each element is of
# the form:
#   {"col_name": <string>, "col_not_req": [<string>, ...]}
# where:
#   - col_name    : the nested column that should be exploded/pivoted.
#   - col_not_req : (optional) list of exploded ID columns that should be
#                   dropped after the pivot step.
explode_and_pivot_lst = []

# Build explode_and_pivot_lst from the explode_and_pivot_required JSON column
# in xpath_df. We guard against None/empty values and malformed JSON and
# re-raise any unexpected errors so callers see the full traceback.
try:
    # Each row in xpath_df corresponds to a table/xpath configuration. The
    # explode_and_pivot_required column, when populated, contains a JSON
    # object describing which nested columns need explode/pivot handling.
    for x in xpath_df.select("explode_and_pivot_required").collect():
        # Skip rows where explode_and_pivot_required is null/empty. These
        # rows do not require any explode/pivot logic.
        if not x.explode_and_pivot_required:
            continue

        # Parse the JSON text into a Python dict. Example shape:
        #   {
        #     "Student_Tags": {"col_not_req": ["Tag_Type"]},
        #     "Program_Tags": {"col_not_req": []}
        #   }
        # json.loads preserves the original structure; any JSON error will
        # raise and be propagated.
        explode_cfg = json.loads(x.explode_and_pivot_required)

        # Each key (k) is a column to explode/pivot; the value (v) holds
        # additional metadata such as which pivoted columns to drop.
        for k, v in explode_cfg.items():
            explode_and_pivot_lst.append(
                {
                    "col_name": k,
                    "col_not_req": v.get("col_not_req", []),
                    "col_rename": v.get("col_rename", {})
                }
            )
except Exception:
    # Re-raise the original exception so the caller retains full type
    # and traceback. This allows upstream orchestration to decide how
    # to handle configuration or data issues (e.g., bad JSON, missing
    # expected keys, etc.).
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

primary_keys_str = ",".join(primary_keys)

flattend_df = explode_ids(renamed_xpath_df, explode_and_pivot_lst)
col_nm_convted_df = to_lowercase_columns(flattend_df)
print("count",col_nm_convted_df.count())
if OPERATION_NAME.lower().endswith("_references"):
    col_nm_convted_df = col_nm_convted_df.distinct()
    
# Default logic based on modified_on
removed_duplicates = col_nm_convted_df\
                .selectExpr("*", f"row_number() over(partition by {primary_keys_str} order by modified_on desc) AS rn")\
                .filter(F.col("rn") == 1)\
                .drop("rn", "modified_on")
    
audit_col_df = add_audit_column(removed_duplicates, PIPELINE_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

SOURCE_TABLE= landing_full_table_name
TARGET_TABLE= bronze_full_table_name

start_time = datetime.now()
end_time = datetime.now()
rows_read = 0
rows_written = 0
rows_rejected = 0

try:
    

    print("Executing business logic...")

    if table_exists(bronze_abfss_path):
        upsert_data(
            bronze_abfss_path,
            audit_col_df,
            primary_keys
        )
    else:
        writer = (
            audit_col_df.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
        )

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.save(bronze_abfss_path)

    print("business logic completed...")

    rows_read = int(filtered_df.count())
    rows_written = int(audit_col_df.count())

    # updated audit log to SUCCESS
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())

    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
        start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("SUCCESS logged:",rows_written)

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
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
