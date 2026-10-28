# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d35b1ce6-d133-4623-9d58-8c88aba0654b",
# META       "default_lakehouse_name": "lh_platform_monitoring",
# META       "default_lakehouse_workspace_id": "b0bd8a9b-9bdf-4a90-99cf-fed9e6800f1a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         },
# META         {
# META           "id": "0277ffad-3784-4551-8d7d-66d1b8ff633a"
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

# ##### The package should be installed at cluster level and mention version

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

from decimal import Decimal
from datetime import date, datetime, timedelta
from pyspark.sql.functions import explode_outer, col, input_file_name, split, element_at
from pyspark.sql.types import *
import requests
import json

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
DOMAIN = "cs"
SOURCE_NAME = "workday"
ACTIVITY_NAME = "notebook activity"
LEVEL="notebook"
LAYER="landing"
SERVICE_NAME =  "ras_report"
OPERATION_NAME = "Get_Student_Stage_Progress_Funnel" #"Get_ISIR_Transactions_Request"
ENVIRONMENT = "dev"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]
STUDENT_DELTA = ""



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_landing"
job_name = "nb_ingest_landing_layer_prospect_stage"
source_name = "workday"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
config_df = spark.read.format("delta").load(config_abfss_path)\
                .filter((F.col("source") == SOURCE_NAME) & (F.col("domain") == DOMAIN))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token_url = get_param_value(config_df,source_name,"token_url")
kv_url = get_param_value(config_df,source_name,"kv_url")
grant_type = get_param_value(config_df,source_name,"grant_type")
api_url = get_param_value(config_df,source_name,"api_url")
api_url = api_url.replace("${place_holder}", SERVICE_NAME)

kv_client_key_name = get_param_value(config_df,source_name,"kv_client_key_name")
kv_secret_key_name = get_param_value(config_df,source_name,"kv_secret_key_name")
kv_std_refreshtoken_name = get_param_value(config_df,source_name,"kv_std_refreshtoken_name")
tenant_name = get_param_value(config_df,source_name,"tenant_name")
version = get_param_value(config_df,source_name,"version")
root_path_suffix =  get_param_value(config_df,source_name,"root_path_suffix")
landing_schema_name = get_param_value(config_df,source_name,"landing_schema_name")
landing_lakehouse_name = get_param_value(config_df,source_name,"landing_lakehouse_name")


parent_object = get_param_value(config_df,source_name, SERVICE_NAME.lower())
landing_table_name = parent_object[OPERATION_NAME.lower()]["landing_table_name"]
tag_name = parent_object[OPERATION_NAME.lower()]["tag_name"]
count_tag_name = parent_object[OPERATION_NAME.lower()]["count_tag_name"]
overwrite_soap_body = parent_object[OPERATION_NAME.lower()].get("soap_body", "")
response_body_block = parent_object[OPERATION_NAME.lower()].get("response_body_block", "")
api_url = parent_object[OPERATION_NAME.lower()].get("api_url", api_url)


kv_client_key_name = get_param_value(config_df,source_name,"kv_client_key_name")
kv_secret_key_name = get_param_value(config_df,source_name,"kv_secret_key_name")
kv_std_refreshtoken_name = get_param_value(config_df,source_name,"kv_std_refreshtoken_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(api_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Derive bronze lakehouse name from ENVIRONMENT (e.g., dev -> EAGLEI-CS-STORE-DEV)
env_suffix = ENVIRONMENT.upper()
bronzelakehouse = f"EAGLEI-CS-STORE-{env_suffix}"

# Get unique student_wid values from the environment-specific bronze lakehouse
students_df = spark.sql(
    f"SELECT DISTINCT student_wid FROM `{bronzelakehouse}`.lh_cs_bronze.workday_cs.students"
)

# Add a monotonically increasing id and create batches of 50 per row
students_with_idx = students_df.withColumn("rn", F.row_number().over(Window.orderBy("student_wid")))

result_df = (
    students_with_idx
    .withColumn("batch", F.ceil(F.col("rn") / F.lit(50)))
    .groupBy("batch")
    .agg(F.concat_ws(",", F.collect_list("student_wid")).alias("sids"))
    .orderBy("batch")
)

# display(result_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, ArrayType

base_api_url = api_url 
print(f"Base API URL (no IDs appended): {base_api_url}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## KeyVault Params
client_id = str(get_secret(kv_client_key_name, kv_url).strip())
client_secret = str(get_secret(kv_secret_key_name, kv_url).strip())
refresh_token = str(get_secret(kv_std_refreshtoken_name, kv_url).strip())

## Lakehouse Params
landing_workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name, lh_name)

lh_root_path = get_lh_root_path(landing_workspace_id, landing_lakehouse_id)

landing_file_root_path = f"{lh_root_path}{root_path_suffix}"

utc_now = datetime.utcnow()
year = utc_now.strftime("%Y")
month = utc_now.strftime("%m")
day = utc_now.strftime("%d")


landing_folder_path = (
    f"{landing_file_root_path}/"
    f"{SERVICE_NAME.lower()}/"
    f"{OPERATION_NAME.lower()}/"
    f"{year}/"
    f"{month}/"
    f"{day}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# ACCESS TOKEN
# =====================================================
 
access_token = fetch_access_token(
    token_url,
    grant_type,
    refresh_token,
    client_id,
    client_secret
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_total_results(raw_df):
    """Try to read Total_Results from the Workday payload using count_tag_name."""

    if raw_df is None or len(raw_df.columns) == 0:
        return None

    total_results = None
    response_data_df = raw_df

    try:
        for tname in count_tag_name.split("."):
            tag_found_for_level = False

            for field in response_data_df.schema:
                if field.name.lower() == tname.lower():
                    tag_found_for_level = True

                    if isinstance(field.dataType, StructType):
                        response_data_df = response_data_df.select(f"{field.name}.*")
                    elif isinstance(field.dataType, ArrayType):
                        response_data_df = (
                            response_data_df
                            .withColumn(field.name, explode_outer(col(field.name)))
                            .select(f"{field.name}.*")
                        )

                    break

        first_row = (
            response_data_df
            .select("Total_Results")
            .limit(1)
            .collect()
        )

        total_results = first_row[0]["Total_Results"] if first_row else None

    except Exception:
        # Bubble up the real error, don't hide it
        raise

    return total_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import array_size, col


def get_report_entry_size(df, default_value=0):
    """Return the size of Report_Entry array (first row only)."""

    if df is None:
        return default_value

    if "Report_Entry" not in df.columns:
        return default_value

    rows = (
        df.select(array_size(col("Report_Entry")).alias("report_entry_size"))
          .limit(1)
          .collect()
    )

    if not rows:
        return default_value

    value = rows[0]["report_entry_size"]
    if value is None:
        return default_value

    return int(value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_parent_tag(raw_df):
    """Walk tag_name path and flatten the matching node."""

    if raw_df is None or len(raw_df.columns) == 0:
        return raw_df

    df = raw_df

    try:
        for tname in tag_name.split("."):
            tag_found_for_level = False

            for field in df.schema:
                if field.name.lower() == tname.lower():
                    tag_found_for_level = True

                    if isinstance(field.dataType, StructType):
                        cols = [c for c in df.columns if c != field.name]
                        df = df.select(*cols, f"{field.name}.*")

                    elif isinstance(field.dataType, ArrayType):
                        cols = [c for c in df.columns if c != field.name]
                        df = (
                            df.withColumn(field.name, explode_outer(col(field.name)))
                              .select(*cols, f"{field.name}.*")
                        )

                    break

            if not tag_found_for_level:
                raise KeyError(f"Expected tag '{tname}' not found in schema")

        return df

    except Exception:
        # Let caller see the real failure
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Main ingest for Workday -> landing

from pyspark.sql.functions import array_size
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from concurrent.futures import ThreadPoolExecutor, as_completed


def ingest_landing_layer(service_name: str, operation_name: str, job_name: str):
    """Hit Workday, land JSON, parse and write to Delta."""
    try:
        if service_name.lower() == "ras_report":
            # ------------------------------------------------------------------
            # Call Workday in 50-ID batches, 20 in parallel, using Student!WID
            # URL pattern:
            #   {base_api_url}?Student!WID=id1!id2!id3&format=json
            # All batches land into the SAME date folder; filenames are
            # distinguished by ingest_api_to_lakehouse (no _batchid in folder).
            # ------------------------------------------------------------------
            headers = {"Authorization": f"Bearer {access_token}"}

            # Collect all batches from result_df
            batch_rows = (
                result_df
                .select("batch", "sids")
                .orderBy("batch")
                .collect()
            )

            def call_batch(batch_id, sids_str):
                # Split comma-separated WIDs, then join with '!'
                wid_list = [s.strip() for s in sids_str.split(",") if s.strip()]
                if not wid_list:
                    return None

                wid_query = "!".join(wid_list)
                sep = "&" if "?" in base_api_url else "?"
                url = f"{base_api_url}{sep}Student!WID={wid_query}&format=json"

                # IMPORTANT: keep operation_name unchanged so all batches
                # write into the same folder structure. Batch identity can be
                # inferred from file naming inside ingest_api_to_lakehouse.
                batch_operation_name = operation_name.lower()

                print(f"Calling batch {batch_id}")
 
                return ingest_api_to_lakehouse(
                    url=url,
                    headers=headers,
                    service_name=service_name.lower(),
                    operation_name=batch_operation_name,
                    workspace_id=landing_workspace_id,
                    lakehouse_id=landing_lakehouse_id,
                )

            # Execute up to 20 batches in parallel
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [
                    executor.submit(call_batch, int(r["batch"]), r["sids"])
                    for r in batch_rows
                ]
                for fut in as_completed(futures):
                    # Propagate any error immediately
                    fut.result()

        # ------------------------------------------------------------------
        # Read all landed JSON (all batch files) from landing folder
        # ------------------------------------------------------------------
        raw_df = (
            spark.read.format("json")
                 .option("multiline", True)
                 .option("mode", "FAILFAST")
                 .option("maxStringLen", 50000000)
                 .load(landing_folder_path)
        )

        # Logical count: if this is a delta run (below threshold), use number of IDs;
        # otherwise rely on Workday metadata.
        global parsed_sids, is_delta_run  # set in SOAP cell when applicable
        if 'is_delta_run' in globals() and is_delta_run and parsed_sids:
            src_count = len(parsed_sids)
        elif count_tag_name == "NA":
            src_count = int(get_report_entry_size(raw_df) or 0)
        else:
            src_count = int(get_total_results(raw_df) or 0)

        parsed_df = parse_parent_tag(raw_df)

        col_converted_df = to_lowercase_columns(parsed_df)
        added_audit_col = add_audit_column(col_converted_df, job_name)
        added_id_col = add_uuid_column(added_audit_col, "id")
        add_file_name_df = added_id_col.withColumn(
            "file_name", element_at(split(input_file_name(), "/"), -1)
        )

        landing_abfss_path = abfss_path_builder(
            landing_workspace_id,
            landing_lakehouse_id,
            landing_schema_name,
            landing_table_name,
        )

        nested_cols_to_normalize = ["student_additional_data", "student_data"]

        for col_name in nested_cols_to_normalize:
            if col_name in add_file_name_df.columns:
                try:
                    existing_df_for_schema = spark.read.format("delta").load(landing_abfss_path)

                    if col_name in existing_df_for_schema.columns:
                        target_type = next(
                            f.dataType
                            for f in existing_df_for_schema.schema.fields
                            if f.name == col_name
                        )

                        if isinstance(target_type, StructType):
                            print(f"Casting '{col_name}' to existing table schema.")
                            add_file_name_df = add_file_name_df.withColumn(
                                col_name,
                                F.from_json(F.to_json(col_name), target_type),
                            )
                        else:
                            print(f"Column '{col_name}' exists but is not StructType; skipping cast.")
                except Exception as e:
                    print(
                        f"Warning: could not align '{col_name}' schema with existing table. Details: {e}"
                    )

        physical_rows = add_file_name_df.count()
        print(f"Debug: physical_rows_to_write={physical_rows}, logical_src_count={src_count}")

        tgt_count = src_count

        (
            add_file_name_df.write.format("delta")
                             .mode("append")
                             .save(landing_abfss_path)
        )

        return {
            "source_count": src_count,
            "target_count": tgt_count,
            "status": "success",
        }

    except Exception as e:
        try:
            mssparkutils.logging.logError(
                f"Error in ingest_landing_layer for service={service_name}, operation={operation_name}: {str(e)}"
            )
            print(str(e))
        except Exception:
            print(
                f"Error in ingest_landing_layer for service={service_name}, "
                f"operation={operation_name}: {str(e)}"
            )

        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException  # needed for the specific exception handler

SOURCE_TABLE=f"{SERVICE_NAME.lower()}.{OPERATION_NAME.lower()}"
TARGET_TABLE= f"{landing_lakehouse_name}.{landing_schema_name}.{landing_table_name}"

start_time = datetime.now()
end_time = datetime.now()
rows_read = 0
rows_written = 0
rows_rejected = 0

try:
    # ==========================================
    # BUSINESS LOGIC
    # ==========================================

    print("Executing business logic...")

    result = ingest_landing_layer(SERVICE_NAME, OPERATION_NAME, RUN_ID)

    print("business logic completed...")

    # print(result)
    rows_read = int(result.get("source_count", 0))
    rows_written = int(result.get("target_count", 0))

    # updated audit log to SUCCESS
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())

    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
        start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("SUCCESS logged")

    # ==========================================
    # WATERMARK TABLE UPDATE (on successful ingestion only)
    # ==========================================
    try:
        from pyspark.sql import functions as F

        watermark_abfss_path = abfss_path_builder(
            landing_workspace_id,
            landing_lakehouse_id,
            landing_schema_name,
            "student_watermark",
        )

        previous_date = (datetime.utcnow() - timedelta(days=1)).date()
        current_ts = datetime.utcnow()

        # rows_count is populated from rows_read (source_count)
        watermark_df = spark.createDataFrame(
            [
                (
                    _safe_str(MASTER_RUN_ID),
                    _safe_str(NOTEBOOK_NAME),
                    _safe_str(SERVICE_NAME),
                    previous_date,
                    current_ts,
                    int(rows_read),
                )
            ],
            schema=["masterRunID", "nb_name", "service_name", "lastsync", "created_at", "rows_count"],
        )

        (
            watermark_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", True)
            .save(watermark_abfss_path)
        )

        print("student_watermark table updated")
    except Exception as wm_err:
        # Do not fail the pipeline if watermark update fails; just log
        print(f"Warning: failed to update student_watermark table: {wm_err}")

except AnalysisException as e:
    # Handle specific Delta schema merge issues gracefully
    error_message = str(e)
    if "DELTA_FAILED_TO_MERGE_FIELDS" in error_message and "student_data" in error_message:
        print("Warning: Delta schema merge conflict on 'student_data'. Skipping write and logging failure.")
    else:
        print(f"Unhandled AnalysisException: {error_message}")

    # updated audit log to FAILURE (AnalysisException)
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
        start_time, end_time, error_message, error_message, _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("FAILED logged (AnalysisException)")
    # Do not re-raise to allow pipeline to continue

except requests.exceptions.HTTPError as e:
    # Upstream Workday HTTP error: log as failure but keep notebook run green
    error_message = str(e)
    print(f"HTTPError from Workday API: {error_message}")

    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())

    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed",  # business failure due to upstream system, but technical run succeeds
        _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
        start_time, end_time, error_message, error_message, _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0,
    )
    print("FAILED logged (HTTPError from Workday API); not re-raising to keep notebook run successful.")

except Exception as e:
    # updated audit log to FAILURE (generic)
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    print("FAILED logged (generic exception)")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
