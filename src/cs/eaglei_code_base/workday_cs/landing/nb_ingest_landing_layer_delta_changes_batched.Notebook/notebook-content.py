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
from datetime import date, datetime
from pyspark.sql.functions import explode_outer, col, input_file_name, split, element_at
from pyspark.sql.types import *
import requests
import json
import time

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
LEVEL = "notebook"
LAYER = "landing"
SERVICE_NAME = "ras_report"
OPERATION_NAME = "get_student_course_registration"
ENVIRONMENT = "dev"
WORKSPACE_NAME = notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]
# BATCHES_JSON = ''
BATCHES_JSON = "[{\"BatchNumber\":1,\"StudentNumbers\":\"10002496,10019136,10403767\",\"wdids\":\"1ccbe1cf678390001ad0096d286e0000,346b4c29337b90015d47b6bd7f8b0000,1ccbe1cf678390001ac73dcc52ee0000\",\"PairCount\":50}]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_landing"
job_name = "nb_ingest_landing_layer_delta_changes"
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

import json

def _parse_batches_json(raw: str):
    """Normalize pipeline BATCHES_JSON into a Python list of batch dicts.

    Supported shapes:
      1) "[{...}, {...}]"                      -> list
      2) "{\"BatchNumber\":1,...}"             -> single dict (wrapped to list)
      3) "{ \"value\": "[{...}]" }"            -> Data Factory-style wrapper
      4) "{ \"value\": { ... } }"              -> wrapper with single object
    """
    if not raw:
        return []

    try:
        top = json.loads(raw)
    except Exception as e:
        raise ValueError(
            "Invalid BATCHES_JSON. Expected JSON string containing "
            "an array of batch objects or a { 'value': ... } wrapper. "
            f"Parse error: {e}"
        )

    # Case 3/4: wrapper object with a "value" property
    if isinstance(top, dict) and "value" in top:
        inner = top["value"]

        # inner might still be a JSON string
        if isinstance(inner, str):
            try:
                inner = json.loads(inner)
            except Exception as e:
                raise ValueError(
                    "BATCHES_JSON.value could not be parsed as JSON. "
                    f"Parse error: {e}"
                )

        if isinstance(inner, dict):
            return [inner]
        if isinstance(inner, list):
            return inner
        raise ValueError(
            "BATCHES_JSON.value must be a JSON array or object containing "
            "batch definitions."
        )

    # Case 1/2: top-level is already the actual payload
    if isinstance(top, dict):
        return [top]
    if isinstance(top, list):
        return top

    raise ValueError(
        "BATCHES_JSON must represent either: a batch object, an array of "
        "batch objects, or a wrapper { 'value': ... }."
    )


BATCHES = _parse_batches_json(BATCHES_JSON)

print(f"Loaded {len(BATCHES)} API batch(es) from pipeline input.")

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

def _csv_values(value):
    """Return a cleaned list from a comma-separated pipeline parameter."""
    if value is None:
        return []
    return [str(x).strip() for x in str(value).split(",") if str(x).strip()]


def build_batch_api_url(base_url, wdids):
    """Build the RaaS URL for one Workday WID batch."""
    wid_values = _csv_values(wdids)

    sep = "&" if "?" in base_url else "?"

    if wid_values:
        # Workday RaaS expects multiple WIDs as a single parameter
        # separated by '!'.
        return f"{base_url}{sep}Student!WID={'!'.join(wid_values)}&format=json"

    # No WIDs: still request JSON from the base URL.
    if "format=json" not in base_url:
        return f"{base_url}{sep}format=json"

    return base_url


def build_batch_soap_body(student_numbers):
    """Build the configured SOAP body for one Student-number batch."""
    student_refs_xml = ""
    student_ids = _csv_values(student_numbers)

    if student_ids:
        parts = []
        for sid in student_ids:
            parts.append(
                f"""                <bsvc:Student_Reference>
                    <bsvc:ID bsvc:type="Student_ID">{sid}</bsvc:ID>
                </bsvc:Student_Reference>"""
            )
        student_refs_xml = "\n".join(parts)

    if overwrite_soap_body:
        return overwrite_soap_body

    if response_body_block:
        request_refs_block = ""
        if student_refs_xml:
            request_refs_block = f"""            <bsvc:Request_References>
    {student_refs_xml}
                </bsvc:Request_References>
    """

        return f"""
                <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:bsvc="urn:com.workday/bsvc">
                    <soapenv:Body>
                        <bsvc:{OPERATION_NAME}_Request xmlns:bsvc="urn:com.workday/bsvc">
    {request_refs_block}                        <bsvc:Response_Filter>
                                <bsvc:Page>1</bsvc:Page>
                                <bsvc:Count>100</bsvc:Count>
                            </bsvc:Response_Filter>
                            {response_body_block}
                        </bsvc:{OPERATION_NAME}_Request>
                    </soapenv:Body>
                </soapenv:Envelope>
        """

    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(soap_body)

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
    """Extract the Total_Results value from the Workday response.

    The function is designed to be defensive against both struct and array
    representations of the `Response_Results` field.

    Args:
        raw_df (DataFrame): Spark DataFrame read from the raw Workday JSON.

    Returns:
        int or None: The `Total_Results` value if present, otherwise None.
    """

    if raw_df is None or len(raw_df.columns) == 0:
        # Nothing to inspect; caller can interpret None as "no results"
        return None

    total_results = None
    response_data_df = raw_df

    try:
        for tname in count_tag_name.split("."):
            tag_found_for_level = False

            for field in response_data_df.schema:
                # If the current schema field name matches the current tag
                # step, select all other columns plus the expanded struct.
                if field.name.lower() == tname.lower():
                    tag_found_for_level = True
                    if isinstance(field.dataType, StructType):
                        response_data_df = response_data_df.select(f"{field.name}.*")

                        # If we encounter an array type, explode it so that each
                        # element becomes its own row before continuing traversal.
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
        # Re-raise the original exception so the caller keeps full type and traceback
        # (preferred over `raise Exception(str(e))`, which would lose that context)
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
    # 1. Basic DF validation
    if df is None:
        return default_value

    # 2. Column existence check
    if "Report_Entry" not in df.columns:
        return default_value

    # 3. Handle empty DF safely
    rows = (
        df
        .select(array_size(col("Report_Entry")).alias("report_entry_size"))
        .limit(1)
        .collect()
    )

    if not rows:
        return default_value

    # 4. Handle null array_size result
    value = rows[0]["report_entry_size"]
    if value is None:
        return default_value

    return int(value)

# # Usage:
# report_entry_size = get_report_entry_size(raw_df, default_value=0)
# print(report_entry_size)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_parent_tag(raw_df):
    """Parse the Workday response payload and explode the configured child tag.

    This function walks the configured `tag_name` path (e.g. "Response_Data.Students")
    and normalizes the structure by:
      - Traversing nested structs level by level.
      - Exploding arrays so each element becomes a separate row.

    Args:
        raw_df (DataFrame): Spark DataFrame read from the raw Workday JSON.

    Returns:
        DataFrame: Parsed and flattened DataFrame for the configured `tag_name`.
    """

    # If there is nothing to parse, return as-is and let the caller decide
    # how to interpret an empty / None DataFrame (often "no data").
    if raw_df is None or len(raw_df.columns) == 0:
        return raw_df

    # Work on a local reference that we progressively traverse / flatten.
    response_data_df = raw_df

    try:
        # Walk the configured tag path (e.g. "Response_Data.Students")
        # one segment at a time, matching tags case-insensitively.
        for tname in tag_name.split("."):
            tag_found_for_level = False

            for field in response_data_df.schema:
                # Match the current tag segment to a field in the schema.
                if field.name.lower() == tname.lower():
                    tag_found_for_level = True

                    if isinstance(field.dataType, StructType):
                        # For a struct, keep all other columns and expand
                        # the struct into top-level columns.
                        cols = [c for c in response_data_df.columns if c != field.name]
                        response_data_df = response_data_df.select(*cols, f"{field.name}.*")

                    elif isinstance(field.dataType, ArrayType):
                        # For an array, explode it so that each element becomes
                        # a separate row, then expand the element fields.
                        cols = [c for c in response_data_df.columns if c != field.name]

                        response_data_df = (
                            response_data_df
                            .withColumn(field.name, explode_outer(col(field.name)))
                            .select(*cols, f"{field.name}.*")
                        )

                    # We handled the matched field for this level; move on
                    # to the next tag segment.
                    break

            # After scanning all fields for this level, ensure we actually
            # matched the expected tag. If not, fail fast with a clear error.
            if not tag_found_for_level:
                raise KeyError(f"Expected tag '{tname}' not found in Response_Data schema")

        # At this point, response_data_df should represent the flattened
        # structure under the configured tag path.
        return response_data_df

    except Exception:
        # Re-raise the original exception so the caller keeps full type and traceback.
        # This lets the orchestration / calling layer decide how to handle failures
        # such as missing tags, schema changes, or malformed payloads.
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ingest one Workday Student Delta batch into the landing layer.
#
# IMPORTANT:
#   - One notebook session can process many API batches.
#   - Each API batch is read only from the file(s) created for that batch.
#   - This avoids re-reading the entire daily landing folder after every batch.
#   - Delta append is performed once per batch to keep memory usage bounded.

from pyspark.sql.functions import array_size
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import builtins


def ingest_landing_layer_batch(
    service_name: str,
    operation_name: str,
    job_name: str,
    batch_number: int,
    student_numbers: str,
    wdids: str,
):
    """Extract, parse, audit, and write one Student Delta API batch."""

    batch_start = time.time()

    try:
        batch_api_url = api_url
        batch_soap_body = None
        output_file = None

        if service_name.lower() == "ras_report":
            headers = {
                "Authorization": f"Bearer {access_token}"
            }

            # RaaS uses Workday IDs for Student!WID.
            batch_api_url = build_batch_api_url(api_url, wdids)

            print("=" * 70)
            print(f"Batch {batch_number}: API request")
            print(f"Student pair count: {len(_csv_values(student_numbers))}")
            print(f"WID count: {len(_csv_values(wdids))}")
            print(f"API URL length: {len(batch_api_url)}")
            print("=" * 70)

            api_start = time.time()

            output_file = ingest_api_to_lakehouse(
                url=batch_api_url,
                headers=headers,
                service_name=service_name.lower(),
                operation_name=operation_name.lower(),
                workspace_id=landing_workspace_id,
                lakehouse_id=landing_lakehouse_id,
            )

            api_elapsed = time.time() - api_start
            print(f"Batch {batch_number}: API/extract completed in {api_elapsed:.2f} sec")
            print(f"Batch {batch_number}: Output file = {output_file}")

            if not output_file:
                raise RuntimeError(
                    f"Batch {batch_number}: ingest_api_to_lakehouse did not return an output file."
                )

            # Read ONLY this batch's API output.
            raw_path = output_file

        else:
            # SOAP body is generated independently for each batch.
            batch_soap_body = build_batch_soap_body(student_numbers)

            extract_workday_api_pages(
                api_url=api_url,
                operation_name=operation_name,
                service_name=service_name,
                count_per_page=200,
                access_token=access_token,
                workspace_id=landing_workspace_id,
                lakehouse_id=landing_lakehouse_id,
                soap_body=batch_soap_body,
            )

            # The existing SOAP helper controls its output path. Because the
            # helper does not expose a batch-specific output path, the RaaS
            # batch path above is the recommended path for this notebook.
            raw_path = landing_folder_path

        # =====================================================
        # Load ONLY the current batch response
        # =====================================================
        read_start = time.time()

        raw_df = (
            spark.read.format("json")
            .option("multiline", True)
            .option("mode", "FAILFAST")
            .option("maxStringLen", 50000000)
            .load(raw_path)
        )

        print(
            f"Batch {batch_number}: response read completed in "
            f"{time.time() - read_start:.2f} sec"
        )

        # =====================================================
        # Logical source count
        # =====================================================
        if count_tag_name == "NA":
            src_count = int(get_report_entry_size(raw_df) or 0)
        else:
            src_count = int(get_total_results(raw_df) or 0)

        # =====================================================
        # Parse response
        # =====================================================
        parsed_df = parse_parent_tag(raw_df)

        # =====================================================
        # Add audit / technical columns
        # =====================================================
        col_converted_df = to_lowercase_columns(parsed_df)
        added_audit_col = add_audit_column(col_converted_df, job_name)
        added_id_col = add_uuid_column(added_audit_col, "id")

        add_file_name_df = added_id_col.withColumn(
            "file_name",
            element_at(split(input_file_name(), "/"), -1)
        )

        # =====================================================
        # Normalize known nested columns to existing Delta schema
        # =====================================================
        landing_abfss_path = abfss_path_builder(
            landing_workspace_id,
            landing_lakehouse_id,
            landing_schema_name,
            landing_table_name,
        )

        nested_cols_to_normalize = [
            "student_additional_data",
            "student_data",
        ]

        for col_name in nested_cols_to_normalize:
            if col_name in add_file_name_df.columns:
                try:
                    existing_df_for_schema = (
                        spark.read.format("delta")
                        .load(landing_abfss_path)
                    )

                    if col_name in existing_df_for_schema.columns:
                        target_type = next(
                            f.dataType
                            for f in existing_df_for_schema.schema.fields
                            if f.name == col_name
                        )

                        if isinstance(target_type, StructType):
                            print(
                                f"Batch {batch_number}: casting '{col_name}' "
                                "to existing Delta schema."
                            )

                            add_file_name_df = add_file_name_df.withColumn(
                                col_name,
                                F.from_json(
                                    F.to_json(F.col(col_name)),
                                    target_type,
                                ),
                            )

                except Exception as e:
                    print(
                        f"Warning: could not align '{col_name}' schema for "
                        f"batch {batch_number}. Details: {e}"
                    )

        physical_rows = add_file_name_df.count()

        print(
            f"Batch {batch_number}: physical rows={physical_rows}, "
            f"logical source count={src_count}"
        )

        # =====================================================
        # Delta append for this batch
        # =====================================================
        write_start = time.time()

        (
            add_file_name_df.write.format("delta")
            .mode("append")
            .save(landing_abfss_path)
        )

        write_elapsed = time.time() - write_start

        total_elapsed = time.time() - batch_start

        print(
            f"Batch {batch_number}: Delta write={write_elapsed:.2f} sec, "
            f"total={total_elapsed:.2f} sec"
        )

        return {
            "batch_number": batch_number,
            "source_count": src_count,
            "target_count": src_count,
            "physical_rows": physical_rows,
            "status": "success",
            "api_url_length": len(batch_api_url) if service_name.lower() == "ras_report" else None,
            # Force use of Python built-in round to avoid PySpark Column overload
            "elapsed_seconds": builtins.round(float(total_elapsed), 2),
            "output_file": output_file,
        }

    except Exception as e:
        print(
            f"Batch {batch_number} failed for service={service_name}, "
            f"operation={operation_name}: {str(e)}"
        )
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Inspect existing Delta table schema for landing table
# from pyspark.sql.functions import col
# from pyspark.sql.types import StringType

# landing_abfss_path = abfss_path_builder(
#     landing_workspace_id,
#     landing_lakehouse_id,
#     landing_schema_name,
#     landing_table_name,
# )

# print("Landing Delta path:", landing_abfss_path)

# try:
#     existing_df = spark.read.format("delta").load(landing_abfss_path)
#     print("Existing table schema:")
#     existing_df.printSchema()
# except Exception as e:
#     print("Could not read existing Delta table:", str(e))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException

SOURCE_TABLE = f"{SERVICE_NAME.lower()}.{OPERATION_NAME.lower()}"
TARGET_TABLE = f"{landing_lakehouse_name}.{landing_schema_name}.{landing_table_name}"

notebook_start_time = datetime.now()

total_rows_read = 0
total_rows_written = 0
total_rows_rejected = 0
batch_results = []

try:
    print("=" * 80)
    print("STUDENT DELTA BATCH INGESTION")
    print("=" * 80)
    print(f"Total API batches : {len(BATCHES)}")
    print(f"Service           : {SERVICE_NAME}")
    print(f"Operation         : {OPERATION_NAME}")
    print(f"Batch size target  : 50")
    print("=" * 80)

    for index, batch in enumerate(BATCHES, start=1):

        batch_number = int(batch.get("BatchNumber", index))
        student_numbers = str(batch.get("StudentNumbers") or "")
        wdids = str(batch.get("wdids") or "")

        # Fail fast if a batch is malformed.
        if not student_numbers and not wdids:
            raise ValueError(
                f"Batch {batch_number} contains neither StudentNumbers nor wdids."
            )

        # RaaS Student!WID uses the Workday IDs.
        # StudentNumbers are retained for pairing, auditing and SOAP support.
        result = ingest_landing_layer_batch(
            service_name=SERVICE_NAME,
            operation_name=OPERATION_NAME,
            job_name=RUN_ID,
            batch_number=batch_number,
            student_numbers=student_numbers,
            wdids=wdids,
        )

        batch_results.append(result)

        total_rows_read += int(result.get("source_count", 0))
        total_rows_written += int(result.get("target_count", 0))

        print(
            f"Completed batch {batch_number} "
            f"({index}/{len(BATCHES)})"
        )

    notebook_end_time = datetime.now()

    print("=" * 80)
    print("ALL STUDENT DELTA BATCHES COMPLETED")
    print("=" * 80)
    print(f"Total batches       : {len(BATCHES)}")
    print(f"Total source rows   : {total_rows_read}")
    print(f"Total target rows   : {total_rows_written}")
    print(
        f"Total notebook time : "
        f"{(notebook_end_time - notebook_start_time).total_seconds():.2f} sec"
    )
    print("=" * 80)

    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID),
        _safe_str(RUN_ID),
        _safe_str(PIPELINE_NAME),
        _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN),
        _safe_str(SOURCE_NAME),
        _safe_str(ACTIVITY_NAME),
        "success",
        _safe_str(LEVEL),
        _safe_str(LAYER),
        _safe_str(SOURCE_TABLE),
        _safe_str(TARGET_TABLE),
        total_rows_read,
        total_rows_written,
        total_rows_rejected,
        notebook_start_time,
        notebook_end_time,
        "",
        "",
        _safe_str(ENVIRONMENT),
        _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID),
        _safe_str(PIPELINE_NAME),
        rerun_count=0
    )

    print("SUCCESS logged")

except AnalysisException as e:

    error_message = str(e)
    notebook_end_time = datetime.now()

    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID),
        _safe_str(RUN_ID),
        _safe_str(PIPELINE_NAME),
        _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN),
        _safe_str(SOURCE_NAME),
        _safe_str(ACTIVITY_NAME),
        "failed",
        _safe_str(LEVEL),
        _safe_str(LAYER),
        _safe_str(SOURCE_TABLE),
        _safe_str(TARGET_TABLE),
        total_rows_read,
        total_rows_written,
        total_rows_rejected,
        notebook_start_time,
        notebook_end_time,
        error_message,
        error_message,
        _safe_str(ENVIRONMENT),
        _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID),
        _safe_str(PIPELINE_NAME),
        rerun_count=0
    )

    print("FAILED logged (AnalysisException)")
    raise

except Exception as e:

    error_message = str(e)
    notebook_end_time = datetime.now()

    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID),
        _safe_str(RUN_ID),
        _safe_str(PIPELINE_NAME),
        _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN),
        _safe_str(SOURCE_NAME),
        _safe_str(ACTIVITY_NAME),
        "failed",
        _safe_str(LEVEL),
        _safe_str(LAYER),
        _safe_str(SOURCE_TABLE),
        _safe_str(TARGET_TABLE),
        total_rows_read,
        total_rows_written,
        total_rows_rejected,
        notebook_start_time,
        notebook_end_time,
        error_message,
        error_message,
        _safe_str(ENVIRONMENT),
        _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID),
        _safe_str(PIPELINE_NAME),
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

batch_results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Diagnostics: check landing Delta path, table registration, and row counts
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

print("Landing lakehouse :", landing_lakehouse_name)
print("Landing schema    :", landing_schema_name)
print("Landing table     :", landing_table_name)

landing_abfss_path = abfss_path_builder(
    landing_workspace_id,
    landing_lakehouse_id,
    landing_schema_name,
    landing_table_name,
)
print("Landing Delta path:", landing_abfss_path)

print("\n-- Metastore tables in landing schema --")
try:
    spark.sql(f"SHOW TABLES IN {landing_lakehouse_name}.{landing_schema_name}").show(truncate=False)
except Exception as e:
    print("SHOW TABLES failed:", e)

full_table_name = f"{landing_lakehouse_name}.{landing_schema_name}.{landing_table_name}"
print("\n-- Count via TABLE name --")
try:
    df_tbl = spark.read.table(full_table_name)
    df_tbl.select(F.count("*").alias("row_count")).show()
except AnalysisException as e:
    print("TABLE read failed (AnalysisException):", e)
except Exception as e:
    print("TABLE read failed:", e)

print("\n-- Count via Delta PATH --")
try:
    df_path = spark.read.format("delta").load(landing_abfss_path)
    df_path.select(F.count("*").alias("row_count")).show()
    print("Sample rows from path:")
    display(df_path.limit(20))
except Exception as e:
    print("PATH read failed:", e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
