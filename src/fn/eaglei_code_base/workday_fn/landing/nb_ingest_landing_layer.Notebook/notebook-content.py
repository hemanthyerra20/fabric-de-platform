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
# META       "environmentId": "d6e9c4b7-fdc8-4966-afb3-443827650df9",
# META       "workspaceId": "088bba09-dd86-49a0-87f3-ad9331892945"
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_ID = ""
PIPELINE_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = ""
LEVEL=""
LAYER=""
SERVICE_NAME = "Financial_Management"
OPERATION_NAME = "Get_Journals" #'Get_Cost_Centers'
ENVIRONMENT = ""
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MASTER_RUN_ID = "test123"
# RUN_ID = "test123"
# PIPELINE_ID = "test123"
# PIPELINE_NAME = "ingest_landing_layer"
# DOMAIN = "fn"
# SOURCE_NAME = "workday"
# ACTIVITY_NAME = "notebook activity"
# LEVEL="notebook"
# LAYER="landing"
# SERVICE_NAME = 'Financial_Management'
# OPERATION_NAME = 'Get_Journals' #'Get_Cost_Centers'
# ENVIRONMENT = "dev"
# WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
# WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
# NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_fn_landing"
job_name = "nb_ingest_landing_layer"
source_name = "workday"
var_cs_lib_name = "var_fn_process"
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
# print(parent_object)
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

if overwrite_soap_body:
    soap_body = overwrite_soap_body
elif response_body_block:
    soap_body = f"""
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:bsvc="urn:com.workday/bsvc">
                <soapenv:Body>
                    <bsvc:{OPERATION_NAME}_Request xmlns:bsvc="urn:com.workday/bsvc">
                    <bsvc:Response_Filter>
                    
                            <bsvc:Page>1</bsvc:Page>
                            <!--Optional:-->
                            <bsvc:Count>100</bsvc:Count>
                        </bsvc:Response_Filter>
                        {response_body_block}
            
            </bsvc:{OPERATION_NAME}_Request>
                </soapenv:Body>
            </soapenv:Envelope>
    """
else:
    soap_body = None

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

# Ingest data from Workday API into landing layer
# This function orchestrates API extraction, raw JSON load, parsing, auditing, and Delta write.

def ingest_landing_layer(service_name: str, operation_name: str, job_name: str):
    """Ingest data from Workday API into the landing layer.

    Args:
        SERVICE_NAME: Name of the Workday service (e.g., "Student_Records").
        OPERATION_NAME: Name of the Workday operation (e.g., "Get_Student_Course_Tag_Categories").
        job_name: Job/notebook name for audit column population.

    Returns:
        dict: Summary with source_count, target_count and status.
    """
    try:
        if service_name.lower() == "ras_report":
            
            headers = {
                        "Authorization": f"Bearer {access_token}",
                        "Content-Type": "application/json"
                    }


            output_file = ingest_api_to_lakehouse(
                            url=api_url,
                            headers= headers,
                            service_name= service_name.lower(),
                            operation_name=  operation_name.lower(),
                            workspace_id= landing_workspace_id,
                            lakehouse_id= landing_lakehouse_id
                        )

            print(output_file)
        else:
            # =====================================================
            # 1. Extract data from Workday API and persist raw JSON pages
            # =====================================================
            extract_workday_api_pages(
                api_url=api_url,
                operation_name=operation_name,
                service_name=service_name,
                count_per_page=200,   # will be enforced in the SOAP body each page
                access_token=access_token,
                workspace_id=landing_workspace_id,
                lakehouse_id=landing_lakehouse_id,
                soap_body=soap_body,
            )

        # =====================================================
        # 2. Load raw JSON response files into a Spark DataFrame
        # =====================================================
        raw_df = (
            spark.read.format("json")
            .option("multiline", True)
            .option("mode", "FAILFAST")
            .option("maxStringLen", 50000000)
            .load(landing_folder_path)
        )

        # =====================================================
        # 3. Derive source record count from Response_Results
        # =====================================================
        if count_tag_name == "NA":
            src_count = raw_df.count()
        else:
            src_count = get_total_results(raw_df)

        # =====================================================
        # 4. Parse Response_Data -> parent tag -> child records
        # =====================================================
        parsed_df = parse_parent_tag(raw_df)


        # =====================================================
        # 5. Add audit and technical columns
        # =====================================================
        col_converted_df = to_lowercase_columns(parsed_df)
        added_audit_col = add_audit_column(col_converted_df, job_name)
        added_id_col = add_uuid_column(added_audit_col, "id")
        add_file_name_df = added_id_col.withColumn("file_name", element_at(split(input_file_name(), "/"), -1))

        # Trigger action to compute final target count
        tgt_count = add_file_name_df.count()

        # =====================================================
        # 6. Persist curated landing data as Delta in Lakehouse
        # =====================================================
        landing_abfss_path = abfss_path_builder(
            landing_workspace_id,
            landing_lakehouse_id,
            landing_schema_name,
            landing_table_name,
        )

        (
            add_file_name_df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", True)
            .save(landing_abfss_path)
        )

        return {
            "source_count": src_count,
            "target_count": tgt_count,
            "status": "success",
        }

    except Exception as e:
        # Log the exception details to driver logs for troubleshooting
        try:
            mssparkutils.logging.logError(
                f"Error in ingest_landing_layer for service={service_name}, operation={operation_name}: {str(e)}"
            )
            print(str(e))
        except Exception:
            # Fallback logging if mssparkutils logging is unavailable
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

SOURCE_TABLE=f"{SERVICE_NAME.lower()}.{OPERATION_NAME.lower()}"
TARGET_TABLE= f"{landing_lakehouse_name}.{landing_schema_name}.{landing_table_name}"

start_time = datetime.now()
end_time = datetime.now()
rows_read = 0
rows_written = 0
rows_rejected = 0
try:
    # ==========================================
    # BUSINESS LOGIC (replace with real work)
    # ==========================================

    print("Executing business logic...")

    result = ingest_landing_layer(SERVICE_NAME, OPERATION_NAME, RUN_ID)

    print("business logic completed...")

    print(result)
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

# CELL ********************

result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
