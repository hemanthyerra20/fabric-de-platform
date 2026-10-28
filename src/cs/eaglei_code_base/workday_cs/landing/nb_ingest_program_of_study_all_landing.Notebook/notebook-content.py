# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5bd544a5-340c-493c-aa88-61dc1992c614",
# META       "default_lakehouse_name": "lh_cs_landing",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "5bd544a5-340c-493c-aa88-61dc1992c614"
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

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

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
ENVIRONMENT = "dev"
TABLENAME="Program_Of_Study_all"
SERVICE_NAME = "ras_report"
OPERATION_NAME =  "Get_Program_Of_Study_all"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_landing"
job_name = "nb_ingest_program_of_study_all"
source_name = "workday"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"
wd_filter_table_name = "workday_extraction_filters"
service_name = "Ras_Report"
operation_name = "Get_Program_Of_Study_all"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
wd_filter_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, wd_filter_table_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)


workspace_id = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store") #"f2aea860-156d-4e18-b078-1356be7161de"
lakehouse_id = get_lakehouse_id(var_cs_lib_name, lh_name)#"5bd544a5-340c-493c-aa88-61dc1992c614"
print(workspace_id,lakehouse_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_df = spark.read.format("delta").load(config_abfss_path)
wd_filter_df = spark.read.format("delta").load(wd_filter_path)\
                    .filter(F.col("table_name") == "program_of_study_all")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

token_url = get_param_value(config_df,source_name,"token_url")
kv_url = get_param_value(config_df,source_name,"kv_url")
grant_type = get_param_value(config_df,source_name,"grant_type")

kv_client_key_name = get_param_value(config_df,source_name,"kv_client_key_name")
kv_secret_key_name = get_param_value(config_df,source_name,"kv_secret_key_name")
kv_std_refreshtoken_name = get_param_value(config_df,source_name,"kv_std_refreshtoken_name")


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
print(landing_workspace_id,landing_lakehouse_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col
import requests
import json
from datetime import datetime

# 1. Get distinct WIDs from df.
wid_list = []
if wd_filter_df.count() > 0:
    for row in wd_filter_df.select("workday_id").distinct().collect():
        wid_list.append(row.workday_id)
    
print(f"Total distinct WIDs to process: {len(wid_list)}")

base_url = "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_Eagle_1_Program_of_study_All_records_RaaS"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}


def fetch_pos_for_wid(wid: str):
    """Call Workday API for a single WID and return payload.

    Returns a tuple: (status, wid, payload_or_message)
    status in {"SUCCESS", "NO_DATA", "FAILED"}.
    """
    url = f"{base_url}?Status!WID={wid}&format=json"
 
    try:
        resp = requests.get(url, headers=headers, timeout=300)
        resp.raise_for_status()
        data = resp.json()

        payload = data.get("Report_Entry") or []
        if not payload:
            # No rows for this WID
            return ("NO_DATA", wid, "No Report_Entry returned")
            
        return ("SUCCESS", wid, data)
    except Exception as e:
        # Capture failure with error message
        return ("FAILED", wid, str(e))


# 2. Call API in parallel and write each successful WID directly to Lakehouse
no_data_wids = []   # you can switch these to just counters if they get large
failed_wids = []
success_count = 0

max_workers = min(16, max(1, len(wid_list)))

# Base folder in default Lakehouse Files
base_folder = "Files/workday/program_of_study_all"

root = (
        f"abfss://{workspace_id}"
        f"@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}"
    )

now = datetime.utcnow()

output_folder = (
            f"{root}/Files/workday/"
            f"{service_name.lower()}/"
            f"{operation_name.lower()}/"
            f"{now:%Y/%m/%d}"
        )

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_wid = {executor.submit(fetch_pos_for_wid, wid): wid for wid in wid_list}
    for future in as_completed(future_to_wid):
        status, wid, payload = future.result()

        if status == "SUCCESS":
            # Current UTC timestamp for content and file name (no colons for file safety)
            ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")

            # Write this WID's payload directly as a JSON file, no accumulation in memory
            file_path = f"{output_folder}/{wid}_{ts}.json"
            content = json.dumps(payload, ensure_ascii=False)
            notebookutils.fs.put(file_path, content, True)  # overwrite if exists
            success_count += 1
        elif status == "NO_DATA":
            no_data_wids.append((wid, payload))
        else:  # FAILED
            failed_wids.append((wid, payload))

print(f"Total WIDs processed   : {len(wid_list)}")
print(f"  SUCCESS (files written): {success_count}")
print(f"  NO_DATA (no rows)     : {len(no_data_wids)}")
print(f"  FAILED (errors)       : {len(failed_wids)}")

# print a small sample of failed and no-data WIDs
print("\nSample FAILED WIDs (up to 10):")
for wid, err in failed_wids[:10]:
    print(f"  WID={wid}  ERROR={err}")

print("\nSample NO_DATA WIDs (up to 10):")
for wid, msg in no_data_wids[:10]:
    print(f"  WID={wid}  MSG={msg}")

# Note: We no longer keep all successful payloads in memory.
# If you still need a combined DataFrame/table later, we can build it
# by reading these JSON files back from Lakehouse instead of accumulating
# them in the driver during the API calls.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.format("json")\
                .option("multiline", True)\
                .load(output_folder)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

flatten_df = raw_df.withColumn("Report_Entry", F.explode_outer(F.col("Report_Entry")))\
                .select("Report_Entry.*")

col_converted_df = to_lowercase_columns(flatten_df)

duplicates_removed_df = col_converted_df.drop_duplicates()

audit_column_added_df = add_audit_column(duplicates_removed_df, job_name)

abfss_path = abfss_path_builder(workspace_id, lakehouse_id, "workday_cs", "program_of_study_all")


audit_column_added_df.write.format("delta")\
        .mode("append")\
        .option("mergeSchema", True)\
        .save(abfss_path)
            

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
