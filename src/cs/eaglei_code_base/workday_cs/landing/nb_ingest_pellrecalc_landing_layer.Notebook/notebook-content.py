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


from pyspark.sql.types import StructType, StructField, StringType

academic_schema = StructType([
    StructField("Academic_Period_End_Date",   StringType(), True),
    StructField("Academic_Period_Start_Date", StringType(), True),
    StructField("Academic_Year",              StringType(), True),
    StructField("Pell_Recalculation_Date",    StringType(), True),
    StructField("academicPeriod",             StringType(), True),
])

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
TABLENAME="PELL_RECALC_DATE"
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
job_name = "nb_ingest_pellrecalc_landing_layer"
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
config_df = spark.read.format("delta").load(config_abfss_path)

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

# print(access_token)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col

df = (spark.sql("SELECT apr.reference_id_reference FROM lh_cs_landing.workday_cs.academic_period_references apr")
.withColumn("Academic_Period_ID", col("reference_id_reference.Academic_Period_ID"))
.withColumn("WID",                col("reference_id_reference.WID"))
)
# display(df)        



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col
import requests

# 1. Get distinct WIDs from df (parsed academic_period_references DataFrame)
wid_list = [r["WID"] for r in df.select("WID").distinct().collect()]
print(f"Total distinct WIDs to process: {len(wid_list)}")

base_url = "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_Student_DEV/CR_EAGLE3_STUDENT_PELL_RECALCUATION"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}


def fetch_pell_for_wid(wid: str):
    """Call Workday API for a single WID and return first record mapped to academic_schema.

    Returns a tuple: (status, wid, payload_dict_or_message)
    status in {"SUCCESS", "NO_DATA", "FAILED"}.
    """
    url = f"{base_url}?academicPeriod!WID={wid}&format=json"
    try:
        resp = requests.get(url, headers=headers, timeout=300)
        resp.raise_for_status()
        data = resp.json()

        entries = data.get("Report_Entry") or []
        if not entries:
            # No rows for this WID
            return ("NO_DATA", wid, "No Report_Entry returned")

        first = entries[0]
        payload = {
            "Academic_Period_End_Date":   first.get("Academic_Period_End_Date"),
            "Academic_Period_Start_Date": first.get("Academic_Period_Start_Date"),
            "Academic_Year":              first.get("Academic_Year"),
            "Pell_Recalculation_Date":    first.get("Pell_Recalculation_Date"),
            "academicPeriod":             first.get("academicPeriod"),
        }
        return ("SUCCESS", wid, payload)
    except Exception as e:
        # Capture failure with error message
        return ("FAILED", wid, str(e))


# 2. Call API in parallel 
success_records = []
no_data_wids = []
failed_wids = []

max_workers = min(16, max(1, len(wid_list)))

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_wid = {executor.submit(fetch_pell_for_wid, wid): wid for wid in wid_list}
    for future in as_completed(future_to_wid):
        status, wid, payload = future.result()

        if status == "SUCCESS":
            success_records.append(payload)
        elif status == "NO_DATA":
            no_data_wids.append((wid, payload))
        else:  # FAILED
            failed_wids.append((wid, payload))

print(f"Total WIDs processed   : {len(wid_list)}")
print(f"  SUCCESS               : {len(success_records)}")
print(f"  NO_DATA (no rows)     : {len(no_data_wids)}")
print(f"  FAILED (errors)       : {len(failed_wids)}")

# print a small sample of failed and no-data WIDs
print("\nSample FAILED WIDs (up to 10):")
for wid, err in failed_wids[:10]:
    print(f"  WID={wid}  ERROR={err}")

print("\nSample NO_DATA WIDs (up to 10):")
for wid, msg in no_data_wids[:10]:
    print(f"  WID={wid}  MSG={msg}")

# 3. Create DataFrame from successful records and write to table
if success_records:
    df_pell_all = spark.createDataFrame(success_records, schema=academic_schema)
    display(df_pell_all.limit(10))

    df_pell_all.write.mode("overwrite").saveAsTable("lh_cs_landing.workday_cs.academic_PELL_RECALC_DATE")
    print("\nData written to table lh_cs_landing.workday_cs.academic_PELL_RECALC_DATE")
else:
    print("\nNo successful records fetched; table not updated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
