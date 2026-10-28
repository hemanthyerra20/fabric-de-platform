# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "d6e9c4b7-fdc8-4966-afb3-443827650df9",
# META       "workspaceId": "088bba09-dd86-49a0-87f3-ad9331892945"
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

import uuid
import json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_hr_lib_name = "var_hr_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"


config_workspace_id = get_variable(var_hr_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_hr_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workday_hr_config_dict = {
    "token_url": "https://services1.myworkday.com/ccx/oauth2/nwtc/token",
    "grant_type": "refresh_token",
    "kv_url": "https://az1-prod-eaglei-dev-kv.vault.azure.net",
    "api_url": "https://services1.myworkday.com/ccx/service/nwtc/${place_holder}/v46.0",
    "source_name": "workday",
    "kv_client_key_name": "eaglei-workday-client",
    "kv_secret_key_name": "eaglei-workday-secret",
    "kv_std_refreshtoken_name": "eaglei-workday-hcm-refreshtoken",
    "tenant_name": "nwtc",
    "version": "v46.1",
    "root_path_suffix": "/Files/workday",
    "landing_schema_name": "workday_hr",
    "landing_lakehouse_name": "lh_hr_landing",
    "bronze_schema_name": "workday_hr",
    "bronze_lakehouse_name": "lh_hr_bronze",
    "silver_schema_name": "workday_hr",
    "silver_lakehouse_name": "lh_hr_silver",
    "ras_report" : {
         "get_faculty_pay_fca" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_HCM_DEV/CR_Fabric_Faculty_Pay_FCA?format=json",
            "landing_table_name": "faculty_pay_fca",
            "bronze_table_name": "faculty_pay_fca",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_faculty_pay_npa" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_HCM_DEV/CR_Fabric_Faculty_Pay_NPA?format=json",
            "landing_table_name": "faculty_pay_npa",
            "bronze_table_name": "faculty_pay_npa",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        },
        "get_faculty_workload_costing" : {
            "api_url" : "https://services1.myworkday.com/ccx/service/customreport2/nwtc/ISU_Eagle_HCM_DEV/CR_Fabric_Faculty_Workload_Costing?format=json",
            "landing_table_name": "faculty_workload_costing",
            "bronze_table_name": "faculty_workload_costing",
            "count_tag_name": "NA",
            "tag_name": "Report_Entry"
        }
    },
    "human_resources":{
         "get_academic_appointee":{
            "tag_name":"Response_Data.Academic_Appointee",
            "count_tag_name":"Response_Results.Total_Results",
            "landing_table_name":"academic_appointee",
            "bronze_table_name":"academic_appointee"
         },
         "get_academic_units":{
            "tag_name":"Response_Data.Academic_Unit",
            "count_tag_name":"Response_Results.Total_Results",
            "landing_table_name":"academic_unit",
            "bronze_table_name":"academic_unit"
         },
         "get_job_families":{
            "tag_name":"Response_Data.Job_Family",
            "count_tag_name":"Response_Results.Total_Results",
            "landing_table_name":"job_family",
            "bronze_table_name":"job_family"
         },
         "get_job_profiles":{
            "tag_name":"Response_Data.Job_Profile",
            "count_tag_name":"Response_Results.Total_Results",
            "landing_table_name":"job_profile",
            "bronze_table_name":"job_profile"
         },
         "get_organizations":{
            "tag_name":"Response_Data.Organization",
            "count_tag_name":"Response_Results.Total_Results",
            "landing_table_name":"organization",
            "bronze_table_name":"organization"
         },
         "get_workers":{
            "tag_name":"Response_Data.Worker",
            "count_tag_name":"Response_Results.Total_Results",
            "landing_table_name":"worker",
            "bronze_table_name":"worker"
         }
         
      },
      "compensation":{
         "get_compensation_plans":{
            "tag_name":"Response_Data.Compensation_Plan",
            "count_tag_name":"Response_Results.Total_Results",
            "landing_table_name":"compensation_plan",
            "bronze_table_name":"compensation_plan"
         }

      }
    }

# Serialize to compact JSON string for param_value
workday_param_value_str = json.dumps(workday_hr_config_dict, separators=(",",":"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ------------------------------------------------------------------
# 2. Define schema
# ------------------------------------------------------------------
schema = StructType([
    StructField("id", StringType(), True),
    StructField("source", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("param_values", StringType(), True),
])

# ------------------------------------------------------------------
# 3. Create a single-row DataFrame
# ------------------------------------------------------------------
rows = [Row(
    id=str(uuid.uuid4()),
    source="workday",
    domain="hr",
    param_values=workday_param_value_str,
),
]

df_config = spark.createDataFrame(rows, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Staged data (new/changed records)
staged_df = df_config

try:
    delta_table = DeltaTable.forPath(spark, config_abfss_path)
except AnalysisException:
    # Table/path does not exist yet – create it from the staged data
    (
        staged_df
        .write
        .format("delta")
        .mode("overwrite")
        .save(config_abfss_path)
    )
    delta_table = DeltaTable.forPath(spark, config_abfss_path)

# Only treat as "changed" when any of these columns differ
change_condition = (
    "tgt.param_values <> src.param_values"
)

(
    delta_table.alias("tgt")
    .merge(
        staged_df.alias("src"),
        "tgt.source = src.source AND tgt.domain = src.domain"   # join condition (business key)
    )
    .whenMatchedUpdate(
        condition=change_condition,
        set={
            "param_values": "src.param_values",
        },
    )
    .whenNotMatchedInsert(
        values={
            "id": "src.id",
            "source": "src.source",
            "domain": "src.domain",
            "param_values": "src.param_values",
        },
    )
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Successfully inserted/updated the config details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
