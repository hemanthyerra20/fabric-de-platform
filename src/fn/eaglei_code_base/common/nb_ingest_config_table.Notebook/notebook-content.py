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

var_cs_lib_name = "var_fn_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workday_fn_config_dict = {
    "token_url": "https://services1.myworkday.com/ccx/oauth2/nwtc/token",
    "grant_type": "refresh_token",
    "kv_url": "https://az1-prod-eaglei-dev-kv.vault.azure.net",
    "api_url": "https://services1.myworkday.com/ccx/service/nwtc/${place_holder}/v46.0",
    "source_name": "workday",
    "kv_client_key_name": "eaglei-workday-client",
    "kv_secret_key_name": "eaglei-workday-secret",
    "kv_std_refreshtoken_name": "eaglei-workday-finance-refreshtoken",
    "tenant_name": "nwtc",
    "version": "v46.1",
    "root_path_suffix": "/Files/workday",
    "landing_schema_name": "workday_fn",
    "landing_lakehouse_name": "lh_fn_landing",
    "bronze_schema_name": "workday_fn",
    "bronze_lakehouse_name": "lh_fn_bronze",
    "silver_schema_name": "workday_fn",
    "silver_lakehouse_name": "lh_fn_silver",
   "financial_management":{
      "get_cost_centers":{
         "tag_name":"Response_Data.Cost_Center",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"cost_center",
         "bronze_table_name":"cost_center"
      },
      "get_fund_types":{
         "tag_name":"Response_Data.Fund_Type",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"fund_type",
         "bronze_table_name":"fund_type"
      },
      "get_ledger_account_summaries":{
         "tag_name":"Response_Data.Ledger_Account_Summary",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"ledger_account_summary",
         "bronze_table_name":"ledger_account_summary"
      },
      "get_spend_category_hierarchies":{
         "tag_name":"Response_Data.Spend_Category_Hierarchy",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"spend_category_hierarchy",
         "bronze_table_name":"spend_category_hierarchy"
      },
      "get_fiscal_years":{
         "tag_name":"Response_Data.Fiscal_Year",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"fiscal_year",
         "bronze_table_name":"fiscal_year"
      },
      "get_custom_worktags":{
         "tag_name":"Response_Data.Custom_Worktag",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"custom_worktag",
         "bronze_table_name":"custom_worktag"
      },
      "get_revenue_categories":{
         "tag_name":"Response_Data.Revenue_Category",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"revenue_category",
         "bronze_table_name":"revenue_category"
      },
      "get_journals":{
         "tag_name":"Body.Get_Journals_Response.Response_Data.Journal_Entry",
         "count_tag_name":"Body.Get_Journals_Response.Response_Results.Total_Results",
         "landing_table_name":"journal_entry",
         "bronze_table_name":"journal_entry"
      },
      "get_organizations":{
         "tag_name":"Response_Data.Organization",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"organization",
         "bronze_table_name":"organization"
      },
      "get_funds":{
         "tag_name":"Response_Data.Fund",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"fund",
         "bronze_table_name":"fund"
      },
      "get_programs":{
         "tag_name":"Response_Data.Program",
         "count_tag_name":"Response_Results.Total_Results",
         "landing_table_name":"program",
         "bronze_table_name":"program"
      }
   },
    }

# Serialize to compact JSON string for param_value
workday_param_value_str = json.dumps(workday_fn_config_dict, separators=(",",":"))


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
    domain="fn",
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
