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

var_fn_lib_name = "var_fn_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "pipeline_config_param"

config_workspace_id = get_variable(var_fn_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_fn_lib_name, audit_lakehouse_name)


config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

table_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("source", StringType(), True),
    StructField("domain", StringType(), True),
    StructField("Service", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("active", StringType(), True),
    StructField("landing_notebook", StringType(), True),
    StructField("bronze_notebook", StringType(), True),
    StructField("silver_notebook", StringType(), True),
    StructField("created_on", StringType(), True),
    StructField("created_by", StringType(), True),
    StructField("modified_on", StringType(), True),
    StructField("modified_by", StringType(), True),
])

now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

default_active = "1"
default_landing_notebook = "nb_ingest_landing_layer"
default_bronze_notebook = "nb_ingest_bronze_layer"
default_created_on = now_str
default_created_by = "nb_ingest_pipeline_param_config_table"
default_modified_on = now_str
default_modified_by = "nb_ingest_pipeline_param_config_table"

rows = [
    Row("workday", "fn", "Financial_Management", "Get_Cost_Centers", default_active, default_landing_notebook, default_bronze_notebook, "nb_cost_center", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Custom_Worktags", default_active, default_landing_notebook, default_bronze_notebook, "nb_custom_worktag", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Fiscal_Years", default_active, default_landing_notebook, default_bronze_notebook, "nb_fiscal_year", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Fund_Types", default_active, default_landing_notebook, default_bronze_notebook, "nb_fund_type", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Funds", default_active, default_landing_notebook, default_bronze_notebook, "nb_fund", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Journals", default_active, "nb_ingest_landing_journals", default_bronze_notebook, "nb_journal", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Ledger_Account_Summaries", default_active, default_landing_notebook, default_bronze_notebook, "nb_ledger_account_summary", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Organizations", default_active, default_landing_notebook, default_bronze_notebook, "nb_organization", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Programs", default_active, default_landing_notebook, default_bronze_notebook, "nb_program", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Revenue_Categories", default_active, default_landing_notebook, default_bronze_notebook, "nb_revenue_category", default_created_on, default_created_by, default_modified_on, default_modified_by),
    Row("workday", "fn", "Financial_Management", "Get_Spend_Category_Hierarchies", default_active, default_landing_notebook, default_bronze_notebook, "nb_spend_category_hierarchy", default_created_on, default_created_by, default_modified_on, default_modified_by),
]

df_source_services = spark.createDataFrame(rows, schema)

display(df_source_services)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_delta_tbl(
        source_df=df_source_services,
        target_table_path=table_path,
        keys=["source", "domain", "Service", "operation"]
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Successfully inserted/updated the pipeline config param details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
