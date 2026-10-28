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
# META           "id": "5bd544a5-340c-493c-aa88-61dc1992c614"
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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_cs_lib_name = "var_fn_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "xpath_details"
notebook_name = notebookutils.runtime.context["currentNotebookName"]


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def workday_cs_generate_data(table_name: str, xpath: dict, primary_key: str,explode_and_pivot_required: dict | None = None, partition_by: str | None = None):
    """Generate configuration rows for a given table/xpath mapping.

    For each (alias_name, path) pair in the `xpath` mapping, this function
    builds a tuple containing:
      - a shared UUID `id` (same for all rows in this batch)
      - static `source` and `domain` values
      - the provided `table_name`
      - the alias name and xpath
      - a JSON string describing explode/pivot details (if configured)

    Args:
        table_name (str): Target logical table name (e.g., bronze table).
        xpath (dict): Mapping of alias_name -> xpath string.
        explode_and_pivot_required (dict | None): Optional mapping where the
            key is alias_name and the value is a dict describing which columns
            should be excluded from explode/pivot or any other required
            metadata.

    Returns:
        list[tuple]: List of configuration tuples ready to be turned into a
        DataFrame.
    """

    # Guard against None to avoid AttributeError when accessing .keys()
    if explode_and_pivot_required is None:
        explode_and_pivot_required = {}

    # Generate a single UUID for this batch so all related rows share the same id
    batch_id = str(uuid.uuid4())
    source = "workday"
    domain = "fn"

    data_lst = []

    try:
        # Precompute a case-insensitive set of keys from explode_and_pivot_required
        explode_keys_lower = {k.lower() for k in explode_and_pivot_required.keys()}

        for alias_name, path in xpath.items():
            # Determine if this alias has explode/pivot metadata configured
            if alias_name.lower() in explode_keys_lower:
                # Use the original key lookup (case-sensitive) to fetch details
                # We assume the exact alias_name exists; if not, this will
                # raise a KeyError, which we allow to propagate.
                explode_details = {
                    alias_name: explode_and_pivot_required[alias_name]
                }

                # Serialize explode/pivot configuration to a compact JSON string
                explode_details_str = json.dumps(explode_details, separators=(",", ":"))
            else:
                explode_details_str = None

            # Append a configuration tuple for this alias/xpath pair
            data_lst.append(
                (batch_id, source, domain, table_name, alias_name, path, explode_details_str, primary_key, partition_by)
            )

        return data_lst

    except Exception:
        # Re-raise the original exception so the caller keeps full type and
        # traceback. This allows upstream orchestration code to decide how
        # to handle configuration or schema issues.
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

cost_center_xpath = {
    "cost_center_reference": "Cost_Center_Reference",
    "simple_cost_center_data": "Simple_Cost_Center_Data",
    "restricted_to_company_or_company_hierarchy_reference": "Cost_Center_Data>Restricted_To_Company_or_Company_Hierarchy_Reference",
    "organization_id": "Cost_Center_Data>Organization_Data>ID",
    "include_organization_id_in_name": "Cost_Center_Data>Organization_Data>Include_Organization_ID_in_Name",
    "organization_name": "Cost_Center_Data>Organization_Data>Organization_Name",
    "phonetic_name": "Cost_Center_Data>Organization_Data>Phonetic_Name",
    "organization_code": "Cost_Center_Data>Organization_Data>Organization_Code",
    "include_organization_code_in_name": "Cost_Center_Data>Organization_Data>Include_Organization_Code_in_Name",
    "organization_active": "Cost_Center_Data>Organization_Data>Organization_Active",
    "availability_date": "Cost_Center_Data>Organization_Data>Availability_Date",
    "organization_visibility_reference": "Cost_Center_Data>Organization_Data>Organization_Visibility_Reference",
    "external_url_reference": "Cost_Center_Data>Organization_Data>External_URL_Reference",
    "integration_id_data": "Cost_Center_Data>Organization_Data>Integration_ID_Data",
    "organization_subtype_reference": "Cost_Center_Data>Organization_Subtype_Reference",
    "organization_container_reference": "Cost_Center_Data>Organization_Container_Reference",
    "related_worktags_data": "Cost_Center_Data>Related_Worktags_Data",
    "effective_date": "Cost_Center_Data>Effective_Date",
    "modified_on": "modified_on"
}



cost_center_tbl = "lh_fn_bronze.workday_fn.cost_center"

cost_center = workday_cs_generate_data(table_name= cost_center_tbl,
                                    xpath = cost_center_xpath,
                                    primary_key= "organization_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

custom_worktag_path = {
    "custom_worktag_reference": "Custom_Worktag_Reference",
    "custom_worktag_configuration_id": "Custom_Worktag_Data>Custom_Worktag_Configuration_ID",
    "custom_worktag_values_data": "Custom_Worktag_Data>Custom_Worktag_Values_Data",
    "modified_on": "modified_on"
}



custom_worktag_tbl = "lh_fn_bronze.workday_fn.custom_worktag"

custom_worktag = workday_cs_generate_data(table_name= custom_worktag_tbl,
                                    xpath = custom_worktag_path,
                                    primary_key= "custom_worktag_configuration_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fiscal_year_xpath = {
    "fiscal_year_reference": "Fiscal_Year_Reference",
    "fiscal_year_number": "Fiscal_Year_Data>Fiscal_Year_Number",
    "fiscal_schedule_reference": "Fiscal_Year_Data>Fiscal_Schedule_Reference",
    "fiscal_year_name": "Fiscal_Year_Data>Fiscal_Year_Name",
    "fiscal_year_start_date": "Fiscal_Year_Data>Fiscal_Year_Start_Date",
    "fiscal_year_end_date": "Fiscal_Year_Data>Fiscal_Year_End_Date",
    "fiscal_period_data": "Fiscal_Year_Data>Fiscal_Period_Data",
    "modified_on": "modified_on"
}




fiscal_year_tbl = "lh_fn_bronze.workday_fn.fiscal_year"

fiscal_year = workday_cs_generate_data(table_name= fiscal_year_tbl,
                                    xpath = fiscal_year_xpath,
                                    primary_key= "fiscal_year_number")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fund_type_xpath = {
    "fund_type_reference": "Fund_Type_Reference",
    "fund_type_id": "Fund_Type_Data>Fund_Type_ID",
    "fund_type_name": "Fund_Type_Data>Fund_Type_Name",
    "include_fund_type_id_in_name": "Fund_Type_Data>Include_Fund_Type_ID_in_Name",
    "inactive": "Fund_Type_Data>Inactive",
    "fund_restriction_reference": "Fund_Type_Data>Fund_Restriction_Reference",
    "funded_by_reference": "Fund_Type_Data>Funded_By_Reference",
    "modified_on": "modified_on"
}


fund_type_tbl = "lh_fn_bronze.workday_fn.fund_type"

fund_type = workday_cs_generate_data(table_name= fund_type_tbl,
                                    xpath = fund_type_xpath,
                                    primary_key= "fund_type_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

revenue_category_xpath = {
    "revenue_category_reference": "Revenue_Category_Reference",
    "revenue_category_id": "Revenue_Category_Data>Revenue_Category_ID",
    "revenue_category_name": "Revenue_Category_Data>Revenue_Category_Name",
    "tax_applicability_configuration_data": "Revenue_Category_Data>Tax_Applicability_Configuration_Data",
    "classification_type_reference": "Revenue_Category_Data>Classification_Type_Reference",
    "revenue_category_inactive": "Revenue_Category_Data>Revenue_Category_Inactive",
    "maps_to_spend_category_reference": "Revenue_Category_Data>Maps_to_Spend_Category_Reference",
    "related_worktags_data": "Revenue_Category_Data>Related_Worktags_Data",
    "modified_on": "modified_on"
}

revenue_category_tbl = "lh_fn_bronze.workday_fn.revenue_category"

revenue_category = workday_cs_generate_data(table_name= revenue_category_tbl,
                                    xpath = revenue_category_xpath,
                                    primary_key= "revenue_category_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spend_category_hierarchy_xpath = {
    "spend_category_hierarchy_reference": "Spend_Category_Hierarchy_Reference",
    "spend_category_hierarchy_id": "Spend_Category_Hierarchy_Data>Spend_Category_Hierarchy_ID",
    "spend_category_hierarchy_names": "Spend_Category_Hierarchy_Data>Spend_Category_Hierarchy_Names",
    "hierarchy_level_reference": "Spend_Category_Hierarchy_Data>Hierarchy_Level_Reference",
    "parent_reference": "Spend_Category_Hierarchy_Data>Parent_Reference",
    "enable_for_external_website": "Spend_Category_Hierarchy_Data>Enable_for_External_Website",
    "included_spend_categories_reference": "Spend_Category_Hierarchy_Data>Included_Spend_Categories_Reference",
    "commodity_detail": "Spend_Category_Hierarchy_Data>Commodity_Detail",
    "description": "Spend_Category_Hierarchy_Data>Description",
    "inactive": "Spend_Category_Hierarchy_Data>Inactive",
    "spend_category_hierarchy_display_option_reference":"Spend_Category_Hierarchy_Data>Spend_Category_Hierarchy_Display_Option_Reference",
    "modified_on": "modified_on"
}


spend_category_hierarchy_tbl = "lh_fn_bronze.workday_fn.spend_category_hierarchy"

spend_category_hierarchy = workday_cs_generate_data(table_name= spend_category_hierarchy_tbl,
                                    xpath = spend_category_hierarchy_xpath,
                                    primary_key= "spend_category_hierarchy_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ledger_account_summary_xpath = {
    "ledger_account_summary_reference": "Ledger_Account_Summary_Reference",
    "all_ledger_account_data": "All_Ledger_Account_Data",
    "ledger_account_summary_id": "Ledger_Account_Summary_Data>Ledger_Account_Summary_ID",
    "effective_date": "Ledger_Account_Summary_Data>Effective_Date",
    "ledger_account_summary_name": "Ledger_Account_Summary_Data>Ledger_Account_Summary_Name",
    "account_set_reference": "Ledger_Account_Summary_Data>Account_Set_Reference",
    "hierarchy_level_reference": "Ledger_Account_Summary_Data>Hierarchy_Level_Reference",
    "ledger_account_summary_reference_data":"Ledger_Account_Summary_Data>Ledger_Account_Summary_Reference",
    "included_ledger_account_reference":"Ledger_Account_Summary_Data>Included_Ledger_Account_Reference",
    "included_ledger_account_range_data":"Ledger_Account_Summary_Data>Included_Ledger_Account_Range_Data",
    "modified_on": "modified_on"
}


ledger_account_summary_tbl = "lh_fn_bronze.workday_fn.ledger_account_summary"

ledger_account_summary = workday_cs_generate_data(table_name= ledger_account_summary_tbl,
                                    xpath = ledger_account_summary_xpath,
                                    primary_key= "ledger_account_summary_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

journal_xpath = {
    "journal_entry_reference_wid": "journal_entry_reference>WID",
    "journal_number": "journal_number",
    # "journal_sequence_number": "Journal_Entry_Data>Journal_Sequence_Number",
    "journal_entry_status_id": "journal_status_reference>Journal_Entry_Status_ID",
    "organization_reference_id": "company_reference>Organization_Reference_ID",
    "company_reference_id": "company_reference>Company_Reference_ID",
    "currency_id": "currency_reference>Currency_ID",
    "currency_numeric_code": "currency_reference>Currency_Numeric_Code",
    # "book_code_reference": "Journal_Entry_Data>Book_Code_Reference",
    "ledger_reference_id": "ledger_reference>Ledger_Reference_ID",
    # "display_account_set_reference": "Journal_Entry_Data>Display_Account_Set_Reference",
    # "ledger_period_reference": "Journal_Entry_Data>Ledger_Period_Reference",
    "accounting_date": "accounting_date",
    "journal_source_id": "journal_source_reference>Journal_Source_ID",
    # "balancing_worktag_reference": "Journal_Entry_Data>Balancing_Worktag_Reference",
    # "optional_balancing_worktags_reference": "Journal_Entry_Data>Optional_Balancing_Worktags_Reference",
    "transaction_date": "transaction_date",
    "student_charge_id": "operational_transaction_reference>Student_Charge_ID",
    "student_charge_document_id": "operational_transaction_reference>Student_Charge_Document_ID",
    "record_quantity": "record_quantity",
    # "memo": "Journal_Entry_Data>Memo",
    # "external_reference_id": "Journal_Entry_Data>External_Reference_ID",
    # "document_link": "Journal_Entry_Data>Document_Link",
    # "originated_by_reference": "Journal_Entry_Data>Originated_By_Reference",
    # "approved_by_reference": "Journal_Entry_Data>Approved_by_Reference",
    "total_ledger_debits": "total_ledger_debits",
    "total_ledger_credits": "total_ledger_credits",
    # "cancel_reversed_by_journal_entry_reference":"Journal_Entry_Data>Cancel_Reversed_By_Journal_Entry_Reference",
    # "cancel_reverses_journal_entry_reference":"Journal_Entry_Data>Cancel_Reverses_Journal_Entry_Reference",
    # "functional_reversed_by_journal_entry_reference":"Journal_Entry_Data>Functional_Reversed_By_Journal_Entry_Reference",
    # "functional_reverses_journal_entry_reference":"Journal_Entry_Data>Functional_Reverses_Journal_Entry_Reference",
    "creation_date": "creation_date",
    # "last_updated_date": "Journal_Entry_Data>Last_Updated_Date",
    "journal_entry_line_data": "journal_entry_line_data",
    # "attachment_data": "Journal_Entry_Data>Attachment_Data",
    "modified_on": "modified_on"
}

journal_tbl = "lh_fn_bronze.workday_fn.journal_entry"

journal = workday_cs_generate_data(table_name= journal_tbl,
                                    xpath = journal_xpath,
                                    primary_key= "journal_entry_reference_wid")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

custom_worktag_path = {
    "custom_worktag_reference": "Custom_Worktag_Reference",
    "custom_worktag_configuration_id": "Custom_Worktag_Data>Custom_Worktag_Configuration_ID",
    "custom_worktag_values_data": "Custom_Worktag_Data>Custom_Worktag_Values_Data",
    "modified_on": "modified_on"
}



custom_worktag_tbl = "lh_fn_bronze.workday_fn.custom_worktag"

custom_worktag = workday_cs_generate_data(table_name= custom_worktag_tbl,
                                    xpath = custom_worktag_path,
                                    primary_key= "custom_worktag_configuration_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fund_xpath = {
    "fund_reference": "Fund_Reference",
    "fund_data": "Fund_Data",
    "modified_on": "modified_on"
}

fund_ids = {
        "fund_reference": {
                "col_not_req": ["WID"],
                "col_rename": {}
        }
    }


fund_tbl = "lh_fn_bronze.workday_fn.fund"

fund = workday_cs_generate_data(table_name= fund_tbl,
                                    xpath = fund_xpath,
                                    primary_key= "fund_id",
                                    explode_and_pivot_required= fund_ids,)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

organization_xpath = {
    "organization_reference" : "Organization_Reference",
    "name" : "Organization_Data>Name",
    "description" : "Organization_Data>Description",
    "organization_code" : "Organization_Data>Organization_Code",
    "organization_type_reference" : "Organization_Data>Organization_Type_Reference",
    "organization_subtype_reference" : "Organization_Data>Organization_Subtype_Reference",
    "availibility_date" : "Organization_Data>Availibility_Date",
    "last_updated_dateTime" : "Organization_Data>Last_Updated_DateTime",
    "inactive" : "Organization_Data>Inactive",
    "inactive_date" : "Organization_Data>Inactive_Date",
    "manager_reference" : "Organization_Data>Manager_Reference",
    "leadership_reference" : "Organization_Data>Leadership_Reference",
    "modified_on": "modified_on"
}


organization_tbl = "lh_fn_bronze.workday_fn.organization"

organizattion = workday_cs_generate_data(table_name= organization_tbl,
                                    xpath = organization_xpath,
                                    primary_key= "organization_code")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

program_xpath = {
    "id" : "Program_Data>Program_ID",
    "name" : "Program_Data>Program_Name",
    "include_program_id_in_name" : "Program_Data>Include_Program_ID_in_Name",
    "program_is_inactive" : "Program_Data>Program_is_Inactive",
    "modified_on": "modified_on"
}

program_table = "lh_fn_bronze.workday_fn.program"

program = workday_cs_generate_data(table_name= program_table,
                                    xpath = program_xpath,
                                    primary_key= "id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    *cost_center,
    *custom_worktag,
    *fiscal_year,
    *fund_type,
    *revenue_category,
    *spend_category_hierarchy,
    *ledger_account_summary,
    *journal,
    *fund,
    *organizattion,
    *program
]

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
    StructField("table_name", StringType(), True),
    StructField("alias_name", StringType(), True),
    StructField("xpath", StringType(), True),
    StructField("explode_and_pivot_required", StringType(), True),
    StructField("primary_keys", StringType(), True),
    StructField("partition_by", StringType(), True),

])

# ------------------------------------------------------------------
# 3. Create a single-row DataFrame
# ------------------------------------------------------------------

df_config = spark.createDataFrame(data, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_config = df_config.withColumns({
    "created_on":  current_timestamp(),
    "created_by":  lit(notebook_name),         # or your username / job name
    "modified_on": current_timestamp(),
    "modified_by": lit(notebook_name),
})

df_delete = df_config.select("source", "domain", "table_name").distinct()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if table_exists(config_abfss_path):
    # Staged data (new/changed records)
    staged_df = df_config

    # Reference the target Delta table by path
    delta_table = DeltaTable.forPath(spark, config_abfss_path)

    (
        delta_table.alias("tgt")
        .merge(
            df_config.alias("src"),
            "tgt.source = src.source AND tgt.domain = src.domain AND tgt.table_name = src.table_name"   # join condition (business key)
        )
        .whenMatchedDelete()
        .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_config.write.format("delta")\
        .mode("append")\
        .option("mergeSchema", True)\
        .save(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("delta")\
        .load(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df = spark.sql("SELECT * FROM `EAGLEI-CS-STORE-DEV`.lh_cs_landing.workday_cs.isir_transactions_request LIMIT 10")
# display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
