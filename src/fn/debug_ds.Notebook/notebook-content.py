# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aefc7ed1-adb6-446e-a24f-6b34123e366d",
# META       "default_lakehouse_name": "lh_fn_gold",
# META       "default_lakehouse_workspace_id": "bedb4898-64aa-4f9d-b9de-e0c4b634ee27",
# META       "known_lakehouses": [
# META         {
# META           "id": "aefc7ed1-adb6-446e-a24f-6b34123e366d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

print(True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select 
# MAGIC J.journal_line_key,
# MAGIC J.journal_key,
# MAGIC J.fund_key,
# MAGIC J.organization_key,
# MAGIC J.program_key,
# MAGIC J.revenue_category_key,
# MAGIC J.spend_category_hierarchy_key,
# MAGIC J.journal_number,
# MAGIC J.journal_sequence_number,
# MAGIC J.journal_entry_status_id,
# MAGIC J.accounting_date,
# MAGIC J.transaction_date,
# MAGIC J.budget_date,
# MAGIC J.book_code_id,
# MAGIC J.accounting_journal_id,
# MAGIC J.journal_line_number,
# MAGIC J.credit_amount,
# MAGIC J.debit_amount,
# MAGIC J.ledger_credit_amount,
# MAGIC J.ledger_debit_amount,
# MAGIC J.currency_rate,
# MAGIC J.exclude_from_spend_report,
# MAGIC J.line_order,
# MAGIC J.memo,
# MAGIC J.currency_id,
# MAGIC J.currency_numeric_code,
# MAGIC J.company_reference_id,
# MAGIC J.ledger_account_parent_id,
# MAGIC J.ledger_account_parent_type,
# MAGIC J.ledger_account_id,
# MAGIC J.cost_center_reference_id,
# MAGIC J.deduction_code,
# MAGIC J.earning_code,
# MAGIC J.wd_creation_ts,
# MAGIC J.wd_last_updated_ts,
# MAGIC F.fund_id,
# MAGIC F.fund_name,
# MAGIC F.fund_is_inactive,
# MAGIC F.include_fund_id_in_name,
# MAGIC F.fund_type_id,
# MAGIC O.name AS organization_name,
# MAGIC O.description,
# MAGIC O.organization_code,
# MAGIC O.organization_subtype_id,
# MAGIC O.organization_type_id,
# MAGIC O.company_reference_id,
# MAGIC O.custom_organization_reference_id,
# MAGIC O.organization_reference_id,
# MAGIC P.id AS program_id,
# MAGIC P.name AS program_name,
# MAGIC P.program_is_inactive,
# MAGIC R.revenue_category_id,
# MAGIC R.revenue_category_name,
# MAGIC R.revenue_category_inactive,
# MAGIC S.spend_category_hierarchy_id,
# MAGIC S.resource_category_hierarchy_id,
# MAGIC S.spend_category_id,
# MAGIC S.spend_category_hierarchy_names,
# MAGIC S.commodity_detail,
# MAGIC S.description AS spend_category_description,
# MAGIC S.inactive AS spend_category_inactive
# MAGIC from workday_fn.fact_journal J
# MAGIC left join workday_fn.dim_fund F
# MAGIC     on J.fund_key = F.fund_key
# MAGIC left join workday_fn.dim_organization O
# MAGIC     on J.organization_key = O.organization_key
# MAGIC left join workday_fn.dim_program P
# MAGIC     on P.program_key = J.program_key
# MAGIC left join workday_fn.dim_revenue_category R
# MAGIC     on J.revenue_category_key = R.revenue_category_key
# MAGIC left join workday_fn.dim_spend_category_hierarchy S
# MAGIC     on J.spend_category_hierarchy_key = S.spend_category_hierarchy_key
# MAGIC order by journal_line_key

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql(
    """     
        
select 
J.journal_line_key,
J.journal_key,
J.fund_key,
J.organization_key,
J.program_key,
J.revenue_category_key,
J.spend_category_hierarchy_key,
J.journal_number,
J.journal_sequence_number,
J.journal_entry_status_id,
J.accounting_date,
J.transaction_date,
J.budget_date,
J.book_code_id,
J.accounting_journal_id,
J.journal_line_number,
J.credit_amount,
J.debit_amount,
J.ledger_credit_amount,
J.ledger_debit_amount,
J.currency_rate,
J.exclude_from_spend_report,
J.line_order,
J.memo,
J.currency_id,
J.currency_numeric_code,
J.company_reference_id,
J.ledger_account_parent_id,
J.ledger_account_parent_type,
J.ledger_account_id,
J.cost_center_reference_id,
J.deduction_code,
J.earning_code,
J.wd_creation_ts,
J.wd_last_updated_ts,
F.fund_id,
F.fund_name,
F.fund_is_inactive,
F.include_fund_id_in_name,
F.fund_type_id,
O.name AS organization_name,
O.description,
O.organization_code,
O.organization_subtype_id,
O.organization_type_id,
O.custom_organization_reference_id,
O.organization_reference_id,
P.id AS program_id,
P.name AS program_name,
P.program_is_inactive,
R.revenue_category_id,
R.revenue_category_name,
R.revenue_category_inactive,
S.spend_category_hierarchy_id,
S.resource_category_hierarchy_id,
S.spend_category_id,
S.spend_category_hierarchy_names,
S.commodity_detail,
S.description AS spend_category_description,
S.inactive AS spend_category_inactive
from workday_fn.fact_journal J
left join workday_fn.dim_fund F
    on J.fund_key = F.fund_key
left join workday_fn.dim_organization O
    on J.organization_key = O.organization_key
left join workday_fn.dim_program P
    on P.program_key = J.program_key
left join workday_fn.dim_revenue_category R
    on J.revenue_category_key = R.revenue_category_key
left join workday_fn.dim_spend_category_hierarchy S
    on J.spend_category_hierarchy_key = S.spend_category_hierarchy_key
order by journal_line_key
    """
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.coalesce(1).write.format("csv")\
    .option("header", True)\
    .save("abfss://bedb4898-64aa-4f9d-b9de-e0c4b634ee27@onelake.dfs.fabric.microsoft.com/aefc7ed1-adb6-446e-a24f-6b34123e366d/Files/finance")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
