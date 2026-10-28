# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c1ad79e0-8d5c-445d-b093-a187bd3af4c3",
# META       "default_lakehouse_name": "lh_fn_silver",
# META       "default_lakehouse_workspace_id": "bedb4898-64aa-4f9d-b9de-e0c4b634ee27",
# META       "known_lakehouses": [
# META         {
# META           "id": "c1ad79e0-8d5c-445d-b093-a187bd3af4c3"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "d6e9c4b7-fdc8-4966-afb3-443827650df9",
# META       "workspaceId": "088bba09-dd86-49a0-87f3-ad9331892945"
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

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

# PARAMETERS CELL ********************

VAR_FN_LIB_NAME = "var_fn_process"
PIPELINE_NAME = "nb_gold_layer_ingestion"
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = "ManualLoad"
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = ""
LEVEL = ""
LAYER = ""
ENVIRONMENT = ""
WORKSPACE_NAME = ""
WORKSPACE_ID = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_variable(VAR_FN_LIB_NAME, "fn_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_FN_LIB_NAME, "lh_fn_silver_id")
gold_lakehouse_id = get_variable(VAR_FN_LIB_NAME, "lh_fn_gold_id")
silver_schema_name = "workday_fn"
gold_schema_name = "workday_fn"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_journal"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_workspace_id = get_variable(VAR_FN_LIB_NAME, "platform_monitoring_workspace_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    journal_silver_path = abfss_path_builder(workspace_id,silver_lakehouse_id,silver_schema_name,"journal")
    fund_silver_path = abfss_path_builder(workspace_id,gold_lakehouse_id,gold_schema_name,"dim_fund")
    org_silver_path = abfss_path_builder(workspace_id,gold_lakehouse_id,gold_schema_name,"dim_organization")
    prg_silver_path = abfss_path_builder(workspace_id,gold_lakehouse_id,gold_schema_name,"dim_program")
    rev_cat_silver_path = abfss_path_builder(workspace_id,gold_lakehouse_id,gold_schema_name,"dim_revenue_category")
    spend_cat_silver_path = abfss_path_builder(workspace_id,gold_lakehouse_id,gold_schema_name,"dim_spend_category_hierarchy")
    cost_center_silver_path = abfss_path_builder(workspace_id,gold_lakehouse_id,gold_schema_name,"dim_cost_center")
    


    fund_df = spark.read.format("delta").load(fund_silver_path)\
                    .select("fund_key", "fund_id")
    org_df = spark.read.format("delta").load(org_silver_path)\
                    .select("organization_key", "organization_reference_id")
    prg_df = spark.read.format("delta").load(prg_silver_path)\
                    .select("program_key", "id")
    rev_cat_df = spark.read.format("delta").load(rev_cat_silver_path)\
                    .select("revenue_category_key", "revenue_category_id")
    spend_cat_df = spark.read.format("delta").load(spend_cat_silver_path)\
                    .select("spend_category_hierarchy_key", "spend_category_id")
    cc_df = spark.read.format("delta").load(cost_center_silver_path)\
                    .select("cost_center_key", "cost_center_id")


    journal_silver_df = (
        spark.read.format("delta").load(journal_silver_path)
        .select(
            F.col("journal_key"),
            F.col("journal_number"),
            # Cast date-like strings to proper date type
            F.to_date(F.col("accounting_date")).alias("accounting_date"),
            F.to_date(F.col("transaction_date")).alias("transaction_date"),
            # Parse ISO 8601 timestamps with offset into Spark timestamp
            F.to_timestamp(F.col("creation_date")).alias("wd_creation_ts"),
            F.col("journal_entry_status_id"),
        )
    )

    journal_line_silver_path = abfss_path_builder(
            workspace_id,
            silver_lakehouse_id,
            silver_schema_name,
            "journal_line_data"
        )

    journal_line_silver_df = (
        spark.read.format("delta").load(journal_line_silver_path)
        .select(
            F.col("journal_key"),
            F.col("journal_line_key"),
            F.col("journal_line_number").cast("int"),    # e.g. 0
            F.to_date(F.col("budget_date")).alias("budget_date"),  # '2024-06-30' -> date
            F.col("credit_amount").cast("double"),       # numeric
            F.col("debit_amount").cast("double"),        # numeric
            F.col("ledger_credit_amount").cast("double"),
            F.col("ledger_debit_amount").cast("double"),
            F.col("currency_rate").cast("double"),       # numeric 1
            F.col("exclude_from_spend_report").cast("boolean"),  # 'false' -> boolean
            F.col("memo"),                                # string / null
            F.col("currency_id"),                         # 'USD'
            F.col("currency_numeric_code").cast("int"),  # '840' -> int
            F.col("company_reference_id"),  
            F.col("ledger_account_id")                    # '5200'
        )
    )

    joined_df = journal_silver_df.alias("J")\
                    .join(journal_line_silver_df.alias("JL"),
                            ["journal_key"],
                            "inner")

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    
    raise Exception(f"Failed to create/load fact_journal table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    journal_silver_path = abfss_path_builder(workspace_id,silver_lakehouse_id,silver_schema_name,"journal_line_worktag")


    journal_silver_df = spark.read.format("delta").load(journal_silver_path)\
                            .filter(F.col("type").isin(["Revenue_Category_ID","Spend_Category_ID"
                                    ,"Deduction_Code","Earning_Code","Program_ID"
                                    ,"Organization_Reference_ID","Fund_ID","Cost_Center_Reference_ID"]))\
                            .select("journal_line_key", "type", "value")

    journal_worktag_pivot_df = (
        journal_silver_df
        .groupBy("journal_line_key")
        .pivot("type")              # each distinct type becomes a column
        .agg(F.first("value"))      # take first non-null value per type
    )


    worktag_joined_df = joined_df.alias("LH")\
                    .join(journal_worktag_pivot_df.alias("RH"),
                            ["journal_line_key"],
                            "left")

    col_converted_df = to_lowercase_columns(worktag_joined_df)


    dim_joined_df = col_converted_df.alias("MN")\
                        .join(fund_df.alias("FN"),F.col("MN.fund_id") == F.col("FN.fund_id"),"left")\
                        .join(org_df.alias("ORG"),F.col("MN.organization_reference_id") == F.col("ORG.organization_reference_id"),"left")\
                        .join(prg_df.alias("PRG"),F.col("MN.program_id") == F.col("PRG.id"),"left")\
                        .join(rev_cat_df.alias("REV"),F.col("MN.revenue_category_id") == F.col("REV.revenue_category_id"),"left")\
                        .join(spend_cat_df.alias("SPD"),F.col("MN.spend_category_id") == F.col("SPD.spend_category_id"),"left")\
                        .join(cc_df.alias("CC"),F.col("MN.cost_center_reference_id") == F.col("CC.cost_center_id"),"left")\
                        .selectExpr(
                            'MN.journal_line_key',
                            'MN.journal_key',
                            'FN.fund_key',
                            'ORG.organization_key',
                            'PRG.program_key',
                            'REV.revenue_category_key',
                            'SPD.spend_category_hierarchy_key',
                            'CC.cost_center_key',
                            'MN.journal_number',
                            'MN.journal_entry_status_id',
                            'MN.accounting_date',
                            'MN.transaction_date',
                            'MN.budget_date', 
                            'MN.journal_line_number',
                            'MN.credit_amount',
                            'MN.debit_amount',
                            'MN.ledger_credit_amount',
                            'MN.ledger_debit_amount',
                            'MN.currency_rate',
                            'MN.exclude_from_spend_report',
                            'MN.memo',
                            'MN.currency_id',
                            'MN.currency_numeric_code',
                            'MN.company_reference_id',
                            'MN.ledger_account_id',
                            'MN.deduction_code',
                            'MN.earning_code',
                            'MN.wd_creation_ts',
                        )

    # ----------------------------------------------------------
    # Configure datetime rebasing for Parquet/Delta writes
    # This avoids SparkUpgradeException when writing very old dates/timestamps
    # that may exist in accounting_date / transaction_date / budget_date / wd_creation_ts.
    # We choose LEGACY to maintain compatibility with readers that expect the old calendar.
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")

    # ----------------------------------------------------------
    # Add Audit Columns
    # ----------------------------------------------------------
    audit_df = add_audit_column(dim_joined_df,PIPELINE_NAME)

    audit_df.cache()

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    gold_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_journal")

    drop_df = audit_df.select("journal_key").drop_duplicates()

    if table_exists(gold_path):
        # Existing Delta table at silver_path
        target = DeltaTable.forPath(spark, gold_path)

        # audit_df must contain column: journal_key
        (
            target.alias("t")
            .merge(
                drop_df.alias("s"),
                "t.journal_key = s.journal_key"   # MERGE condition
            )
            .whenMatchedDelete()                  # When matched, delete from target
            .execute()
        )

    audit_df.write.format("delta").mode("append").option("mergeSchema", True).save(gold_path)

    audit_df.unpersist()
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    
    raise Exception(f"Failed to create/load fact_journal table: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = datetime.now()
insert_pipeline_monitoring_record( 
    _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
    "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), rows_read, rows_written, 0,
    start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
    rerun_count=0
)
print("SUCCESS logged")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
