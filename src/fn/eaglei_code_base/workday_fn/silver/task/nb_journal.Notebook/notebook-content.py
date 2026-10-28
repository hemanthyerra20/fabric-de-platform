# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5cb1dc45-6dbe-470f-8ef9-0291324328c2",
# META       "default_lakehouse_name": "lh_fn_landing",
# META       "default_lakehouse_workspace_id": "bedb4898-64aa-4f9d-b9de-e0c4b634ee27",
# META       "known_lakehouses": [
# META         {
# META           "id": "5cb1dc45-6dbe-470f-8ef9-0291324328c2"
# META         },
# META         {
# META           "id": "c1ad79e0-8d5c-445d-b093-a187bd3af4c3"
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

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

PIPELINE_NAME = "testing"
DOMAIN = ""
SERVICE_NAME = "Financial_Management"
OPERATION_NAME = "Get_Journals"
SOURCE_NAME = ""
VAR_FN_LIB_NAME = "var_fn_process"
AUDIT_LAKEHOUSE_NAME = ""
CONFIG_SCHEMA_NAME = ""
CONFIG_TABLE_NAME = ""
MASTER_RUN_ID = ""
RUN_ID = ""
NOTEBOOK_NAME = ""
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

DOMAIN = "fn"
SOURCE_NAME = "workday"
AUDIT_LAKEHOUSE_NAME = "lh_platform_monitoring"
CONFIG_SCHEMA_NAME = "config"
CONFIG_TABLE_NAME = "config_param"
ACTIVITY_NAME = "notebook"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    start_time = datetime.now()
    config_workspace_id = get_variable(VAR_FN_LIB_NAME, "platform_monitoring_workspace_id")
    config_lakehouse_id = get_lakehouse_id(VAR_FN_LIB_NAME, AUDIT_LAKEHOUSE_NAME)
    config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, CONFIG_SCHEMA_NAME, CONFIG_TABLE_NAME)

    ## Fetching Config object values
    config_df = spark.read.format("delta").load(config_abfss_path)\
                .filter((F.col("source") == SOURCE_NAME) & (F.col("domain") == DOMAIN))

    bronze_schema_name = get_param_value(config_df,SOURCE_NAME,"bronze_schema_name")
    bronze_lh_name = get_param_value(config_df,SOURCE_NAME,"bronze_lakehouse_name")

    silver_schema_name = get_param_value(config_df,SOURCE_NAME,"silver_schema_name")
    silver_lh_name = get_param_value(config_df,SOURCE_NAME,"silver_lakehouse_name")


    parent_object = get_param_value(config_df,SOURCE_NAME, SERVICE_NAME.lower())
    bronze_table_name = parent_object[OPERATION_NAME.lower()]["bronze_table_name"]

    workspace_id = get_workspace_id(VAR_FN_LIB_NAME, DOMAIN, "store")
    bronze_lakehouse_id = get_lakehouse_id(VAR_FN_LIB_NAME, bronze_lh_name)
    silver_lakehouse_id = get_lakehouse_id(VAR_FN_LIB_NAME, silver_lh_name)

    bronze_abfss_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, bronze_schema_name, bronze_table_name)
    bronze_df = spark.read.format("delta").load(bronze_abfss_path)


except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(""), _safe_str(""), 0, 0, 0,
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

try:
    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.journal"

    journal_df = bronze_df.selectExpr(
                                    "journal_entry_reference_wid as journal_key",
                                    "ledger_reference_id",
                                    "accounting_date",
                                    "journal_source_id",
                                    "transaction_date",
                                    "student_charge_id",
                                    "student_charge_document_id",
                                    "record_quantity",
                                    "total_ledger_debits",
                                    "total_ledger_credits",
                                    "creation_date",
                                    "journal_entry_line_data",
                                    "journal_entry_reference_wid",
                                    "journal_number",
                                    "journal_entry_status_id",
                                    "organization_reference_id",
                                    "company_reference_id",
                                    "currency_id",
                                    "currency_numeric_code"
                                )

    filter_status_df = journal_df.filter(F.col("journal_entry_status_id") == "POSTED")
    journal_bronze_df = to_lowercase_columns(filter_status_df)

    audit_df = add_audit_column(journal_bronze_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "journal"
    )

    write_delta_tbl(
        source_df=audit_df,
        target_table_path=silver_path,
        keys=["journal_key"]
    )

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
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

# CELL ********************

try:
    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.journal_line"

    journal_line_df = bronze_df\
                                .withColumn("journal_entry_line_data", F.explode(F.col("journal_entry_line_data")))\
                                .selectExpr(
                                    "journal_entry_reference_wid as journal_key",
                                    "journal_entry_status_id",
                                    "journal_entry_line_data.*"
                                )


    select_df = journal_line_df\
                    .selectExpr(
                        "uuid() as journal_line_key",
                        "journal_key",
                        "journal_entry_status_id",
                        "CAST(Budget_Date as date) as budget_date",
                        "CAST(Credit_Amount as decimal(26,6)) as credit_amount",
                        "CAST(Currency_Rate as decimal(26,6)) as currency_rate",
                        "Currency_Reference.Currency_ID AS currency_id",
                        "Currency_Reference.Currency_Numeric_Code AS currency_numeric_code",
                        "Debit_Amount",
                        "Exclude_from_Spend_Report",
                        "Journal_Line_Number",
                        "Ledger_Account_Reference.Ledger_Account_ID AS Ledger_Account_ID",
                        "CAST(Ledger_Credit_Amount as decimal(18,3)) as ledger_credit_amount",
                        "CAST(Ledger_Debit_Amount as decimal(18,3)) as ledger_debit_amount",
                        "Line_Company_Reference.Organization_Reference_ID AS Organization_Reference_ID",
                        "Line_Company_Reference.Company_Reference_ID AS Company_Reference_ID",
                        "Memo",
                        "Worktags_Reference"
                    ).cache()


    filter_df = select_df.filter(F.col("journal_entry_status_id") == "POSTED")\
                        .drop("journal_entry_status_id")

    
    journal_select_df =  filter_df\
                    .selectExpr("*")\
                    .drop("Worktags_Reference")

    journal_tag_select_df =  filter_df\
                    .selectExpr(
                        "journal_line_key",
                        "journal_key",
                        "Worktags_Reference")

    container_bronze_df = to_lowercase_columns(journal_select_df)
    
    audit_df = add_audit_column(container_bronze_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "journal_line_data"
    )

    drop_df = audit_df.select("journal_key").drop_duplicates()

    if table_exists(silver_path):
        # Existing Delta table at silver_path
        target = DeltaTable.forPath(spark, silver_path)

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

    # FIX: set session-level rebase mode for Parquet writes to avoid
    # Spark 3.x "ancient datetime" error when writing pre-1582 dates.
    # Use CORRECTED if all readers are Spark 3+/Proleptic Gregorian.
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
    # Optionally also handle INT96 timestamps if present:
    # spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")

    (
        audit_df.write
        .format("delta")
        .mode("append")
        .save(silver_path)
    )

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
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

# CELL ********************

try:
    source_table = f"{bronze_lh_name}.{bronze_schema_name}.{bronze_table_name}"
    target_table = f"{silver_lh_name}.{silver_schema_name}.journal_line_worktag"

    wt_elem_type = journal_tag_select_df.schema["Worktags_Reference"].dataType.elementType

    # Build expressions for keys and values arrays, from struct fields except WID
    key_cols = []
    val_cols = []
    for field in wt_elem_type.fields:
        if field.name == "WID":
            continue
        key_cols.append(F.lit(field.name))
        val_cols.append(F.col(f"w.{field.name}"))

    worktag_df = journal_tag_select_df.select(
        "journal_line_key",
        "journal_key",
        "Worktags_Reference",
    )

    result_df = (
        worktag_df
        # explode array of structs -> one struct per row
        .withColumn("w", F.explode("Worktags_Reference"))
        # build map<type, value> using map_from_arrays(keys_array, values_array)
        .withColumn(
            "kv_map",
            F.map_from_arrays(
                F.array(*key_cols),
                F.array(*val_cols),
            ),
        )
        # explode the map into key/value structs
        .withColumn("kv", F.explode(F.map_entries("kv_map")))
        .select(
            "journal_line_key",
            "journal_key",
            F.col("kv.key").alias("type"),
            F.col("kv.value").alias("value"),
        )
        .filter(F.col("value").isNotNull())
    )

    container_bronze_df = to_lowercase_columns(result_df)
    
    audit_df = add_audit_column(container_bronze_df, PIPELINE_NAME)

    final_count = audit_df.count()
    rows_read = final_count
    rows_written = final_count

    silver_path = abfss_path_builder(
        workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "journal_line_worktag"
    )

    drop_df = audit_df.select("journal_key").drop_duplicates()

    if table_exists(silver_path):
        # Existing Delta table at silver_path
        target = DeltaTable.forPath(spark, silver_path)

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

    audit_df.write.format("delta").mode("append").option("mergeSchema", True).save(silver_path)

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
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
