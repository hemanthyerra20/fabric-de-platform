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
# META           "id": "0277ffad-3784-4551-8d7d-66d1b8ff633a"
# META         },
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run dq_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run logger_util

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run get_variable_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType,
    DoubleType
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logger = get_logger("dq_pipeline")
var_lib_name = get_variable_name("var_cs_name")
audit_lakehouse_name = get_variable_name("audit_lakehouse_name")


workspace_id = get_variable(var_lib_name, "platform_monitoring_workspace_id")
lakehouse_id = get_lakehouse_id(var_lib_name,audit_lakehouse_name)
schema_name = "audit1"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SCHEMA
# ============================================================

dq_run_results_schema = StructType([
    StructField("id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("rule_group", StringType(), True),
    StructField("description", StringType(), True),
    StructField("custom_rule", StringType(), True),
    StructField("lakehouse_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("workspace_name", StringType(), True),
    StructField("label", StringType(), True),
    StructField("status", StringType(), True),
    StructField("total_rows", IntegerType(), True),
    StructField("success_rows", IntegerType(), True),
    StructField("failed_rows", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField("created_on", TimestampType(), True),
    StructField("created_by", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SCHEMA
# ============================================================

dq_failed_records_schema = StructType([
    StructField("id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("result_id", StringType(), False),
    StructField("rule_id", StringType(), False),
    StructField("rule_group", StringType(), True),
    StructField("description", StringType(), True),
    StructField("lakehouse_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("workspace_name", StringType(), True),
    StructField("label", StringType(), True),
    StructField("column_name", StringType(), True),
    StructField("source_data", StringType(), True),
    StructField("failure_reason", StringType(), True),
    StructField("created_on", TimestampType(), True),
    StructField("created_by", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SCHEMA
# ============================================================

dq_rules_schema = StructType([
    StructField("id", StringType(), False),
    StructField("group", StringType(), False),
    StructField("name", StringType(), False),
    StructField("type", StringType(), False),
    StructField("level", StringType(), False),
    StructField("weightage", DoubleType(), False),
    StructField("definition", StringType(), False),
    StructField("created_on", TimestampType(), True),
    StructField("created_by", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# SCHEMA
# ============================================================

control_log_schema = StructType([
    StructField("id", StringType(), False),
    StructField("run_id", StringType(), False),
    StructField("process_name", StringType(), False),
    StructField("lakehouse_name", StringType(), True),
    StructField("schema_name", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("inc_column_name", StringType(), True),
    StructField("inc_watermark", TimestampType(), True),
    StructField("workspace_name", StringType(), True),
    StructField("label", StringType(), True),
    StructField("status", StringType(), True),
    StructField("created_on", TimestampType(), True),
    StructField("created_by", StringType(), True),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_to_create = [
    {
        "name": "dq_rules",
        "schema": dq_rules_schema,
    },
    {
        "name": "dq_run_results",
        "schema": dq_run_results_schema,
    },
    {
        "name": "dq_failed_records",
        "schema": dq_failed_records_schema,
    },
    {
        "name": "control_log",
        "schema": control_log_schema,
    },
]


for t in tables_to_create:
    try:
        abfss_path = abfss_path_builder(
                            workspace_id,
                            lakehouse_id,
                            schema_name,       # e.g. "audit"
                            t['name']
                        )
        df_empty = spark.createDataFrame([], t["schema"])

        (
            df_empty.write
            .format("delta")
            .mode("ignore")
            .save(abfss_path)
        )

        logger.info(f"SUCCESS: {t['name']} table created (if not existing)")

    except Exception as e:
        logger.error(f"ERROR creating {t['name']} table: {str(e)}")
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
