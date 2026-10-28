# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run get_variable_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run dq_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType
)
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, col
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_lib_name = get_variable_name("var_cs_name")
audit_lakehouse_name = get_variable_name("audit_lakehouse_name")
audit_schema_name = get_variable_name("audit_schema_name")
audit_table_name = get_variable_name("dq_rules_table_name")
label = get_variable_name("label")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Insert new rule in the below cell

# CELL ********************

# 1. Define the static part of the data (without timestamps)
data = [
    # id, group, name, type, level, weightage, definition, created_by
    ("R1", "Contact", "Core completeness", "Completeness", "L1", 0.3,
     "Ensure all required fileds have valid, non-NULL values", "system"),
    ("R2", "Contact", "Email format valid", "Completeness", "L1", 0.2,
     "Ensure the email is in a valid format", "system"),
    ("R3", "Contact", "Phone format valid", "Completeness", "L1", 0.2,
     "Ensure the phone number is in a valid format", "system"),
    ("R4", "Contact", "Integrity check", "Integrity", "L1", 0.3,
     "Ensure all employee assigned to the project", "system")
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Define schema (created_on will be added via expression)
schema = StructType([
    StructField("id", StringType(), False),
    StructField("group", StringType(), False),
    StructField("name", StringType(), False),
    StructField("type", StringType(), False),
    StructField("level", StringType(), False),
    StructField("weightage", DoubleType(), False),
    StructField("definition", StringType(), False),
    StructField("created_by", StringType(), False),
])

# 3. Create base DataFrame
df_rules_base = spark.createDataFrame(data, schema)

# 4. Add created_on using Spark's current_timestamp()

df_rules = (
    df_rules_base
    .withColumn("created_on", current_timestamp())
    # optional: reorder columns to match the INSERT statement
    .select(
        "id",
        "group",
        "name",
        "type",
        "level",
        "weightage",
        "definition",
        "created_on",
        "created_by",
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_variable(var_lib_name, "platform_monitoring_workspace_id")
lakehouse_id = get_lakehouse_id(var_lib_name, audit_lakehouse_name)

abfss_path = abfss_path_builder(workspace_id, lakehouse_id, audit_schema_name, audit_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target = DeltaTable.forPath(spark, abfss_path)

merge_cond = "t.id = s.id AND t.group = s.group"

# Columns to compare (all non-key columns)
cols_to_compare = [
    "group",
    "name",
    "type",
    "level",
    "weightage",
    "definition"
]

# Build "any column changed" condition using null-safe equality (<=>)
change_cond = " OR ".join(
    [f"NOT (t.{c} <=> s.{c})" for c in cols_to_compare]
)

(
    target.alias("t")
    .merge(df_rules.alias("s"), merge_cond)
    .whenMatchedUpdate(
        condition=change_cond,
        set={c: col(f"s.{c}") for c in cols_to_compare}
    )
    .whenNotMatchedInsert(
        values={
            "id": col("s.id"),
            "group": col("s.group"),
            "name": col("s.name"),
            "type": col("s.type"),
            "level": col("s.level"),
            "weightage": col("s.weightage"),
            "definition": col("s.definition"),
            "created_on": col("s.created_on"),
            "created_by": col("s.created_by"),
        }
    )
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
