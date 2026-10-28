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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select distinct status from lh_platform_monitoring.audit.master_pipeline_monitoring

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC UPDATE lh_platform_monitoring.audit.master_pipeline_monitoring
# MAGIC SET status =
# MAGIC CASE
# MAGIC     -- Failed
# MAGIC     WHEN LOWER(TRIM(status)) IN ('fail', 'failed') THEN 'Failed'
# MAGIC  
# MAGIC     -- In Progress
# MAGIC     WHEN LOWER(REPLACE(TRIM(status), '-', '')) IN ('inprogress') THEN 'In Progress'
# MAGIC  
# MAGIC     -- Success
# MAGIC     WHEN LOWER(TRIM(status)) IN ('success', 'successsuccess', 'successs') THEN 'Success'
# MAGIC  
# MAGIC     -- Keep already standardized values
# MAGIC     ELSE status
# MAGIC END;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql.functions import col, lower


print("=" * 80)
print(f"Started at: {datetime.now()}")
print("=" * 80)

tables = spark.sql("SHOW TABLES").collect()

success_tables = []
failed_tables = []

for row in tables:
    table_name = row.tableName

    try:
        print(f"\nProcessing table: {table_name}")

        # Read the table as a DataFrame
        df = spark.read.table(table_name)

        # Original column names and target lowercase names (based on schema)
        original_cols = df.columns
        lower_cols = [c.lower() for c in original_cols]

        # If lowercasing causes duplicates, skip this table
        if len(lower_cols) != len(set(lower_cols)):
            print(f"Skipping {table_name}: duplicate column issue after lowercase")
            failed_tables.append((table_name, "Duplicate column issue"))
            continue

        # Build expressions:
        # - For STRING columns, lowercase the values
        # - For non-STRING columns, keep values as-is
        # - Alias all columns to their lowercase names from schema
        string_cols = {name for name, dtype in df.dtypes if dtype == "string"}

        select_exprs = []
        for orig_name, lower_name in zip(original_cols, lower_cols):
            if orig_name in string_cols:
                # Lowercase string values
                select_exprs.append(lower(col(orig_name)).alias(lower_name))
            else:
                # Keep non-string values but rename column
                select_exprs.append(col(orig_name).alias(lower_name))

        df_lower = df.select(*select_exprs)

        # Overwrite the existing table as Delta with the new schema and data
        df_lower.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(table_name)

        print(f"Completed: {table_name}")
        success_tables.append(table_name)

    except Exception as e:
        # Any error reading or writing this table is recorded, but processing continues
        print(f"Failed: {table_name} - {str(e)}")
        failed_tables.append((table_name, str(e)))

print("\nSUMMARY")
print(f"Success: {len(success_tables)}")
print(f"Failed : {len(failed_tables)}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
