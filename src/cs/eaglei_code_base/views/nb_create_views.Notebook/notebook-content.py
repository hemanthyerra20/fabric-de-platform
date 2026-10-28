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
# META     },
# META     "warehouse": {
# META       "known_warehouses": [
# META         {
# META           "id": "da7c9db2-6639-4346-b4e5-bd3256de4395",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW lh_platform_monitoring.audit.vw_pipeline_monitoring
# MAGIC AS
# MAGIC     WITH latest_run AS (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY id
# MAGIC                 ORDER BY end_time DESC
# MAGIC             ) AS rn,
# MAGIC             COUNT(*) OVER (
# MAGIC                 PARTITION BY id
# MAGIC             ) - 1 AS calc_rerun_count
# MAGIC         FROM lh_platform_monitoring.audit.pipeline_monitoring
# MAGIC     )
# MAGIC     SELECT
# MAGIC         id,
# MAGIC         master_run_id,
# MAGIC         run_id,
# MAGIC         pipeline_name,
# MAGIC         notebook_name,
# MAGIC         domain,
# MAGIC         source_name,
# MAGIC         layer,
# MAGIC         source_table,
# MAGIC         target_table,
# MAGIC         level,
# MAGIC         activity_name,
# MAGIC         status,
# MAGIC         calc_rerun_count AS rerun_count,
# MAGIC         rows_read,
# MAGIC         rows_written,
# MAGIC         rows_rejected,
# MAGIC         start_time,
# MAGIC         end_time,
# MAGIC         duration_seconds,
# MAGIC         error_message,
# MAGIC         error_type,
# MAGIC         environment,
# MAGIC         workspace_name,
# MAGIC         workspace_id,
# MAGIC         triggered_by
# MAGIC     FROM latest_run
# MAGIC     WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE OR REPLACE VIEW lh_platform_monitoring.audit.vw_master_pipeline_monitoring
# MAGIC AS
# MAGIC 
# MAGIC     WITH latest_run AS (
# MAGIC         SELECT *,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY id
# MAGIC                 ORDER BY end_time DESC
# MAGIC             ) AS rn,
# MAGIC             SUM(CASE WHEN LOWER(STATUS) IN ("success", "failed") THEN 1 ELSE 0 END) OVER (
# MAGIC                 PARTITION BY id
# MAGIC             ) AS calc_rerun_count
# MAGIC         FROM lh_platform_monitoring.audit.master_pipeline_monitoring
# MAGIC     )
# MAGIC     SELECT 
# MAGIC         pipeline_id,
# MAGIC         pipeline_name,
# MAGIC         master_run_id,
# MAGIC         source_name,
# MAGIC         domain,
# MAGIC         environment,
# MAGIC         status,
# MAGIC         CASE WHEN calc_rerun_count >= 1 THEN calc_rerun_count - 1 ELSE calc_rerun_count END AS rerun_count,
# MAGIC         start_time,
# MAGIC         end_time,
# MAGIC         duration_seconds,
# MAGIC         error_message,
# MAGIC         triggered_by,
# MAGIC         workspace_id
# MAGIC     FROM latest_run
# MAGIC     WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
