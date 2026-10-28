# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# ============================================
# Common Pipeline Utility Functions
# ============================================

# ---------- Imports ----------
import json
import requests
import paramiko

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, row_number, when
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 1. API INGESTION
# ============================================

import requests


def fetch_api_data(url, headers=None, params=None, pagination_key=None):
    """Fetch JSON data from a (possibly paginated) HTTP API.

    Args:
        url (str): Initial URL to call.
        headers (dict, optional): HTTP headers.
        params (dict, optional): Query parameters for the first call.
        pagination_key (str, optional):
            JSON key in the response that contains the URL for the next page.
            If None, pagination is not used.

    Returns:
        list: Aggregated list of items from all pages.
    """
    all_data = []
    print(url)

    # Loop until there is no next URL (for paginated APIs) or after first call
    while url:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Reset params after the first request so they are not re‑applied
        params = None

        if isinstance(data, dict):
            # Common pattern: items in a "results" (or similar) key
            items = data.get("results")
            if items is None:
                # If no "results" key, assume the dict itself is a single record
                # and append it as-is.
                all_data.append(data)
            else:
                all_data.extend(items)

            # Get next page URL if pagination_key is provided
            url = data.get(pagination_key) if pagination_key else None
        else:
            # If the API directly returns a list, just extend and stop
            all_data.extend(data)
            break

    return all_data


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 2. SFTP INGESTION
# ============================================
def fetch_sftp_file(host, username, password, remote_path, local_path):
   transport = paramiko.Transport((host, 22))
   transport.connect(username=username, password=password)

   sftp = paramiko.SFTPClient.from_transport(transport)
   sftp.get(remote_path, local_path)

   sftp.close()
   transport.close()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 3. RAW / LANDING WRITE
# ============================================
def write_to_raw(df, path, file_format="parquet", mode="append"):
  df.write.format(file_format).mode(mode).save(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def validate_required_columns(df, required_cols):
  missing = [c for c in required_cols if c not in df.columns]
  if missing:
    raise Exception(f"Missing columns: {missing}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 6. DEDUPLICATION (BRONZE)
# ============================================
def deduplicate(df, keys, order_col):
  window_spec = Window.partitionBy(*keys).orderBy(col(order_col).desc())

  return (
    df.withColumn("rn", row_number().over(window_spec))
      .filter(col("rn") == 1)
      .drop("rn")
  )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def merge_data(target_table_path, source_df, keys):
  from delta.tables import DeltaTable

  delta_table = DeltaTable.forName(spark, target_table)

  merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])

  (
    delta_table.alias("t")
    .merge(source_df.alias("s"), merge_condition)
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
  )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 9. DATA QUALITY CHECKS
# ============================================
def check_nulls(df, column):
  return df.filter(col(column).isNull()).count()


def check_duplicates(df, keys):
  return (
    df.groupBy(*keys)
      .count()
      .filter(col("count") > 1)
      .count()
  )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 10. LOGGING message on to the output
# ============================================
def log_message(message):
  print(f"[INFO] {message}")


def log_record_count(df, stage):
  print(f"[INFO] {stage} count: {df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# 12. ERROR HANDLING
# ============================================
def safe_execute(func, *args, **kwargs):
  try:
    return func(*args, **kwargs)
  except Exception as e:
    print(f"[ERROR] {str(e)}")
    raise


# ============================================
# 13. FILE MANAGEMENT
# ============================================
def archive_file(src_path, archive_path):
  dbutils.fs.mv(src_path, archive_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
