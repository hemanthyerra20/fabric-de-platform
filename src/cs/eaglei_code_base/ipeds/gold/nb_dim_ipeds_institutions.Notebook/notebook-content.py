# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fd975be1-c831-4267-b419-318d73f5b16b",
# META       "default_lakehouse_name": "lh_cs_gold",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

files = mssparkutils.fs.ls("Files/ipeds")
for f in files:
    print(f.name)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import re
file_path = "/lakehouse/default/Files/ipeds/IPEDS_Institutions.xlsx"
# Read Excel
df = pd.read_excel(
    file_path,
    header=None,
    engine="openpyxl"
).fillna("")
output_rows = []
 
# ----------------------------------
# Parse first row
# NWTC IPEDS NUMBER (239488)

# ----------------------------------
header_text = str(df.iloc[0, 0]).strip()
m = re.search(
    r'(.+?)\s+IPEDS NUMBER\s*\((\d+)\)',
    header_text,
    re.IGNORECASE
)
if m:
    institution_name = m.group(1).strip()
    institution_ipeds_number = int(m.group(2))
    output_rows.append({
        "Name": institution_name,
        "IPEDS Number": institution_ipeds_number,
        "Type": institution_name

    })
 
# ----------------------------------
# Parse Institution Groups
# ----------------------------------
current_type = None
for i in range(1, len(df)):
    col1 = str(df.iloc[i, 0]).strip()
    col2 = str(df.iloc[i, 1]).strip()
    if col1 == "":
        continue
    # Group header row
    if col2.upper() == "IPEDS NUMBER":
        current_type = (
            col1
            .replace(" Institutions", "")
            .replace(" Institution", "")
            .strip()
        )
        continue
    # Institution row
    if current_type:
        try:
            ipeds_number = int(float(col2))

        except:
            continue
        output_rows.append({
            "Name": col1,
            "IPEDS Number": ipeds_number,
            "Type": current_type
        })
 
# Final dataframe
final_pd = pd.DataFrame(output_rows)
final_pd = pd.DataFrame(output_rows)
final_pd.columns = [
    "institution_name",
    "unitid",
    "peer_group"
]

df = spark.createDataFrame(final_pd)
 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import functions as F
df_dim_ipeds = (
    df
    .select(
        F.col("unitid").cast("int").alias("unitid"),
        F.col("institution_name").cast("string").alias("institution_name"),
        F.col("peer_group").cast("string").alias("peer_group")
    )
    # SHA2 surrogate key
    .withColumn(
        "sk_dim_ipeds_institutions_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.coalesce(F.col("unitid").cast("string"), F.lit("")),
                F.coalesce(F.trim(F.col("peer_group")), F.lit("")),
                F.coalesce(F.trim(F.col("institution_name")), F.lit(""))
            ),
            256
        )
    )
    .withColumn(
        "is_nwtc",
        F.when(
            F.upper(F.trim(F.col("peer_group"))) == "NWTC",
            F.lit(True)
        ).otherwise(F.lit(False))
    )
 
    .withColumn(
        "display_sort",
        F.when(F.upper(F.trim(F.col("peer_group"))) == "NWTC", 1)
         .when(F.upper(F.trim(F.col("peer_group"))) == "WTCS", 2)
         .when(F.upper(F.trim(F.col("peer_group"))) == "PEER", 3)
         .when(F.upper(F.trim(F.col("peer_group"))) == "ASPIRANT", 4)
         .otherwise(None)
         .cast("byte")

    )
    .withColumn("created_on", F.current_timestamp())
    .withColumn("modified_on", F.current_timestamp())
 
    # Explicit final column order

    .select(
        "sk_dim_ipeds_institutions_key",
        "unitid",
        "institution_name",
        "peer_group",
        "is_nwtc",
        "display_sort",
        "created_on",
        "modified_on"
    )
)
display(df_dim_ipeds)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE SCHEMA IF NOT EXISTS ipeds
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dim_ipeds.write \
.mode("overwrite") \
.format("delta") \
.saveAsTable("ipeds.dim_ipeds_institutions")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
