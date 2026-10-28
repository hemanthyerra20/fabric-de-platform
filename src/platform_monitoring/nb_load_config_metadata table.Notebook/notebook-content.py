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
# MAGIC -- update config.config_metadata_ipeds 
# MAGIC -- set load_required_flag=1
# MAGIC -- where 
# MAGIC -- destination_table_name in ('cyear_a') and 
# MAGIC -- year='2023'
# MAGIC --where destination_table_name='drvefyear' and year='2023'
# MAGIC 
# MAGIC --where source_table_name like 'DRVEF12%'
# MAGIC --year='2023'
# MAGIC --where destination_table_name='drvef12year'
# MAGIC select * from config.config_metadata_ipeds 
# MAGIC --where 
# MAGIC --load_required_flag=1
# MAGIC 
# MAGIC -- Insert into config.config_metadata_ipeds (db_name,source_table_name,schema,
# MAGIC -- destination_table_name,year,load_required_flag)
# MAGIC -- values ('ipeds202425','EFFY2023','ipeds','effyyear',2024,1)
# MAGIC -- Update config.config_metadata_ipeds
# MAGIC -- set source_table_name= 'EFFY2024'
# MAGIC -- where source_table_name= 'EFFY2023' and year=2024
# MAGIC 
# MAGIC -- where load_required_flag=1
# MAGIC -- where source_table_name='C2024_A'
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



# ==========================================

# Parameters

# ==========================================
 
db_name = "ipeds201718"

year_value = 2017

load_required_flag = 1

schema_name = "ipeds"
 
# ==========================================

# Imports

# ==========================================
 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from pyspark.sql.functions import current_timestamp

import re
 
# ==========================================

# Source Tables

# ==========================================
 
source_tables = [
 
    "C2017_A","C2017_B","C2017_C","DRVC2017","DRVEF122017","DRVEF2017",

    "DRVGR2017","DRVOM2017","EF2017","EF2017A","EF2017B","EF2017D",

    "EFFY2017","EFFY2017_DIST","EFIA2017","GR200_17",

    "GR2017","GR2017_L2","GR2017_PELL_SSL","OM2017","EFFY2017_HS","GR2017_GENDER"
 
]
 
# ==========================================

# Destination Table Logic

# ==========================================
 
def build_destination_table_name(source_table):
 
    destination = source_table

    destination = re.sub(r'122017', '12YEAR', destination)

    destination = re.sub(r'2217', 'YEAR', destination)

    destination = re.sub(r'2017', 'YEAR', destination)

    destination = re.sub(r'_17', '_YEAR', destination)
 
    return destination.lower()
 
# ==========================================

# Build Rows

# ==========================================
 
rows = []
 
for table_name in source_tables:
 
    rows.append(

        (

            db_name,

            table_name,

            schema_name,

            build_destination_table_name(table_name),

            year_value,

            load_required_flag

        )

    )
 
# ==========================================

# Schema

# ==========================================
 
metadata_schema = StructType([

    StructField("db_name", StringType(), True),

    StructField("source_table_name", StringType(), True),

    StructField("schema", StringType(), True),

    StructField("destination_table_name", StringType(), True),

    StructField("year", IntegerType(), True),

    StructField("load_required_flag", IntegerType(), True)

])
 
metadata_df = spark.createDataFrame(rows, schema=metadata_schema)
 
# Add timestamp safely (NO Delta DEFAULT dependency)

metadata_df = metadata_df.withColumn("created_datetime", current_timestamp())
 
display(metadata_df)
 
# ==========================================

# Create Schema

# ==========================================
 
spark.sql("CREATE SCHEMA IF NOT EXISTS config")
 
# ==========================================

# Recreate Table (SAFE AFTER DELETE)

# ==========================================
 
spark.sql("""

CREATE TABLE IF NOT EXISTS config.config_metadata_ipeds

(

    db_name STRING,

    source_table_name STRING,

    schema STRING,

    destination_table_name STRING,

    year INT,

    load_required_flag INT,

    created_datetime TIMESTAMP

)

USING DELTA

""")
 
# ==========================================

# Delete existing records (if any)

# ==========================================
 
spark.sql(f"""

DELETE FROM config.config_metadata_ipeds

WHERE db_name = '{db_name}'

AND year = {year_value}

""")
 
# ==========================================

# Insert fresh records

# ==========================================
 
metadata_df.write \
.mode("append") \
.format("delta") \
.saveAsTable("config.config_metadata_ipeds")
 
# ==========================================

# Validation

# ==========================================
 
result_df = spark.sql(f"""

SELECT *

FROM config.config_metadata_ipeds

WHERE db_name = '{db_name}'

AND year = {year_value}

ORDER BY source_table_name

""")
 
print(f"Total Records Loaded = {result_df.count()}")
 
display(result_df)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
