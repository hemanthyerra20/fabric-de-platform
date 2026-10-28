# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "f45c2c8e-a6e5-4098-aaf6-567ab6b6ab98",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
import json
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "workday_extraction_filters"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    ("program_of_study_all", "Applied", "8cb7cc3625a110001eabe0a17aea01d6", "Student_Program_of_Study_Record_Status_ID", "APPLIED"),
    ("program_of_study_all", "Conferral Pending", "8cb7cc3625a110001eabe0b2105701d7", "Student_Program_of_Study_Record_Status_ID", "CONFERRAL_PENDING"),
    ("program_of_study_all", "Requirements Not Met", "8cb7cc3625a110001eabe0b64f3f01d8", "Student_Program_of_Study_Record_Status_ID", "REQUIREMENTS_NOT_MET"),
    ("program_of_study_all", "Requirements Met", "8cb7cc3625a110001eabe0ba9eac01d9", "Student_Program_of_Study_Record_Status_ID", "REQUIREMENTS_MET"),
    ("program_of_study_all", "Ready for Review", "8cb7cc3625a110001eabe0beb5c101da", "Student_Program_of_Study_Record_Status_ID", "READY_FOR_REVIEW"),
    ("program_of_study_all", "In Progress", "d8694c8e2f33100009872c816b440063", "Student_Program_of_Study_Record_Status_ID", "IN_PROGRESS"),
    ("program_of_study_all", "Conferred", "d30fc924866a100066ae0c1d7702037f", "Student_Program_of_Study_Record_Status_ID", "CONFERRED"),
    ("program_of_study_all", "Leave of Absence", "4ff81f83ffcd10001999020c41cf769c", "Student_Program_of_Study_Record_Status_ID", "LEAVE_OF_ABSENCE"),
    ("program_of_study_all", "Pending Completion", "4ff81f83ffcd10001999022b20d9769d", "Student_Program_of_Study_Record_Status_ID", "PENDING_COMPLETION"),
    ("program_of_study_all", "Pending Change", "4ff81f83ffcd100019990241fc66769e", "Student_Program_of_Study_Record_Status_ID", "PENDING_CHANGE"),
    ("program_of_study_all", "Suspended", "4ff81f83ffcd100019990265a2a4769f", "Student_Program_of_Study_Record_Status_ID", "SUSPENDED"),
    ("program_of_study_all", "Dismissed", "4ff81f83ffcd1000199902766a6076a0", "Student_Program_of_Study_Record_Status_ID", "DISMISSED"),
    ("program_of_study_all", "Institutional Withdrawal", "4ff81f83ffcd10001999028ace5e76a1", "Student_Program_of_Study_Record_Status_ID", "INSTITUTIONAL_WITHDRAWAL"),
    ("program_of_study_all", "Discontinued", "4ff81f83ffcd10001999029e97ac76a2", "Student_Program_of_Study_Record_Status_ID", "DISCONTINUED"),
    ("program_of_study_all", "Complete", "4ff81f83ffcd1000199902b0169176a3", "Student_Program_of_Study_Record_Status_ID", "COMPLETE"),
    ("program_of_study_all", "Matriculated", "63c4a1ef32521000129ee15569d48bb6", "Student_Program_of_Study_Record_Status_ID", "MATRICULATED"),
]

# Column names
columns = [
    "table_name",
    "instance",
    "workday_id",
    "type",
    "instance_id"
]

# Create DataFrame
df = spark.createDataFrame(data, schema=columns)

# Add GUID column to the existing DataFrame
df_with_guid = (
    df
    .withColumn("id", F.expr("uuid()"))
    .withColumn("created_on", F.current_timestamp())
    .withColumn("created_by", F.lit("system"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_with_guid.write.format("delta")\
    .mode("overwrite")\
    .save(config_abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Successfully inserted/updated the config details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
