# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run common_imports

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_pipeline_monitoring_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timezone
import time
from dateutil import parser 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_ID = ""
PIPELINE_NAME = ""
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = "notebook activity"
LEVEL="pipeline"
LAYER=""
SOURCE_TABLE=""
TARGET_TABLE=""
ENVIRONMENT = ""
WORKSPACE_NAME=""
WORKSPACE_ID = ""
TRIGGERED_BY = ""
ROWS_READ = 0
ROWS_WRITTEN = 0
ROWS_REJECTED = 0
START_TIME=""
STATUS=""
ERROR_MESSAGE=""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("MASTER_RUN_ID:", MASTER_RUN_ID)
print("RUN_ID:", RUN_ID)
print("PIPELINE_ID:", PIPELINE_ID)
print("PIPELINE_NAME:", PIPELINE_NAME)
print("NOTEBOOK_NAME:", NOTEBOOK_NAME)
print("DOMAIN:", DOMAIN)
print("SOURCE_NAME:", SOURCE_NAME)
print("ACTIVITY_NAME:", ACTIVITY_NAME)
print("LEVEL:", LEVEL)
print("LAYER:", LAYER)
print("SOURCE_TABLE:", SOURCE_TABLE)
print("TARGET_TABLE:", TARGET_TABLE)
print("ENVIRONMENT:", ENVIRONMENT)
print("WORKSPACE_NAME:", WORKSPACE_NAME)
print("WORKSPACE_ID:", WORKSPACE_ID)
print("TRIGGERED_BY:", TRIGGERED_BY)
print("ROWS_READ:", ROWS_READ)
print("ROWS_WRITTEN:", ROWS_WRITTEN)
print("ROWS_REJECTED:", ROWS_REJECTED)
print("START_TIME:", START_TIME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Derive the pipeline start time
# If START_TIME is empty, fall back to the current UTC time
if START_TIME:
    # Replace trailing 'Z' (Zulu/UTC) with explicit +00:00 offset for fromisoformat
    start_time = datetime.fromisoformat(START_TIME.replace("Z", "+00:00"))
else:
    start_time = datetime.now(timezone.utc)

end_time = datetime.now(timezone.utc)

insert_pipeline_monitoring_record(
    _safe_str(MASTER_RUN_ID),
    _safe_str(RUN_ID),
    _safe_str(PIPELINE_NAME),
    _safe_str(NOTEBOOK_NAME),
    _safe_str(DOMAIN),
    _safe_str(SOURCE_NAME),
    _safe_str(ACTIVITY_NAME),
    _safe_str(STATUS),
    _safe_str(LEVEL),
    _safe_str(LAYER),
    _safe_str(SOURCE_TABLE),
    _safe_str(TARGET_TABLE),
    ROWS_READ,
    ROWS_WRITTEN,
    ROWS_REJECTED,
    start_time,
    end_time,
    str(ERROR_MESSAGE),
    str(ERROR_MESSAGE),
    _safe_str(ENVIRONMENT),
    _safe_str(WORKSPACE_NAME),
    _safe_str(WORKSPACE_ID),
    _safe_str(TRIGGERED_BY),
    rerun_count=0,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
