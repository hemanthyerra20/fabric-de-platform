# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "d6e9c4b7-fdc8-4966-afb3-443827650df9",
# META       "workspaceId": "088bba09-dd86-49a0-87f3-ad9331892945"
# META     }
# META   }
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

MASTER_RUN_ID = "M-123"
RUN_ID = "123"
PIPELINE_ID = "p-123"
PIPELINE_NAME = "test"
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]
ACTIVITY_NAME = "notebookactivity"
ENVIRONMENT = "dev"
TRIGGERED_BY = "manual"
WORKSPACE_ID = "12fwerwe"
SOURCE_NAME = "workday_hr"
DOMAIN = "hr"
STATUS = "in-progress"
START_TIME = ""

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

try:
    # Log STARTED (or whatever STATUS is passed in) to the master monitoring table
    insert_master_status(
        pipeline_id=PIPELINE_ID,
        pipeline_name=PIPELINE_NAME,
        master_run_id=MASTER_RUN_ID,
        source_name=SOURCE_NAME,
        domain=DOMAIN,
        status=STATUS,  # e.g. "in-progress" / "STARTED"
        environment=ENVIRONMENT,
        start_time=start_time,
        end_time=datetime.now(timezone.utc),
        error_message="",           # no error on success path
        triggered_by=TRIGGERED_BY,
        workspace_id=WORKSPACE_ID,
    )
    print("SUCCESS logged in master monitor")

except Exception as e:
    print("FAILED logged in master monitor")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
