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

#PARAMETERS FROM PIPLELINE
MASTER_RUN_ID = "M-123-29092026"
RUN_ID = "123"
PIPELINE_ID = "p-123"
PIPELINE_NAME = "test"
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]
ACTIVITY_NAME = "notebookactivity"
ENVIRONMENT = "dev"
TRIGGERED_BY = "manual"
WORKSPACE_ID = "12fwerwe"
SOURCE_NAME = "ipeds"
DOMAIN = "CS"
STATUS = "In Progress"
START_TIME = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(RUN_ID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

start_time = datetime.now()

try:
    # Log STARTED (or whatever STATUS is passed in) to the master monitoring table
    log_master_status(
        pipeline_id=PIPELINE_ID,
        pipeline_name=PIPELINE_NAME,
        master_run_id=MASTER_RUN_ID,
        source_name=SOURCE_NAME,
        domain=DOMAIN,
        status=STATUS,  # e.g. "in-progress" / "STARTED"
        environment=ENVIRONMENT,
        start_time=start_time,
        end_time=datetime.now(),
        error_message="",           # no error on success path
        triggered_by=TRIGGERED_BY,
        workspace_id=WORKSPACE_ID,
    )
    print("SUCCESS logged in master monitor")

except Exception as e:
    # If logging itself fails, capture the error message and still attempt to write a FAILED record
    end_time = datetime.now()

    log_master_status(
        pipeline_id=PIPELINE_ID,
        pipeline_name=PIPELINE_NAME,
        master_run_id=MASTER_RUN_ID,
        source_name=SOURCE_NAME,
        domain=DOMAIN,
        status="Failed",           # override to FAILED when logging throws
        environment=ENVIRONMENT,
        start_time=start_time,
        end_time=end_time,
        error_message=str(e),       # store the logging error
        triggered_by=TRIGGERED_BY,
        workspace_id=WORKSPACE_ID,
    )
    print("FAILED logged in master monitor")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
