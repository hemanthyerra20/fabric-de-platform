# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run dq_insert_rules

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

%run dq_rules

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import uuid
import json
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

RUN_ID = generate_guid()
JOB_NAME = "dq_job"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

VAR_LIB_NAME = get_variable_name("var_cs_name")
AUDIT_SCHEMA_NAME = get_variable_name("audit_schema_name")
DQ_FAILED_RECORDS_TABLE_NAME = get_variable_name("dq_failed_records_table_name")
DQ_RUN_RESULT_TABLE_NAME = get_variable_name("dq_run_results_table_name")
CONTROL_LOG_TABLE_NAME = get_variable_name("control_log_table_name")
AUDIT_LAKEHOUSE_NAME =  get_variable_name("audit_lakehouse_name")
LABEL = get_variable_name("label")
WORKSPACE_NAME = get_variable_name("workspace_name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities" : [],
    "timeoutInSeconds": 43200, # max timeout for the entire DAG, default to 12 hours
    "concurrency": 5 # max number of notebooks to run concurrently, default to 3x CPU cores, 0 means unlimited
}

for gi, group in enumerate(dq_rules):
    # Skip empty dicts (like the last {} in your list)
    if not isinstance(group, dict) or not group:
        continue

    DAG["activities"].append(
        {
            "name": f"{group['rule_group']}_{gi}",  # activity name, must be unique
            "path": "dq_task",            # notebook item name
            "timeoutPerCellInSeconds": 900,  # max timeout for each cell
            "args": {
                "RUN_ID": RUN_ID,
                "JOB_NAME": JOB_NAME,
                "RULE_GROUP": group["rule_group"],
                # pass the full list of rules for this group
                "RULES": json.dumps(group["rules"]),
                "VAR_LIB_NAME": VAR_LIB_NAME,
                "AUDIT_SCHEMA_NAME": AUDIT_SCHEMA_NAME,
                "DQ_FAILED_RECORDS_TABLE_NAME": DQ_FAILED_RECORDS_TABLE_NAME,
                "DQ_RUN_RESULT_TABLE_NAME": DQ_RUN_RESULT_TABLE_NAME,
                "CONTROL_LOG_TABLE_NAME": CONTROL_LOG_TABLE_NAME,
                "AUDIT_LAKEHOUSE_NAME": AUDIT_LAKEHOUSE_NAME,
                "LABEL": LABEL,
                "WORKSPACE_NAME": WORKSPACE_NAME
            },  # notebook parameters
            "workspace": WORKSPACE_NAME,  # both name and id are supported
        }
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if notebookutils.notebook.validateDAG(DAG):
    try:
        results = notebookutils.notebook.runMultiple(DAG, {"displayDAGViaGraphviz": False})
    except Exception as ex:
        results = ex.result
else:
    cols["status"] = "failed"
    create_control_log_record(cols)
    raise Exception("DAG is not valid. Please Validate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

failed_count = 0
for activity_name, result in results.items():
    if result["exception"]:
        failed_count += 1
        print(f"{activity_name} - failed")
    else:
        print(f"{activity_name} - succeeded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
