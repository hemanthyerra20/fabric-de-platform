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

%run get_variable_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

MASTER_RUN_ID = "test123"
RUN_ID = "test123"
PIPELINE_NAME = "test123"
LEVEL = "notebook"
LAYER = "gold"
ENVIRONMENT = "dev"
NOTEBOOK_TO_RUN = "" # from the pipeline its not allowing the array so we will get a string with , seperated.
CONCURRENCY = 7

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DOMAIN = "cs"
SOURCE_NAME = "starfish"
VAR_CS_LIB_NAME = "var_cs_process"
AUDIT_LAKEHOUSE_NAME = "lh_platform_monitoring"
CONFIG_SCHEMA_NAME = "config"
CONFIG_TABLE_NAME = "config_param"
ACTIVITY_NAME = "notebook"
WORKSPACE_NAME = notebookutils.runtime.context["currentNotebookName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Hardcoded list of {name, path} dicts (independent of DAG)
notebook_details = [
    {"name": "nb_dim_closure_reason", "path": "nb_dim_closure_reason"},
    {"name": "nb_dim_location", "path": "nb_dim_location"},
    {"name": "nb_dim_meeting", "path": "nb_dim_meeting"},
    {"name": "nb_dim_provider", "path": "nb_dim_provider"},
    {"name": "nb_dim_respondent", "path": "nb_dim_respondent"},
    {"name": "nb_dim_survey", "path": "nb_dim_survey"},
    {"name": "nb_dim_tracking_item", "path": "nb_dim_tracking_item"},
]


  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_to_execute = [op.strip().lower() for op in NOTEBOOK_TO_RUN.split(",") if op.strip() != ""]

print("notebook_to_execute:", notebook_to_execute)
print("NOTEBOOK_TO_RUN:", NOTEBOOK_TO_RUN)

# If NOTEBOOK_TO_RUN length (after stripping) is less than 5, use full param_df
if len(NOTEBOOK_TO_RUN.strip()) < 5:
    filtered_notebook_details = notebook_details
else:
    # Otherwise, filter based on the provided notebook names
    filtered_notebook_details = [{"name" : x.get("name"), "path" : x.get("path")} for x in notebook_details if x.get("name").lower() in notebook_to_execute]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

default_args = {
    "MASTER_RUN_ID": MASTER_RUN_ID,
    "RUN_ID" : RUN_ID,
    "PIPELINE_NAME" : PIPELINE_NAME,
    "LEVEL" : LEVEL,
    "LAYER" : LAYER,
    "ENVIRONMENT" : ENVIRONMENT,
    "ACTIVITY_NAME" : ACTIVITY_NAME,
    "WORKSPACE_NAME" : WORKSPACE_NAME,
    "WORKSPACE_ID" : WORKSPACE_ID,
    "NOTEBOOK_NAME": NOTEBOOK_NAME,
    "DOMAIN" : "cs",
    "SOURCE_NAME" : "starfish",
    "VAR_CS_LIB_NAME" : "var_cs_process",
    "useRootDefaultLakehouse": True,
}


DAG = {
    "activities" : [],
    "timeoutInSeconds": 43200, # max timeout for the entire DAG, default to 12 hours
    "concurrency": CONCURRENCY # max number of notebooks to run concurrently, default to 3x CPU cores, 0 means unlimited
}


for idx, notebook_detail in enumerate(filtered_notebook_details):
    if notebook_detail.get("dependencies"):
        DAG["activities"].append(
            {
                "name": notebook_detail.get("name"),  # activity name, must be unique
                "path": notebook_detail.get("path"),            # notebook item name
                "timeoutPerCellInSeconds": 1200, # max timeout for each cell
                "args": {
                    **default_args   
                },  # notebook parameters
                "workspace": WORKSPACE_ID,  # both name and id are supported
                "dependencies": notebook_detail.get("dependencies")
            }

        )
    else:
        DAG["activities"].append(
            {
                "name": notebook_detail.get("name"),  # activity name, must be unique
                "path": notebook_detail.get("path"),            # notebook item name
                "timeoutPerCellInSeconds": 1200, # max timeout for each cell
                "args": {
                    **default_args   
                },  # notebook parameters
                "workspace": WORKSPACE_ID,  # both name and id are supported
            }

        )


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
    raise Exception("DAG is not valid. Please Validate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

failed_count = 0
success_count = 0
total_count = 0
for activity_name, result in results.items():
    if result["exception"]:
        failed_count += 1
        print(f"{activity_name} - failed: {result['exception']}")
    else:
        print(f"{activity_name} - succeeded: {result['exitVal']}")
        success_count += 1
    total_count += 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Total task count - {total_count}")
print(f"Succeeded task count - {success_count}")
print(f"Failed task count - {failed_count}")

if failed_count > 0:
    raise Exception("Few tasks are failed please look into the logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
