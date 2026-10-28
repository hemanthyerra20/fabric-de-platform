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

%run nb_get_variable_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils

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
LAYER = "silver"
ENVIRONMENT = "dev"
OPERATION_TO_RUN = "" # from the pipeline its not allowing the array so we will get a string with , seperated. pass the notebook name 
CONCURRENCY = 14

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DOMAIN = "fn"
SOURCE_NAME = "workday"
VAR_FN_LIB_NAME = "var_fn_process"
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

config_workspace_id = get_variable(VAR_FN_LIB_NAME, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(VAR_FN_LIB_NAME, AUDIT_LAKEHOUSE_NAME)


config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, CONFIG_SCHEMA_NAME, "pipeline_config_param")

param_df = spark.read.format("delta").load(config_abfss_path)\
                    .filter((F.col("active") == 1) & (F.col("silver_notebook") != "") & (F.col("domain") == DOMAIN))\
                    .select("service", "operation", "silver_notebook")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

operation_to_execute = [op.strip().lower() for op in OPERATION_TO_RUN.split(",") if op.strip() != ""]

print("operation_to_execute:", operation_to_execute)
print("OPERATION_TO_RUN:", OPERATION_TO_RUN)

# If OPERATION_TO_RUN length (after stripping) is less than 5, use full param_df
if len(OPERATION_TO_RUN.strip()) < 5:
    filtered_df = param_df
else:
    # Otherwise, filter based on the provided notebook names
    filtered_df = param_df.filter(F.lower(F.col("operation")).isin(operation_to_execute))

nb_names = [
    {
        "notebook_name": row["silver_notebook"],
        "service_name": row["service"],
        "operation_name": row["operation"],
    }
    for row in filtered_df.collect()
]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities" : [],
    "timeoutInSeconds": 43200, # max timeout for the entire DAG, default to 12 hours
    "concurrency": CONCURRENCY # max number of notebooks to run concurrently, default to 3x CPU cores, 0 means unlimited
}



for idx, nb_obj in enumerate(nb_names):
    
    DAG["activities"].append(
        {
            "name": nb_obj["notebook_name"],  # activity name, must be unique
            "path": nb_obj["notebook_name"],            # notebook item name
            "timeoutPerCellInSeconds": 1200, # max timeout for each cell
            "args": {
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
                "SERVICE_NAME": nb_obj["service_name"],
                "OPERATION_NAME": nb_obj["operation_name"],
                "DOMAIN" : DOMAIN,
                "SOURCE_NAME" : SOURCE_NAME,
                "VAR_FN_LIB_NAME" : VAR_FN_LIB_NAME,
                "AUDIT_LAKEHOUSE_NAME" : AUDIT_LAKEHOUSE_NAME,
                "CONFIG_SCHEMA_NAME" : CONFIG_SCHEMA_NAME,
                "CONFIG_TABLE_NAME" : CONFIG_TABLE_NAME,
                "useRootDefaultLakehouse": True,
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
