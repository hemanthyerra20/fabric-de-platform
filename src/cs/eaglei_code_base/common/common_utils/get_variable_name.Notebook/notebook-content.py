# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Dictionary of known variable names → values
variable_name_map = {
    "var_cs_name": "var_cs_process",
    "audit_lakehouse_name": "lh_platform_monitoring",
    "audit_schema_name": "audit",
    "dq_rules_table_name": "dq_rules",
    "dq_run_results_table_name": "dq_run_results",
    "dq_failed_records_table_name": "dq_failed_records",
    "control_log_table_name": "control_log",
    "workspace_name": notebookutils.runtime.context['currentWorkspaceName'],
    "label": notebookutils.runtime.context['currentWorkspaceName'].split("-")[1].lower(),
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_variable_name(var_name: str):
    """
    Return the value for a given variable name from variable_name_map.

    Parameters
    ----------
    var_name : str
        The key to look up in variable_name_map.

    Returns
    -------
    Any
        The corresponding value if the key exists.

    Raises
    ------
    KeyError
        If var_name is not found in variable_name_map.
    """
    if var_name in variable_name_map:
        return variable_name_map[var_name]
    else:
        # Include the list of valid keys in the error for easier debugging
        available = list(variable_name_map.keys())
        raise KeyError(f"{var_name} not found in variable_name_map. Available keys: {available}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
