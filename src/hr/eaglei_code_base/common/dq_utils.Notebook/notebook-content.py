# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import uuid

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_variable(lib_name: str, var_name: str):
    """
    Retrieve a variable from a Fabric Variable Library.

    Parameters
    ----------
    lib_name : str
        Name of the variable library to load (as defined in Fabric).
    var_name : str
        Name of the variable (attribute) to retrieve from that library.

    Returns
    -------
    Any
        The value of the requested variable.

    Raises
    ------
    AttributeError
        If the variable does not exist in the specified library.
        The exception includes a list of available variables as context.
    """
    # Load the variable library object by its name
    vl = notebookutils.variableLibrary.getLibrary(lib_name)

    try:
        # Try to get the attribute (variable) from the library object
        return getattr(vl, var_name)
    except AttributeError:
        # If the attribute does not exist, build a list of available variables
        # Skip internal attributes (starting with "_") and the method "getVariable"
        vars = [
            name
            for name in dir(vl)
            if not name.startswith("_") and name != "getVariable"
        ]

        # Raise a more informative AttributeError including the list of valid variable names
        raise AttributeError(
            f"{var_name} not available in {lib_name} variable library",
            vars
        )


def get_workspace_id(var_lib_name: str, workspace_name: str, layer_name: str):
    """
    Build the variable name for a workspace ID and fetch it from a Fabric Variable Library.

    Example expected variable name in the library:
        "<workspace_name>_<layer_name>_workspace_id"
        e.g. "sales_bronze_workspace_id"

    Parameters
    ----------
    var_lib_name : str
        Name of the variable library where workspace IDs are stored.
    workspace_name : str
        Logical name of the workspace (e.g. "sales").
    layer_name : str
        Data layer name (e.g. "bronze", "silver", "gold").

    Returns
    -------
    Any
        The value stored under the constructed variable name.
    """
    # Construct the variable name following the naming convention
    var_name = f"{workspace_name}_{layer_name}_workspace_id"
    # Retrieve the value from the variable library using the helper function
    value = get_variable(var_lib_name, var_name)
    return value


def get_lakehouse_id(var_lib_name: str, lakehouse_name: str):
    """
    Build the variable name for a lakehouse ID and fetch it from a Fabric Variable Library.

    Example expected variable name in the library:
        "<lakehouse_name>_id"
        e.g. "lh_platform_monitoring_id"

    Parameters
    ----------
    var_lib_name : str
        Name of the variable library where lakehouse IDs are stored.
    lakehouse_name : str
        Logical name of the lakehouse.

    Returns
    -------
    Any
        The value stored under the constructed variable name.
    """
    # Construct the variable name following the naming convention
    var_name = f"{lakehouse_name}_id"
    # Retrieve the value from the variable library using the helper function
    value = get_variable(var_lib_name, var_name)
    return value


def replace_query_placeholder(conf: dict, query: str) -> str:
    """
    Replace placeholders in a query string using values from a configuration dictionary.

    Placeholders in the query must be in the form: ${key}
    where 'key' is a key in the 'conf' dictionary.

    Example:
        conf = {"TABLE_NAME": "customers"}
        query = "SELECT * FROM ${TABLE_NAME}"
        => "SELECT * FROM customers"

    Parameters
    ----------
    conf : dict
        Dictionary of key/value pairs used for substitution.
    query : str
        SQL or text template containing ${key} placeholders.

    Returns
    -------
    str
        Query string with all matching placeholders replaced.
    """
    # Iterate over all key/value pairs and perform string replacement
    for k, v in conf.items():
        query = query.replace(f"${{{k}}}", str(v))
    return query


def abfss_path_builder(workspace_id: str, lakehouse_id: str, schema_name: str, table_name: str):
    """
    Construct an ABFSS path for a table stored in a Fabric Lakehouse.

    The resulting path follows the pattern:
        abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema_name}/{table_name}

    This can be used, for example, with Spark read/write operations.

    Parameters
    ----------
    workspace_id : str
        Workspace GUID where the lakehouse resides.
    lakehouse_id : str
        Lakehouse GUID (artifact ID).
    schema_name : str
        Schema name under Tables (e.g. 'dbo' or 'audit').
    table_name : str
        Table name.

    Returns
    -------
    str
        Fully qualified ABFSS path to the target table.
    """
    # Base ABFSS endpoint for the workspace in OneLake
    base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com"
    # Append lakehouse and table-specific path
    return f"{base_path}/{lakehouse_id}/Tables/{schema_name}/{table_name}"

def generate_guid() -> str:
    """ Return a single GUID (UUID v4)"""
    return str(uuid.uuid4())

def row_to_map(df: DataFrame) -> DataFrame:
    """
    Converts each row of a spark Dataframe into a map (dictonary-like structure)

    Example output per row:
    {id -> 1, a -> 10, b -> 20}
    """

    cols = df.columns

    map_expr = []
    for c in cols:
        map_expr.extend([F.lit(c), df[c]])

    target_df = df.select(F.expr("uuid()").alias("id"), F.create_map(*map_expr).cast("string").alias("source_data"))
    return target_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
