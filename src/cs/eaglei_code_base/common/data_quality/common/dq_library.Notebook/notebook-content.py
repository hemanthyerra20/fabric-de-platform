# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

dq_lib = {
    "ExceptColumnValuesNotToBeNull": 
        {
            "id": "DQ1",
            "description": "Column values should not be null",
            "query": "SELECT * FROM ${full_table_name} WHERE ${column_name} IS NULL",
            "failure_reason": "Column value is null"
        },
    "ExceptColumnValuesToBeBetween":
        {
            "id": "DQ2",
            "description": "Column value should be between min_value and max_value",
            "query": "SELECT * FROM ${full_table_name} WHERE NOT ${column_name} BETWEEN ${min_value} AND ${max_value}",
            "failure_reason": "value is outside allowed range [${min_value}, ${max_value}]"
        },
    "ExceptValidEmailFormat": 
        {
            "id": "DQ3",
            "description": "Email should be in the valid format",
            "query": "SELECT * FROM ${full_table_name} WHERE ${column_name} IS NULL OR "
                    "${column_name} NOT RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' ",
            "failure_reason": "Email id is not in correct format"
        },
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
