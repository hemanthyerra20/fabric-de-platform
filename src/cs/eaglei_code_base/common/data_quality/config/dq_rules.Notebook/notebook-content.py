# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

dq_rules = [{
    "rule_group": "Contact",
    "rules": [
        {
        "rule_id": "R1",
        "rule_details": [
            {
                "full_table_name": "lh_cs_bronze.dbo.employee",
                "column_name": "id",
                "is_custom_rule": False,
                "rule_lib_name": "ExceptColumnValuesNotToBeNull",
                "load_type" : "delta",
                "inc_column_name": "modified_on"
            },
            {
                "full_table_name": "lh_cs_bronze.dbo.employee",
                "column_name": "age",
                "is_custom_rule": False,
                "rule_lib_name": "ExceptColumnValuesNotToBeNull",
                "load_type" : "delta",
                "inc_column_name": "modified_on"
            }
        ]
    },
    {
        "rule_id": "R2",
        "rule_details": [
            {
                "full_table_name": "lh_cs_bronze.dbo.employee",
                "is_custom_rule": False,
                "column_name": "email",
                "rule_lib_name": "ExceptValidEmailFormat",
                "load_type" : "delta",
                "inc_column_name": "modified_on"
            }
        ]
    },
    {
        "rule_id": "R3",
        "rule_details": [
            {
                "full_table_name": "lh_cs_bronze.dbo.employee",
                "column_name": "phone_number",
                "is_custom_rule": False,
                "rule_lib_name": "ExceptColumnValuesNotToBeNull",
                "load_type" : "delta",
                "inc_column_name": "modified_on"
            }
        ]

    },
    {
        "rule_id": "R4",
        "rule_details": [
            {
                "is_custom_rule": True,
                "placeholders": {
                    "full_table_name_emp" : "lh_cs_bronze.dbo.employee",
                    "full_table_name_proj": "lh_cs_bronze.dbo.project"
                },
                "query": "SELECT E.* FROM ${full_table_name_emp} E LEFT JOIN ${full_table_name_proj} P ON E.id = P.employee_id WHERE P.employee_id IS NULL AND (P.modified_on >= ${inc_watermark_ts} OR E.modified_on >= ${inc_watermark_ts})",
                "total_count_query": "SELECT E.* FROM ${full_table_name_emp} E LEFT JOIN ${full_table_name_proj} P ON E.id = P.employee_id WHERE P.modified_on >= ${inc_watermark_ts} OR E.modified_on >= ${inc_watermark_ts}",
                "failure_reason": "Project is not assigned to employee"
            }
        ]

    }
    ]
},
{}]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
