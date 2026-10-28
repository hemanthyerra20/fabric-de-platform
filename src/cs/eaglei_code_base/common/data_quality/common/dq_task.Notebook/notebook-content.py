# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run dq_library

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

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import uuid
import json
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

RUN_ID = ""
JOB_NAME = ""
RULE_GROUP = ""
RULES = ""
VAR_LIB_NAME = ""
AUDIT_SCHEMA_NAME = ""
DQ_FAILED_RECORDS_TABLE_NAME = ""
DQ_RUN_RESULT_TABLE_NAME = ""
CONTROL_LOG_TABLE_NAME = ""
AUDIT_LAKEHOUSE_NAME = ""
LABEL = ""
WORKSPACE_NAME = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

RULES = json.loads(RULES)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_inc_watermark_value(**kwargs):
    workspace_id = get_variable(kwargs.__getitem__("var_lib_name"), "platform_monitoring_workspace_id")
    lakehouse_id = get_lakehouse_id(kwargs.__getitem__("var_lib_name"), kwargs.__getitem__("audit_lakehouse_name"))

    abfss_path = abfss_path_builder(workspace_id, lakehouse_id, kwargs.__getitem__("audit_schema_name"), kwargs.__getitem__("audit_table_name"))

    df = spark.read.format("delta")\
            .load(abfss_path)

    filtered_df = df.filter(
        (F.col("process_name") == kwargs.__getitem__("process_name"))
        & (F.col("lakehouse_name") == kwargs.__getitem__("lakehouse_name"))
        & (F.col("schema_name") == kwargs.__getitem__("schema_name"))
        & (F.col("table_name") == kwargs.__getitem__("table_name"))
        & (F.col("status") == "success")
    )

    inc_ts = filtered_df.select(F.max("inc_watermark")).first()[0]

    return inc_ts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_control_log_record(**kwargs: dict[str, any]):

    workspace_id = get_variable(kwargs.__getitem__("var_lib_name"), "platform_monitoring_workspace_id")
    lakehouse_id = get_lakehouse_id(kwargs.__getitem__("var_lib_name"), kwargs.__getitem__("audit_lakehouse_name"))

    abfss_path = abfss_path_builder(workspace_id, lakehouse_id, kwargs.__getitem__("audit_schema_name"), kwargs.__getitem__("audit_table_name"))

    cols = {
        "id": F.lit(kwargs.__getitem__("id")),
        "run_id": F.lit(kwargs.__getitem__("run_id")),
        "process_name": F.lit(kwargs.__getitem__("process_name")),
        "lakehouse_name":  F.lit(kwargs.__getitem__("lakehouse_name")),
        "schema_name":  F.lit(kwargs.__getitem__("schema_name")),
        "table_name": F.lit(kwargs.__getitem__("table_name")),
        "inc_column_name": F.lit(kwargs.__getitem__("inc_column_name")),
        "inc_watermark":  F.lit(kwargs.__getitem__("inc_watermark")),
        "workspace_name":  F.lit(kwargs.__getitem__("workspace_name")),
        "label":  F.lit(kwargs.__getitem__("label")),
        "status":  F.lit(kwargs.__getitem__("status")),
        "created_on": F.lit(F.current_timestamp()),
        "created_by": F.lit(kwargs.__getitem__("created_by")),
    }    

    audit_df = spark.range(1).select(
        *[expr.alias(col_name) for col_name, expr in cols.items()]
    )

    audit_df.write.format("delta")\
            .mode("append")\
            .option("mergeSchema", True)\
            .save(abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dq_run_result_record(**kwargs: dict[str, any]):

    workspace_id = get_variable(kwargs.__getitem__("var_lib_name"), "platform_monitoring_workspace_id")
    lakehouse_id = get_lakehouse_id(kwargs.__getitem__("var_lib_name"), kwargs.__getitem__("audit_lakehouse_name"))

    abfss_path = abfss_path_builder(workspace_id, lakehouse_id, kwargs.__getitem__("audit_schema_name"), kwargs.__getitem__("audit_table_name"))
    
    cols = {
        "id": F.lit(kwargs.__getitem__("id")),
        "run_id": F.lit(kwargs.__getitem__("run_id")),
        "rule_id": F.lit(kwargs.__getitem__("rule_id")),
        "rule_group":  F.lit(kwargs.__getitem__("rule_group")),
        "description": F.lit(kwargs.__getitem__("description")),
        "custom_rule": F.lit(kwargs.__getitem__("custom_rule")),
        "lakehouse_name": F.lit(kwargs.__getitem__("lakehouse_name")),
        "schema_name":  F.lit(kwargs.__getitem__("schema_name")),
        "table_name": F.lit(kwargs.__getitem__("table_name")),
        "column_name": F.lit(kwargs.__getitem__("column_name")),
        "workspace_name": F.lit(kwargs.__getitem__("workspace_name")),
        "label": F.lit(kwargs.__getitem__("label")),
        "status":  F.lit(kwargs.__getitem__("status")),
        "created_on": F.lit(F.current_timestamp()),
        "created_by": F.lit(kwargs.__getitem__("created_by")),
    }

    if kwargs.__getitem__("status") == "success":
        cols["total_rows"] =  F.lit(kwargs.__getitem__("total_rows"))
        cols["success_rows"] =  F.lit(kwargs.__getitem__("success_rows"))
        cols["failed_rows"] =  F.lit(kwargs.__getitem__("failed_rows"))
        cols["success_rows"] =  F.lit(kwargs.__getitem__("success_rows"))
    else:
        cols["error_message"] =  F.lit(kwargs.__getitem__("error_message"))

    

    audit_df = spark.range(1).select(
        *[expr.alias(col_name) for col_name, expr in cols.items()]
    )

    audit_df.write.format("delta")\
            .mode("append")\
            .option("mergeSchema", True)\
            .save(abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_dq_failed_record(df: DataFrame, **kwargs: dict[str, any]):
    
    workspace_id = get_variable(kwargs.__getitem__("var_lib_name"), "platform_monitoring_workspace_id")
    lakehouse_id = get_lakehouse_id(kwargs.__getitem__("var_lib_name"), kwargs.__getitem__("audit_lakehouse_name"))

    abfss_path = abfss_path_builder(workspace_id, lakehouse_id, kwargs.__getitem__("audit_schema_name"), kwargs.__getitem__("audit_table_name"))


    cols = {
        "result_id": F.lit(kwargs.__getitem__("result_id")),
        "rule_id": F.lit(kwargs.__getitem__("rule_id")),
        "run_id": F.lit(kwargs.__getitem__("run_id")),
        "rule_group": F.lit(kwargs.__getitem__("rule_group")),
        "workspace_name": F.lit(kwargs.__getitem__("workspace_name")),
        "label": F.lit(kwargs.__getitem__("label")),
        "description": F.lit(kwargs.__getitem__("description")),
        "lakehouse_name":  F.lit(kwargs.__getitem__("lakehouse_name")),
        "schema_name":  F.lit(kwargs.__getitem__("schema_name")),
        "table_name": F.lit(kwargs.__getitem__("table_name")),
        "column_name": F.lit(kwargs.__getitem__("column_name")),
        "failure_reason":  F.lit(kwargs.__getitem__("failure_reason")),
        "created_on": F.lit(F.current_timestamp()),
        "created_by": F.lit(kwargs.__getitem__("created_by")),
        
    }

    audit_df = df.withColumns(cols)

    audit_df.write.format("delta")\
            .mode("append")\
            .save(abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_watermark_cols(rule_id, lh_name, schema_name, table_name, inc_col_name, status):
    return  {
                "id": F.expr("uuid()"),
                "run_id": RUN_ID,
                "process_name":f"DataQualityCheck#{rule_id}#{RULE_GROUP}",
                "lakehouse_name":  lh_name,
                "schema_name":  schema_name,
                "table_name": table_name,
                "inc_column_name": inc_col_name,
                "inc_watermark": datetime.now(),
                "workspace_name": WORKSPACE_NAME,
                "label": LABEL,
                "status": status,
                "created_by": JOB_NAME,
                "audit_lakehouse_name": AUDIT_LAKEHOUSE_NAME,
                "audit_schema_name": AUDIT_SCHEMA_NAME,
                "audit_table_name": CONTROL_LOG_TABLE_NAME,
                "var_lib_name": VAR_LIB_NAME
                }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for rule in RULES:
    result_id = generate_guid()
    for details in rule["rule_details"]:
        try:
            cols = dict()
            if "is_custom_rule" in details and details["is_custom_rule"]:
                lh_name, schema_name, table_name = "", "", ""
                tables = set()
                for k, v in details["placeholders"].items():
                    vw_name = f"{v.replace('.', '_')}_temp_vw"
                    details["placeholders"][k] = vw_name
                    if v not in tables and k.startswith("full_table_name"):
                        tables.add(v)
                        c_lh_name, c_schema_name, c_table_name = v.split(".")
                        workspace_id = get_workspace_id(VAR_LIB_NAME, c_lh_name.split("_")[1], "store")
                        lakehouse_id = get_lakehouse_id(VAR_LIB_NAME, c_lh_name)
                        abfss_path = abfss_path_builder(workspace_id, lakehouse_id, c_schema_name, c_table_name)
                        raw_df = spark.read.format("delta").load(abfss_path)
                        raw_df.createOrReplaceTempView(vw_name)
                        lh_name += c_lh_name + ","
                        schema_name += c_schema_name + ","
                        table_name += c_table_name + ","

                watermark_cols = get_watermark_cols(rule.get("rule_id", ""), lh_name, schema_name, table_name, 
                                        "", "success")
                query = replace_query_placeholder(details["placeholders"], details["query"])
                total_count_query = replace_query_placeholder(details["placeholders"], details["total_count_query"])
                failure_reason = replace_query_placeholder(details["placeholders"], details["failure_reason"])
                inc_ts = get_inc_watermark_value(**watermark_cols)
                inc_ts = inc_ts if inc_ts else '1999-01-01'
                query = query.replace("${inc_watermark_ts}", f"'{inc_ts}'")
                total_count_query = total_count_query.replace("${inc_watermark_ts}", f"'{inc_ts}'")
                raw_df = spark.sql(total_count_query)
                filtered_df = spark.sql(query)
            
            else:
                lh_name, schema_name, table_name = details["full_table_name"].split(".")
                temp_view_name = f"temp_vw_{schema_name}_{table_name}"
                inc_temp_view_name = f"temp_vw_inc_{schema_name}_{table_name}"
                lib = dq_lib[details["rule_lib_name"]]
                details["full_table_name"] = temp_view_name
                watermark_cols = get_watermark_cols(rule.get("rule_id", ""), lh_name, schema_name, table_name, 
                                        details.get("inc_column_name"), "success")
                # Apply placeholder replacement using the column rule config
                query = replace_query_placeholder(details, lib["query"])
                failure_reason = replace_query_placeholder(details, lib["failure_reason"])
                workspace_id = get_workspace_id(VAR_LIB_NAME, lh_name.split("_")[1], "store")
                lakehouse_id = get_lakehouse_id(VAR_LIB_NAME, lh_name)
                abfss_path = abfss_path_builder(workspace_id, lakehouse_id, schema_name, table_name)
                raw_df = spark.read.format("delta").load(abfss_path)
                if details.get("load_type", None) == "delta":
                    inc_ts = get_inc_watermark_value(**watermark_cols)
                    if inc_ts:
                        cond_lst = [f"{x.strip()} >= '{inc_ts}'" for x in details.get("inc_column_name").split(",")]
                        filter_cond = " OR ".join(cond_lst)
                        raw_df.createOrReplaceTempView(inc_temp_view_name)
                        filtered_df = spark.sql(f"SELECT * FROM {inc_temp_view_name} WHERE {filter_cond}")
                    else:
                        raw_df.createOrReplaceTempView(temp_view_name)
                        filtered_df = spark.sql(query)
                else:
                    raw_df.createOrReplaceTempView(temp_view_name)
                    filtered_df = spark.sql(query)
            

            converted_df = row_to_map(filtered_df)
            cols = {
                "run_id": RUN_ID,
                "rule_id": rule.get("rule_id", ""),
                "rule_group": RULE_GROUP,
                "description": lib.get("description", ""),
                "lakehouse_name":  lh_name,
                "schema_name":  schema_name,
                "table_name": table_name,
                "column_name": details.get("column_name", ""),
                "created_by": JOB_NAME,
                "audit_schema_name": AUDIT_SCHEMA_NAME,
                "audit_lakehouse_name": AUDIT_LAKEHOUSE_NAME,
                "custom_rule": details.get("custom_rule"),
                "workspace_name": WORKSPACE_NAME,
                "label": LABEL,
                "var_lib_name": VAR_LIB_NAME
            }

            failed_records_dict = {
                **cols,
                "audit_table_name": DQ_FAILED_RECORDS_TABLE_NAME,
                "failure_reason":  failure_reason,
                "result_id": result_id

            }
            
            total_rows = raw_df.count()
            failed_rows = filtered_df.count()
            success_rows = total_rows - failed_rows
            run_result_dict = {
                **cols,
                "status": "success",
                "total_rows": total_rows,
                "failed_rows": failed_rows,
                "success_rows": success_rows,
                "audit_table_name": DQ_RUN_RESULT_TABLE_NAME,
                "id": result_id

            }
            create_dq_run_result_record(**run_result_dict)
            create_dq_failed_record(converted_df, **failed_records_dict)
            create_control_log_record(**watermark_cols)
        except Exception as e:
            run_result_dict = {
                **cols,
                "status": "failed",
                "audit_table_name": DQ_RUN_RESULT_TABLE_NAME,
                "id": result_id
            }
            watermark_cols["status"] = "failed"
            create_dq_run_result_record(**run_result_dict)
            create_control_log_record(**watermark_cols)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
