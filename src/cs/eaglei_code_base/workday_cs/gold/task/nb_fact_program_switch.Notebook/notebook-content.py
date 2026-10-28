# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "08846ef1-977d-4698-b06f-84fdbd834bc6",
# META       "default_lakehouse_name": "lh_cs_silver",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "08846ef1-977d-4698-b06f-84fdbd834bc6"
# META         },
# META         {
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "f45c2c8e-a6e5-4098-aaf6-567ab6b6ab98",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

%run nb_pipeline_monitoring_utils

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

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, ArrayType
import json
from datetime import date
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

VAR_CS_LIB_NAME = "var_cs_process"
PIPELINE_NAME = "nb_gold_layer_ingestion"
MASTER_RUN_ID = "test123421"
RUN_ID = "test123421"
PIPELINE_NAME = "test123421"
NOTEBOOK_NAME = "test123421"
DOMAIN = "cs"
SOURCE_NAME = "workday"
ACTIVITY_NAME = "notebook"
LEVEL = "cs"
LAYER = "gold"
ENVIRONMENT = "dev"
WORKSPACE_NAME = "EAGLEI-CS-PROCESS-DEV"
WORKSPACE_ID = "088bba09-dd86-49a0-87f3-ad9331892945"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

store_workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_program_switch"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    #===========================================================
    # Silver Path
    #===========================================================
    program_switch_sil_path = abfss_path_builder(store_workspace_id, silver_lakehouse_id, silver_schema_name, "program_of_study_all")
    dim_student_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    dim_program_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")
    dim_college_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")
    dim_academic_period_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_date_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_date")

    program_switch_silver_df = spark.read.format("delta").load(program_switch_sil_path)

    dim_student_gold_df = spark.read.format("delta").load(dim_student_path).filter(F.col("is_current") == 1)

    window_spec = Window.partitionBy(F.trim("program_name")).orderBy(F.col("program_key").desc())

    dim_program_gold_df = (
        spark.read.format("delta")
        .load(dim_program_path)
        .withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    dim_college_gold_df = spark.read.format("delta").load(dim_college_path)

    dim_academic_period_gold_df = spark.read.format("delta").load(dim_academic_period_path)

    dim_date_gold_df = spark.read.format("delta").load(dim_date_path)
except Exception as e:
    print("Error in Silver Path cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    clean_df = program_switch_silver_df.filter(
        ~F.coalesce(F.col("reason"), F.lit(""))
        .isin("Data conversion Mass Discontinue", "Duplicate Matriculated Student")
    )

    primary_candidates_df = (
        clean_df
        .filter(F.col("is_primary_program") == 1)
        .withColumn(
            "status_priority",
            F.when(F.col("program_status") == "In Progress", 1)
            .when(F.col("program_status") == "Matriculated", 2)
            .when(F.col("program_status") == "Pending Completion", 3)
            .when(F.col("program_status") == "Complete", 4)
            .when(F.col("program_status") == "Discontinued", 5)
            .when(F.col("program_status") == "Dismissed", 6)
            .when(F.col("program_status") == "Institutional Withdrawal", 7)
            .otherwise(99)
        )
    )

    primary_window = (
        Window
        .partitionBy("student_id", "effective_date")
        .orderBy(
            F.col("status_priority").asc(),
            F.col("created_moment").desc(),
            F.col("pos_key").desc(),
        )
    )

    primary_by_date_df = (
        primary_candidates_df
        .withColumn("rn", F.row_number().over(primary_window))
        .filter(F.col("rn") == 1)
        .drop("rn", "status_priority")
    )
except Exception as e:
    print("Error in primary_by_date_df cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    window1 = Window.partitionBy("student_id").orderBy(F.col("effective_date"), F.col("program_status"))

    window2 = (
        Window
        .partitionBy("student_id", "program_of_study")
        .orderBy(F.col("effective_date"), F.col("program_status"))
    )

    islands_df = (
        primary_by_date_df
        .withColumn("rn1", F.row_number().over(window1))
        .withColumn("rn2", F.row_number().over(window2))
        .withColumn("grp", F.col("rn1") - F.col("rn2"))
        .drop("rn1", "rn2")
    )
except Exception as e:
    print("Error in islands_df cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    last_record_window = (
        Window
        .partitionBy("student_id", "program_of_study", "grp")
        .orderBy(F.col("effective_date").desc())
    )

    last_record_df = (
        islands_df
        .withColumn("rn", F.row_number().over(last_record_window))
        .filter(F.col("rn") == 1)
        .select(
            "student_id",
            "program_of_study",
            "grp",
            F.col("program_status").alias("end_status"),
            F.col("reason").alias("end_reason"),
            F.col("declare_date").alias("span_declare_date"),
        )
    )

    display(last_record_df)
except Exception as e:
    print("Error in last_record_df cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    spans_df = (
        islands_df.alias("I")
        .join(
            last_record_df.alias("L"),
            (
                (F.col("I.student_id") == F.col("L.student_id"))
                & (F.col("I.program_of_study") == F.col("L.program_of_study"))
                & (F.col("I.grp") == F.col("L.grp"))
            ),
            "left",
        )
        .groupBy(
            F.col("I.student_id"),
            F.col("I.academic_unit"),
            F.col("I.program_of_study"),
            F.col("I.grp"),
            F.col("L.end_status"),
            F.col("L.end_reason"),
            F.col("L.span_declare_date"),
        )
        .agg(
            F.min("effective_date").alias("span_start"),
            F.min("declare_date").alias("declare_date"),
        )
        .withColumnRenamed("program_of_study", "primary_program")
    )
except Exception as e:
    print("Error in spans_df cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    seq_window = Window.partitionBy("student_id").orderBy("span_start")

    seq_df = (
        spans_df
        .withColumn("previous_program", F.lag("primary_program").over(seq_window))
        .withColumn("previous_status", F.lag("end_status").over(seq_window))
        .withColumn("previous_reason", F.lag("end_reason").over(seq_window))
        .withColumn("previous_declare_date", F.lag("declare_date").over(seq_window))
        .withColumnRenamed("primary_program", "new_program")
        .withColumnRenamed("span_start", "switch_date")
    )

    program_switch_df = (
        seq_df
        .filter(F.col("previous_program").isNotNull())
        .filter(F.trim(F.col("previous_program")) != F.trim(F.col("new_program")))
        .filter(~F.col("previous_program").like("Undeclared%"))
        .filter(~F.col("new_program").like("Undeclared%"))
        .filter(~F.coalesce(F.col("previous_status"), F.lit("")).isin("Complete", "Pending Completion"))
    )
except Exception as e:
    print("Error in program_switch_df cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    dim_student_df = (
        spark.read.format("delta")
        .load(dim_student_path)
        .filter(F.col("is_current") == 1)
        .select("student_key", "student_id")
    )

    program_window = Window.partitionBy(F.trim("program_name")).orderBy(F.col("program_key").desc())

    dim_program_df = (
        spark.read.format("delta")
        .load(dim_program_path)
        .withColumn("rn", F.row_number().over(program_window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select("program_key", "program_name")
    )

    dim_college_df = (
        spark.read.format("delta")
        .load(dim_college_path)
        .select("college_key", "name")
    )

    dim_academic_period_df = (
        spark.read.format("delta")
        .load(dim_academic_period_path)
        .select("academic_period_key", "period_start_date", "period_end_date")
    )

    dim_date_df = (
        spark.read.format("delta")
        .load(dim_date_path)
        .select("date_key", "calendar_date")
    )
except Exception as e:
    print("Error in dimension load cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    joined_df = (
        program_switch_df.alias("PS")
        .join(dim_student_gold_df.alias("STU"), F.col("PS.student_id") == F.col("STU.student_id"), "left")
        .join(
            dim_program_gold_df.alias("NEW_PROGRAM"),
            F.trim(F.col("PS.new_program")) == F.trim(F.col("NEW_PROGRAM.program_name")),
            "left",
        )
        .join(
            dim_program_gold_df.alias("PREV_PROGRAM"),
            F.trim(F.col("PS.previous_program")) == F.trim(F.col("PREV_PROGRAM.program_name")),
            "left",
        )
        .join(dim_college_gold_df.alias("COLLEGE"), F.col("PS.academic_unit") == F.col("COLLEGE.name"), "left")
        .join(
            dim_academic_period_gold_df.alias("PERIOD"),
            (F.col("PS.switch_date") >= F.col("PERIOD.period_start_date"))
            & (F.col("PS.switch_date") <= F.col("PERIOD.period_end_date")),
            "left",
        )
        .join(
            dim_date_gold_df.alias("DATE"),
            F.to_date(F.col("PS.switch_date")) == F.col("DATE.calendar_date"),
            "left",
        )
        .select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("STU.student_key").cast("string"), F.lit("")),
                    F.coalesce(F.col("PREV_PROGRAM.program_key").cast("string"), F.lit("")),
                    F.coalesce(F.col("NEW_PROGRAM.program_key").cast("string"), F.lit("")),
                    F.coalesce(F.col("COLLEGE.college_key").cast("string"), F.lit("")),
                    F.coalesce(F.col("PERIOD.academic_period_key").cast("string"), F.lit("")),
                    F.coalesce(F.col("DATE.date_key").cast("string"), F.lit("")),
                ),
                256,
            ).alias("program_switch_key"),
            F.col("STU.student_key"),
            F.col("PREV_PROGRAM.program_key").alias("previous_program_key"),
            F.col("NEW_PROGRAM.program_key").alias("new_program_key"),
            F.col("COLLEGE.college_key"),
            F.col("PERIOD.academic_period_key"),
            F.col("DATE.date_key").alias("switch_date_key"),
        )
    )
except Exception as e:
    print("Error in joined_df cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    fact_program_switch_df = (
        program_switch_df.alias("PS")
        .join(dim_student_df.alias("ST"), F.col("PS.student_id") == F.col("ST.student_id"), "left")
        .join(
            dim_program_df.alias("NP"),
            F.trim(F.col("PS.new_program")) == F.trim(F.col("NP.program_name")),
            "left",
        )
        .join(
            dim_program_df.alias("PP"),
            F.trim(F.col("PS.previous_program")) == F.trim(F.col("PP.program_name")),
            "left",
        )
        .join(dim_college_df.alias("DC"), F.col("PS.academic_unit") == F.col("DC.name"), "left")
        .join(
            dim_academic_period_df.alias("DAP"),
            (F.col("PS.switch_date") >= F.col("DAP.period_start_date"))
            & (F.col("PS.switch_date") <= F.col("DAP.period_end_date")),
            "left",
        )
        .join(
            dim_date_df.alias("DD"),
            F.to_date(F.col("PS.switch_date")) == F.col("DD.calendar_date"),
            "left",
        )
    )

    fact_program_switch_df = fact_program_switch_df.withColumn(
        "program_switch_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("ST.student_key").cast("string"), F.lit("")),
                F.coalesce(F.col("PP.program_key").cast("string"), F.lit("")),
                F.coalesce(F.col("NP.program_key").cast("string"), F.lit("")),
                F.coalesce(F.col("DC.college_key").cast("string"), F.lit("")),
                F.coalesce(F.col("DAP.academic_period_key").cast("string"), F.lit("")),
                F.coalesce(F.col("DD.date_key").cast("string"), F.lit("")),
            ),
            256,
        ),
    )
except Exception as e:
    print("Error in fact_program_switch_df build cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    fact_program_switch_df = fact_program_switch_df.select(
        "program_switch_key",
        F.col("ST.student_key").alias("student_key"),
        F.col("PP.program_key").alias("previous_program_key"),
        F.col("NP.program_key").alias("new_program_key"),
        F.col("DC.college_key").alias("college_key"),
        F.col("DAP.academic_period_key").alias("academic_period_key"),
        F.col("DD.date_key").alias("switch_date_key"),
    )

    fact_program_switch_df = add_audit_column(fact_program_switch_df, PIPELINE_NAME)
    fact_program_switch_df.cache()

    final_count = fact_program_switch_df.count()
    rows_read = final_count
    rows_written = final_count

    fact_program_switch_df.write.format("delta").mode("overwrite").save(
        abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "fact_program_switch")
    )

    fact_program_switch_df.unpersist()
except Exception as e:
    print("Error in final write cell:", e)
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = datetime.now()
insert_pipeline_monitoring_record( 
    _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
    "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), rows_read, rows_written, 0,
    start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
    rerun_count=0
)
print("SUCCESS logged")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
