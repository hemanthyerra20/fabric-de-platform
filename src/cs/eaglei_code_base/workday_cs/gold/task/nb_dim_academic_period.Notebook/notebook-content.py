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

# MARKDOWN ********************

# ### Silver layer tables (used with `abfss_path_builder`)
# - maintained_academic_periods  
# - academic_period_date_controls  
# - academic_pell_recalc  
# 
# ### Gold layer tables (used with `abfss_path_builder`)
# - dim_academic_period


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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

VAR_CS_LIB_NAME = "var_cs_process"
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = ""
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = ""
LEVEL = ""
LAYER = ""
ENVIRONMENT = ""
WORKSPACE_NAME = ""
WORKSPACE_ID = ""

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
target_table = f"lh_cs_gold.{gold_schema_name}.dim_academic_period"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    maintained_academic_periods_sil_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "maintained_academic_periods"
    )
    academic_period_date_controls_sil_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "academic_period_date_controls"
    )
    academic_pell_recalc_silver_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "academic_pell_recalc"
    )
    # ----------------------------------------------------------
    # Read Silver Tables
    # ----------------------------------------------------------
    maintained_academic_periods_df = (
        spark.read
            .format("delta")
            .load(maintained_academic_periods_sil_path)
            .withColumn (
                "period_name",
                F.col("academic_period_name")
            )
            .withColumn(
                "session",
                F.when(
                    F.col("academic_period_name").endswith("Session 1"),
                    F.lit("Session 1")
                )
                .when(
                    F.col("academic_period_name").endswith("Session 2"),
                    F.lit("Session 2")
                )
                .when(
                    F.col("academic_period_name").contains("J Term"),
                    F.lit("J Term")
                )
            )
            .withColumn(
                "is_major_term",
                F.when(
                    (F.col("academic_period_name").endswith("Session 1")) |
                    (F.col("academic_period_name").endswith("Session 2")) |
                    (F.col("academic_period_name").contains("J Term")),
                    F.lit(0)
                ).otherwise(F.lit(1))
            )            
            .withColumn(
                "academic_period_name",
                F.when(
                    F.col("academic_period_name").endswith("Session 1"),
                    F.trim(F.regexp_replace("academic_period_name", "Session 1$", ""))
                )
                .when(
                    F.col("academic_period_name").endswith("Session 2"),
                    F.trim(F.regexp_replace("academic_period_name", "Session 2$", ""))
                )
                .when(
                    F.col("academic_period_name").contains("J Term"),
                    F.trim(F.regexp_replace("academic_period_name", "J Term", "Spring"))
                )
                .otherwise(F.col("academic_period_name"))
            )
            .withColumn(         
                "current_flag",         
                F.when(             
                    (F.current_date() >= F.col("academic_period_start_date")) &            
                    (F.current_date() <= F.col("academic_period_end_date")),             
                    F.lit(1)         
                ).otherwise(F.lit(0))
                    )
    
    )
    academic_period_date_controls_df = (
        spark.read
            .format("delta")
            .load(academic_period_date_controls_sil_path)
            .select(
                "academic_period_id",
                "date_control_id",
                "control_date"
            )

    )
    academic_pell_recalc_df = (
        spark.read
            .format("delta")
            .load(academic_pell_recalc_silver_path)
            .select(
                F.trim(F.col("academicperiod")).alias("academic_period"),
                #F.col("academic_year"),
                F.trim(
                    F.regexp_replace(
                        F.col("academic_year"),
                        "\\s*Academic\\s+Year$",
                        ""
                        ) 
                    ).alias("academic_year"),
                F.to_date(
                    F.col("pell_recalculation_date"),
                    "MM/dd/yyyy"
                ).alias("pell_recalculation_date")
            )
            .dropDuplicates(["academic_period"])

    )
    
    # ----------------------------------------------------------

    # Derive Registration Dates

    # ----------------------------------------------------------
    registration_dates_df = (
        academic_period_date_controls_df
            .groupBy("academic_period_id")
            .agg(
                F.max(
                    F.when(
                        F.col("date_control_id") == "ENROLLMENT_WINDOW_START",
                        F.col("control_date")
                    )
                ).alias("registration_open_date"),
                F.max(
                    F.when(
                        F.col("date_control_id") == "ENROLLMENT_WINDOW_END",
                        F.col("control_date")
                    )
                ).alias("registration_close_date")
            )
    )
    # ----------------------------------------------------------
    # Build Dimension
    # ----------------------------------------------------------
    maintained_academic_periods_silver_df = (
        maintained_academic_periods_df.alias("map")
        .join(
            registration_dates_df.alias("rd"),
            F.col("map.id") == F.col("rd.academic_period_id"),
            "inner"
        )
        .join(
            academic_pell_recalc_df.alias("pr"),
            F.trim(F.col("map.academic_period_name")) == F.col("pr.academic_period"),
            "left"
        )
        .select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("map.id").cast("string"), F.lit(""))
                ),
                256
            ).alias("academic_period_key"),
            F.col("map.id").alias("academic_period_id"),
            F.col("map.period_name").alias ("period_name"),
            F.col("map.academic_period_name").alias("academic_period_name"),
            F.col("map.session").alias("session"),
            F.col("map.is_major_term").alias("is_major_term"),
            F.to_date("map.academic_period_start_date", "yyyy-MM-dd").alias("period_start_date"),
            F.to_date("map.academic_period_end_date", "yyyy-MM-dd").alias("period_end_date"),
            F.col("map.academic_period_weeks_of_instruction").alias("weeks_of_instruction"),
            F.col("map.academic_calendar_id").alias("academic_calendar_id"),
            F.col("pr.academic_year").alias("academic_year"),
            F.to_timestamp(F.col("rd.registration_open_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("registration_open_date"),
            F.to_timestamp(F.col("rd.registration_close_date"), "yyyy-MM-dd'T'HH:mm:ssXXX").alias("registration_close_date"),
            F.col("pr.pell_recalculation_date").alias("pell_recalculation_date"),
            F.col("map.current_flag").alias("current_flag")
        )
        .filter(F.col("academic_period_id").isNotNull())
    )
    

    maintained_academic_periods_silver_df=maintained_academic_periods_silver_df.distinct()
    # ----------------------------------------------------------
    # Add Audit Columns
    # ----------------------------------------------------------

    non_sess_df = maintained_academic_periods_silver_df.filter(F.col("session").isNull())
    sess_df =  maintained_academic_periods_silver_df.filter(F.col("session").isNotNull())

    # Apply term/curation logic for non-session academic periods
    non_sess_curated_df = (
        non_sess_df
            # Term sequence based on academic_period_name
            .withColumn(
                "term_sequence",
                F.when(F.col("academic_period_name").contains("Spring"), F.lit(1))
                .when(F.col("academic_period_name").contains("Fall"),   F.lit(3))
                .when(F.col("academic_period_name").contains("Summer"), F.lit(2))
            )
            # Derive year from period_start_date

            .withColumn("year", F.regexp_extract(F.trim(F.col("period_name")), r"(\d{4})", 1).cast("int"))
    )

    # Window ordered by year and term sequence (and start date for stability)
    w_non_sess = Window.orderBy("year", "term_sequence", "period_start_date")

    non_sess_curated_df = (
        non_sess_curated_df
            .withColumn("prev_period_end_date", F.lag("period_end_date").over(w_non_sess))
            # Curated end date is the current academic_period_end_date
            .withColumn("declare_end_date", F.col("period_end_date"))
            # Curated start date = previous period_end_date + 1 day (or current start if first)
            .withColumn(
                "declare_start_date",
                F.when(
                    F.col("prev_period_end_date").isNull(),
                    F.col("period_start_date")
                ).otherwise(F.date_add(F.col("prev_period_end_date"), 1))
            )
    )

    calculated_df = non_sess_curated_df.unionByName(sess_df, allowMissingColumns=True)\
            .drop("prev_period_end_date")
    dim_academic_period_df = add_audit_column(
            calculated_df,
            PIPELINE_NAME
        )
    

    dim_academic_period_df.cache()

    final_count = dim_academic_period_df.count()
    rows_read = final_count
    rows_written = final_count

    dim_academic_period_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    write_delta_tbl(source_df = dim_academic_period_df,target_table_path = dim_academic_period_path
                ,keys = ["academic_period_id","academic_period_key"])

    dim_academic_period_df.unpersist()
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_academic_period table: {e}")

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
