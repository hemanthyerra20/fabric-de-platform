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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

VAR_CS_LIB_NAME = "var_cs_process"
PIPELINE_NAME = "nb_gold_layer_ingestion"
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = "workday_fact_gold_load"
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
target_table = f"lh_cs_gold.{gold_schema_name}.fact_application"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    std_applications_abfss_path=abfss_path_builder(store_workspace_id, silver_lakehouse_id
                        ,silver_schema_name, "student_applications")
    std_applications_adm_resp_abfss_path=abfss_path_builder(store_workspace_id, silver_lakehouse_id
                        ,silver_schema_name, "student_application_admission_responses")

    # --------------------------------------------------------
    # Build ABFSS Paths
    # --------------------------------------------------------
    student_applications_abfss_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "student_applications"
    )
    student_application_admission_responses_abfss_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "student_application_admission_responses"
    )
    
    dim_students_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_students"
    )
    dim_programs_of_study_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_program"
    )
    
    dim_academic_period_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_academic_period"
    )
    
    dim_application_status_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_application_status"
    )
    dim_admission_decision_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_admission_decision"
    )
    
    dim_date_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_date"
    )

    dim_college_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_college"
    )

    dim_applicant_type_abfss_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_applicant_type"
    )
    
    # --------------------------------------------------------
    # Read Tables
    # --------------------------------------------------------
    student_applications_df = (
    spark.read.format("delta")
    .load(student_applications_abfss_path)
    #.filter(F.col("student_application_id").isNotNull() &
        #(F.trim(F.col("student_application_id")) != ""))
 
)
 
    student_application_admission_responses_df = (
    spark.read.format("delta")
    .load(student_application_admission_responses_abfss_path)
    .select("student_id", "admission_decision")\
    .filter((F.col("admission_decision").isNotNull()) & (F.col("student_id").isNotNull()))
    .drop_duplicates()
)
 
    dim_students_df = (
    spark.read.format("delta")
    .load(dim_students_abfss_path)
    .filter(F.col("is_current") == 1)
)
 
    dim_programs_of_study_df = (
    spark.read.format("delta")
    .load(dim_programs_of_study_abfss_path)
    .selectExpr("program_key", "program_id","academic_unit",
    "row_number() over(partition by program_id order by expiration_date desc) AS RN")
    .filter(F.col("RN") == "1")
    .drop("RN")
)
 
    dim_academic_period_df = (
    spark.read.format("delta")
    .load(dim_academic_period_abfss_path)
    .filter(F.col("is_major_term") == 1)
)
    dim_application_status_df = spark.read.format("delta").load(dim_application_status_abfss_path)
    dim_admission_decision_df = spark.read.format("delta").load(dim_admission_decision_abfss_path)
    dim_date_df = spark.read.format("delta").load(dim_date_abfss_path)
    dim_college_df = spark.read.format("delta").load(dim_college_abfss_path)
    dim_applicant_type_df= spark.read.format("delta").load(dim_applicant_type_abfss_path)
    # --------------------------------------------------------
    # Build Fact DataFrame
    # --------------------------------------------------------
    fact_student_applications_df = (
        student_applications_df.alias("sa")
        .join(
            student_application_admission_responses_df.alias("sadr"),
            F.col("sa.student_id") == F.col("sadr.student_id"),
            "left"
        )
        .join(
            dim_students_df.alias("s"),
            (F.col("sa.student_id") == F.col("s.student_id")), 
            "left"
        )
        .join(
            dim_programs_of_study_df.alias("pos"),
            (F.trim(F.col("sa.program_of_study_id")) == F.trim(F.col("pos.program_id"))),
            "left"
        )
        .join(
        dim_academic_period_df.alias("dap"),
        F.upper(F.trim(F.col("sa.academic_period_id")))
        ==
        F.upper(F.trim(F.col("dap.academic_period_id"))),
        "left"
    )
 
        .join(
            dim_application_status_df.alias("das"),
            F.trim(F.col("sa.student_load_status_id")) == F.trim(F.col("das.application_status_id")),
            "left"
        )
        .join(
            dim_admission_decision_df.alias("dad"),
            F.trim(F.col("sadr.admission_decision")) ==
            F.trim(F.col("dad.admission_decision_id")),
            "left"
        )
        .join(
            dim_date_df.alias("dd"),
            F.col("sa.application_date") == F.col("dd.calendar_date"),
            "left"
        )
        .join(
            dim_college_df.alias("dc"),
            F.col("dc.name")==F.col("pos.academic_unit"),
            "left"
        )
        .join(
            dim_applicant_type_df.alias("dat"),
            F.col("dat.applicant_type_id")==F.col("sa.tenanted_student_applicant_type_id"),
            "left"
        )

        .select(
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("s.student_key").cast("string"),
                    F.col("dap.academic_period_key").cast("string"),
                    F.col("sa.student_application_id").cast("string"),
                    F.col("sa.application_date").cast("string"),
                    F.col("pos.program_key").cast("string"),
                    F.col("das.application_status_key").cast("string"),
                    F.col("dat.applicant_type_key").cast("string"),
                    F.col("dad.admission_decision_key").cast("string")
                ),
                256
            ).alias("application_key"),
            F.col("s.student_key").alias("application_student_key"),
            F.col("pos.program_key").alias("program_key"),
            F.col("dc.college_key").alias("college_key"),
            F.col("dap.academic_period_key").alias("application_academic_period_key"),
            F.col("das.application_status_key").alias("application_status_key"),
            F.col("dad.admission_decision_key").alias("admission_decision_key"),
            F.col("dd.date_key").alias("application_date_key"),
            F.col("dat.applicant_type_key").alias("applicant_type_key"),
            F.col("sa.student_application_id")
        )
        .distinct()
    )

except Exception as e:
    print("Error in reading and processing:", e)
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
    fact_student_applications_df=fact_student_applications_df.distinct()
    
    fact_student_applications_df = (
        fact_student_applications_df
        .withColumn("created_on", F.current_timestamp())
        .withColumn("created_by", F.lit(PIPELINE_NAME))
        .withColumn("modified_on", F.current_timestamp())
        .withColumn("modified_by", F.lit(PIPELINE_NAME))
    )

    fact_student_applications_df.cache()

    final_count = fact_student_applications_df.count()
    rows_read = final_count
    rows_written = final_count


    fact_app_audit_df = add_audit_column(fact_student_applications_df, PIPELINE_NAME)
    fact_application_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "fact_application")
    fact_app_audit_df.write.format("delta").mode("overwrite").save(fact_application_path)

    fact_student_applications_df.unpersist()
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
