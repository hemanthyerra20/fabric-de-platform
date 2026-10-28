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
JOB_NAME = "nb_gold_layer_ingestion"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
silver_schema_name = 'workday_cs'
gold_schema_name = 'workday_cs'
silver_lakehouse_name = 'lh_cs_silver'
gold_lakehouse_name = 'lh_cs_gold'
notebook_name = notebookutils.runtime.context["currentNotebookName"]


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
#config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
store_workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, silver_lakehouse_name)
gold_lakehouse_id = get_lakehouse_id(var_cs_lib_name, gold_lakehouse_name)
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_enrollment_snapshot_aidcode"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    ##
    # Build ABFSS paths for all required tables
    std_enrollment_abfss_path = abfss_path_builder(store_workspace_id, silver_lakehouse_id, silver_schema_name, "student_enrollment")
    student_transfer_credit_path = abfss_path_builder(store_workspace_id, silver_lakehouse_id, silver_schema_name, "student_transfer_credit")
    dim_students_abfss_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_students")
    dim_college_abfss_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_college")
    dim_cohort_abfss_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_cohort")
    dim_academic_period_abfss_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
    dim_programs_of_study_abfss_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_program")
    dim_load_status_abfss_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_load_status")
    dim_delivery_mode_abfss_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_delivery_mode")
    dim_campus_loc_path = abfss_path_builder(store_workspace_id, gold_lakehouse_id, gold_schema_name, "dim_campus_location")

    # Read each table into a DataFrame with only required columns
    std_transfer_credit_df = (
        spark.read.format("delta")
        .load(student_transfer_credit_path)
        .groupBy(F.col("course"), F.col("student_id"))
        .agg(F.sum(F.col("credits")).alias("credits"))
    )

    # se_df = (
    #     spark.read.format("delta").load(std_enrollment_abfss_path)
    #     .select(
    #         F.col("student_id"),
    #         F.col("academic_record_id"),
    #         F.col("program_of_study"),
    #         F.col("academic_unit"),
    #         F.col("academic_period")
    #     )
    # )

    dim_students_df = (
        spark.read.format("delta")
        .load(dim_students_abfss_path)
        .where(F.col("is_current") == 1)
        .select(F.col("student_id"), F.col("student_key"))
    )

    dim_college_df = (
        spark.read.format("delta").load(dim_college_abfss_path)
        .select(
            F.col("college_key"),
            F.col("id"),
            F.col("name").alias("college_name")
        )
    )

    dim_academic_period_df = (
        spark.read.format("delta").load(dim_academic_period_abfss_path)
        .select(
            F.col("Academic_Period_Name"),
            F.col("period_name"),
            F.col("Academic_Period_Key"),
            F.col("pell_recalculation_date"),
            F.col("current_flag")
        )
    )

    dim_pos_df = (
        spark.read.format("delta").load(dim_programs_of_study_abfss_path)
        .select(
            F.col("program_name"),
            F.col("program_key"),
            F.col("is_current"),
            F.col("effective_date"),
            F.col("expiration_date")
        )
    )

    dim_load_status_df = (
        spark.read.format("delta").load(dim_load_status_abfss_path)
        .select(F.col("load_status_key"), F.col("id"))
    )

    dim_load_status_df = (
        spark.read.format("delta").load(dim_delivery_mode_abfss_path)
        .select(F.col("delivery_mode_key"), F.col("delivery_mode_description"))
    )


    dim_campuslocation = (
        spark.read.format("delta")
        .load(dim_campus_loc_path)
        .select(
            F.col("campus_location_key"),
            F.concat(F.col("name"), F.lit("_"), F.col("primary_campus_location")).alias("campus_location"),
        )
    )
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed processing dim_program: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    prd_academic_period_df = (
        spark.read.format("delta")
        .load(dim_academic_period_abfss_path)
        .select(F.col("Academic_Period_Name"), F.col("pell_recalculation_date"))
        .filter(F.col("current_flag") == 1)
    )

    # Get the first row as the current academic period and PRD date
    first_row = prd_academic_period_df.orderBy("Academic_Period_Name").first()

    if first_row is not None:
        current_academic_period = first_row["Academic_Period_Name"]
        prd_date = first_row["pell_recalculation_date"]
    else:
        current_academic_period = None
        prd_date = None

    print("current_academic_period:",current_academic_period,"  prd_date:",prd_date)
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed processing dim_program: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# snapshot table

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql import functions as F

try:

    # Build paths for course_registration and course_sections tables and load base snapshot
    std_course_registration_abfss_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "course_registration",
    )

    std_course_sections_abfss_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "course_sections",
    )

    course_section_academic_unit_abfss_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "course_section_academic_unit_data",
    )

    delivery_mode_references_abfss_path = abfss_path_builder(
        store_workspace_id,
        silver_lakehouse_id,
        silver_schema_name,
        "delivery_mode_references",
    )

    course_section_taxonomy_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_course_section_taxonomy",
    )

    course_section_taxonomy_df = (
        spark.read.format("delta")
        .load(course_section_taxonomy_path)
        .groupBy("course_section_id")
        .agg(F.concat_ws(",", F.collect_set("code")).alias("tax_code_list"))
    )

    course_section_taxonomy_aid_code_df = (
        spark.read.format("delta")
        .load(course_section_taxonomy_path)
        .filter(F.col("scheme") == "Aid Code")
        .groupBy("course_section_id")
        .agg(F.concat_ws(",", F.collect_set("code")).alias("tax_code_list"))
    )

    # Base course_registration DF
    course_registration_base_df = (
        spark.read.format("delta")
        .load(std_course_registration_abfss_path)
        .alias("cr")
    )
    RCC_DUAL = ["1A", "1B", "1E", "8A", "8B","9B", "9C", "9D", "9G", "9H", "9K"]
    # Derive cleaned campus_location from semicolon-separated list using location_code
    campus_list = F.transform(
        F.split(F.col("cr.campus_location"), ";"),
        lambda x: F.trim(x),
    )
    keyword = F.regexp_extract(F.col("cr.location_code"), "-(.*)", 1)
    matches = F.filter(
        campus_list,
        lambda x: x.contains(keyword),
    )
    selected_campus = F.when(
        F.size(matches) > 0,
        matches.getItem(0),
    ).otherwise(
        campus_list.getItem(0),
    )

    course_registration_base_df = course_registration_base_df.withColumn(
        "campus_location_clean",
        F.when(
            F.col("cr.campus_location").isNotNull() & (F.col("cr.campus_location") != ""),
            selected_campus,
        ).otherwise(F.col("cr.campus_location")),
    )

    course_registration_df = (
        course_registration_base_df.alias("cr")
        .select(
            F.col("cr.student_id"),
            F.col("cr.primary_program_of_study"),
            F.col("cr.academic_period"),
            F.col("cr.course_section_id"),
            F.col("cr.credits"),
            F.col("cr.registration_status"),
            F.col("cr.earned_credits"),
            F.col("cr.course_listing"),
            F.col("cr.location_code"),
            F.col("cr.campus_location_clean"),
            F.col("cr.registered_date"),
        )
        .join(
            course_section_taxonomy_df.alias("cst"),
            F.col("cr.course_section_id") == F.col("cst.course_section_id"),
            "left",
        )
        .join(
            course_section_taxonomy_aid_code_df.alias("cstac"),
             F.col("cr.course_section_id") == F.col("cstac.course_section_id"),
            "left",
        )
        .join(
            std_transfer_credit_df.alias("tc"),
            (F.col("cr.course_listing") == F.col("tc.course"))
            & (F.col("cr.student_id") == F.col("tc.student_id")),
            "left",
        )
        .select(
            F.col("cr.student_id").alias("student_id"),
            F.col("cr.primary_program_of_study").alias("primary_program_of_study"),
            F.col("cr.academic_period").alias("academic_period"),
            F.col("cr.course_section_id").alias("course_section_id"),
            F.col("cr.credits").alias("credits"),
            F.col("cr.registration_status").alias("registration_status"),
            F.col("cr.earned_credits").alias("earned_credits"),
            F.col("cst.tax_code_list").alias("tax_code_list"),
            F.col("cstac.tax_code_list").alias("aid_code_list"),
            F.col("tc.credits").alias("transfer_credits"),
            F.col("cr.location_code"),
            F.col("cr.campus_location_clean"),
            F.concat(
                F.col("cr.location_code"),
                F.lit("_"),
                F.col("cr.campus_location_clean"),
            ).alias("location"),
            F.col("cr.registered_date"),
        )
    )
    display(course_registration_df.filter((F.col("student_id").isin('11056601','30036236','30312850','30335600','30358948','30360885','24812341','25206566','30179639','13519529','25186780','30330569','30231236','20609986'
,'21351351','25371549','30365396','24562377','25288830'))))
#################################################################################
    course_section_academic_unit_df = (
        spark.read.format("delta")
        .load(course_section_academic_unit_abfss_path)
        .select(
            F.col("academic_unit_id"),
            F.col("course_section_key"),
        )
    )

    delivery_mode_ref_df = (
        spark.read.format("delta")
        .load(delivery_mode_references_abfss_path)
        .select(
            F.col("delivery_mode_id"),
            F.col("description"),
        )
    )

    course_sections_base_df = (
        spark.read.format("delta")
        .load(std_course_sections_abfss_path)
        .select(
            F.col("course_section_id"),
            F.col("course_section_key"),
            F.col("delivery_mode_id"),
        )
    )

    course_sections_df = (
        course_sections_base_df.alias("cs")
        .join(
            delivery_mode_ref_df.alias("dm"),
            F.col("cs.delivery_mode_id") == F.col("dm.delivery_mode_id"),
            "left",
        )
        .select(
            F.col("cs.course_section_id"),
            F.col("cs.course_section_key"),
            F.col("dm.description"),
        )
    )

    # -------------------------------------------------------------------------
    # 1st DF: joins + filter + location CASE logic (no aggregation)
    # -------------------------------------------------------------------------
    enrollment_pre_agg_df = (
        course_registration_df.alias("cr")
        .join(
            course_sections_df.alias("cs"),
            F.col("cr.course_section_id") == F.col("cs.course_section_id"),
            "inner",
        )
        .join(
            course_section_academic_unit_df.alias("csau"),
            F.col("cs.course_section_key") == F.col("csau.course_section_key"),
            "inner",
        )
        .filter(F.col("cr.registration_status").isin("Registered", "Completed"))
        .withColumn(
            "location",
            F.when(
                F.col("cs.description").isin("Online Live", "Online")
                & (
                    (F.col("cr.location_code").isNull() | (F.col("location_code") == ""))
                    | (F.col("cr.campus_location_clean").isNull() | (F.col("cr.campus_location_clean")== ""))
                ),
                F.lit(None).cast("string"),
            )
            .when(
                ~F.col("cs.description").isin("Online Live", "Online")
                & (
                    (F.col("cr.location_code").isNull() | (F.col("location_code") == ""))
                    | (F.col("cr.campus_location_clean").isNull() | (F.col("cr.campus_location_clean")== ""))
                ),
                F.lit("1301-Green Bay_NWTC Green Bay"),
            )
            .otherwise(F.col("cr.location")),
        )
        .select(
            F.col("cr.student_id").alias("student_id"),
            F.col("cr.primary_program_of_study").alias("primary_program_of_study"),
            F.col("cr.academic_period").alias("academic_period"),
            F.col("cs.description").alias("description"),
            F.col("csau.academic_unit_id").alias("academic_unit_id"),
            F.col("cr.credits").alias("credits"),
            F.col("cr.earned_credits").alias("earned_credits"),
            F.col("cr.tax_code_list").alias("tax_code_list"),
            F.col("cr.aid_code_list").alias("aid_code_list"),
            F.col("cr.transfer_credits").alias("transfer_credits"),
            F.col("location"),
            F.col("cr.registered_date").alias("registered_date"),
        )
    )
    display("apply the null and default rule enrollment_pre_agg_df:")
    display(enrollment_pre_agg_df.filter((F.col("student_id").isin('11056601','30036236','30312850','30335600','30358948','30360885','24812341','25206566','30179639','13519529','25186780','30330569','30231236','20609986'
,'21351351','25371549','30365396','24562377','25288830'))))
    # -------------------------------------------------------------------------
    # 2nd DF: aggregation – campus_location = first non-null location at min registered_date
    #         academic_unit_id = first non-null academic_unit_id at min registered_date
    # -------------------------------------------------------------------------
    cte_enrollment_snapshot_df = (
        enrollment_pre_agg_df
        .groupBy(
            "student_id",
            "primary_program_of_study",
            "academic_period",
            "description",
            # "academic_unit_id",
        )
        .agg(
            F.sum(F.col("credits").cast("decimal(18,5)")).alias("credits"),
            F.sum(F.col("earned_credits").cast("decimal(18,5)")).alias("earned_credits"),
            F.concat_ws(",", F.collect_set("tax_code_list")).alias("tax_code_list2"),
            F.concat_ws(",", F.collect_set("aid_code_list")).alias("aid_code_list2"),
            F.sum(F.col("transfer_credits")).alias("transfer_credits2"),
            # Pick first non-null academic_unit_id based on registered_date
            F.min(
                F.struct(
                    F.when(
                        F.col("academic_unit_id").isNull(),
                        F.lit(1),
                    )
                    .otherwise(F.lit(0))
                    .alias("is_null_acad_unit"),
                    F.col("registered_date"),
                    F.col("academic_unit_id"),
                )
            ).alias("acad_unit_struct"),
            # Existing logic for campus_location
            F.min(
                F.struct(
                    F.when(
                        (F.col("location").isNull()) | (F.col("location") == ""),
                        F.lit(1),
                    )
                    .otherwise(F.lit(0))
                    .alias("is_null_loc"),
                    F.col("registered_date"),
                    F.col("location"),
                )
            ).alias("loc_struct"),
        )
        .select(
            F.col("student_id"),
            F.col("primary_program_of_study"),
            F.col("academic_period"),
            F.col("description"),
            F.col("acad_unit_struct.academic_unit_id").alias("academic_unit_id"),
            F.col("credits"),
            F.col("earned_credits"),
            F.col("tax_code_list2"),
            F.col("aid_code_list2"),
            F.col("transfer_credits2"),
            F.col("loc_struct.location").alias("campus_location"),
        )
    )
    display("after aggr")
    display(cte_enrollment_snapshot_df.filter((F.col("student_id").isin('11056601','30036236','30312850','30335600','30358948','30360885','24812341','25206566','30179639','13519529','25186780','30330569','30231236','20609986'
,'21351351','25371549','30365396','24562377','25288830'))))
    w = (
        Window.partitionBy(
            "a_student_id",
            "primary_program_of_study",
            "academic_period",
            "description",
            "academic_unit_id",
        )
        .orderBy("academic_period")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    cte_enrollment_snapshot_df_renamed = cte_enrollment_snapshot_df.withColumnRenamed(
        "student_id", "a_student_id"
    )

    # Snapshot date: if current_flag = 1 then current_date else pell_recalculation_date
    snapshot_expr = F.when(
        F.col("ap.current_flag") == 1,
        F.current_date()
    ).otherwise(
        F.col("ap.pell_recalculation_date")
    )

    # PRD flag: 1 when snapshotdate equals pell_recalculation_date, else 0
    prd_expr = F.when(
        snapshot_expr == F.col("ap.pell_recalculation_date"),
        F.lit(1)
    ).otherwise(F.lit(0)).alias("is_prd_snapshot")

    # Expression for load_status_per based on term and credits
    load_status_per_expr = (
        F.when(
            F.col("a.academic_period").contains("Fall"),
            (F.col("credits") / F.lit(12)) * F.lit(100),
        )
        .when(
            F.col("a.academic_period").contains("Spring"),
            (F.col("credits") / F.lit(12)) * F.lit(100),
        )
        .when(
            F.col("a.academic_period").contains("Summer"),
            (F.col("credits") / F.lit(6)) * F.lit(100),
        )
        .otherwise(F.lit(0.0))
    )

    enrollment_snapshot_with_cum_df = (
        cte_enrollment_snapshot_df_renamed.alias("a")
        .join(
            dim_students_df.alias("s"),
            F.col("a.a_student_id") == F.col("s.student_id"),
            "inner",
        )
        .join(
            dim_academic_period_df.alias("ap"),
            F.col("a.academic_period") == F.col("ap.period_name"),
            "inner",
        )
        .join(
            dim_pos_df.alias("pos"),
            (
                (F.col("a.primary_program_of_study") == F.col("pos.program_name"))
                & (F.to_date(F.col("ap.pell_recalculation_date")) >= F.to_date(F.col("pos.effective_date")))
                & (
                    F.col("pos.expiration_date").isNull()
                    | (F.to_date(F.col("ap.pell_recalculation_date")) <= F.to_date(F.col("pos.expiration_date")))
                )
            ),
            "inner",
        )
        .join(
            dim_college_df.alias("cl"),
            (F.col("a.academic_unit_id") == F.col("cl.id")),
            "left",
        )
        .join(
            dim_campuslocation.alias("cloc"),
            F.col("a.campus_location") == F.col("cloc.campus_location"),
            "left",
        )
        .withColumn("cumulative_credits", F.sum("credits").over(w))
        .withColumn("cumulative_earned_credits", F.sum("earned_credits").over(w))
        .withColumn("load_status_per", load_status_per_expr)
        .withColumn(
            "load_status",
            F.when(F.col("load_status_per") >= 100, F.lit("Full-Time"))
            .when(
                (F.col("load_status_per") >= 50) & (F.col("load_status_per") < 100),
                F.lit("Half-Time"),
            )
            .otherwise(F.lit("Part-Time")),
        )
        .select(
            F.col("a.a_student_id").alias("student_id"),
            F.col("s.student_key").alias("student_key"),
            F.col("a.primary_program_of_study"),
            F.col("pos.program_key").alias("program_key"),
            F.col("a.academic_period"),
            F.col("ap.Academic_Period_Key").alias("academic_period_key"),
            F.col("a.description").alias("course_modality"),
            F.col("credits"),
            F.col("transfer_credits2").alias("transfer_credits"),
            F.when(F.col("transfer_credits2") > 0, F.lit("Yes")).otherwise(F.lit("No")).alias("transfer_credits_flag"),
            F.col("ap.pell_recalculation_date"),
            snapshot_expr.alias("snapshotdate"),
            prd_expr,
            F.col("load_status"),
            F.col("academic_unit_id"),
            F.col("cl.college_key"),
            F.col("a.campus_location"),
            F.col("cloc.campus_location_key").alias("campus_location_key"),
            F.col("tax_code_list2").alias("tax_code_list"),
            F.col("aid_code_list2").alias("aid_code_list"),
        )
        .withColumn(
            "is_dual_credit_flag",
            F.when(
                F.arrays_overlap(
                    F.transform(
                        F.split(F.coalesce(F.col("tax_code_list"), F.lit("")), ","),
                        lambda c: F.upper(F.trim(c)),
                    ),
                    F.array(*[F.lit(c) for c in RCC_DUAL]),
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "enrollment_snapshot_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("student_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("program_key").cast("string"), F.lit("")),
                    F.coalesce(F.col("academic_period_key").cast("string"), F.lit("")),
                    F.coalesce(F.col("course_modality").cast("string"), F.lit("")),
                    F.coalesce(F.col("academic_unit_id").cast("string"), F.lit("")),
                    F.coalesce(F.col("credits").cast("string"), F.lit("")),
                    F.coalesce(F.col("is_prd_snapshot").cast("string"), F.lit("")),
                ),
                256,
            ),
        )
        .orderBy("student_id", "primary_program_of_study", "academic_period")
    )

    # Add audit columns and preview the result
    enrollment_snapshot_with_cum_audit_df = add_audit_column(enrollment_snapshot_with_cum_df, JOB_NAME)
    display("final df:")
    display(enrollment_snapshot_with_cum_audit_df.filter((F.col("student_id").isin('11056601','30036236','30312850','30335600','30358948','30360885','24812341','25206566','30179639','13519529','25186780','30330569','30231236','20609986'
,'21351351','25371549','30365396','24562377','25288830'))))
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed processing fact_enrollment_snapshot_aidcode: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter enrollment_snapshot_with_cum_audit_df where campus_location is not null or empty
df_test = enrollment_snapshot_with_cum_audit_df.filter(
    F.col("campus_location").isNotNull() & (F.col("campus_location") != "")
)
# display(df_test)
# display(course_registration_df.filter((F.col("student_id") == "14496167")))
# display(enrollment_snapshot_with_cum_audit_df.filter((F.col("student_id") == "14496167")))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

distinct_row_count = enrollment_snapshot_with_cum_audit_df.distinct().count()
distinct_key_count = (enrollment_snapshot_with_cum_audit_df.select("student_id","program_key","academic_period_key","course_modality","academic_unit_id","credits","is_prd_snapshot").distinct().count())
counts_df = spark.createDataFrame([(distinct_row_count, distinct_key_count)],["distinct_row_count", "distinct_key_count"])
display(counts_df)
#210513 #238734 1236734

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_enrollement_snapshot_path = abfss_path_builder(
#     store_workspace_id,
#     gold_lakehouse_id,
#     gold_schema_name,
#     "fact_enrollment_snapshot",
# )
# write_delta_tbl(
#         source_df=enrollment_snapshot_with_cum_audit_df.distinct(),
#         target_table_path=fact_enrollement_snapshot_path,
#         keys=[
#             "student_id","program_key","academic_period_key","course_modality","academic_unit_id","credits","is_prd_snapshot"
#         ],
#     )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date
try:
        
    fact_enrollement_snapshot_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "fact_enrollment_snapshot_aidcode",
    )
    final_count = enrollment_snapshot_with_cum_audit_df.count()
    rows_read = final_count
    rows_written = final_count

    # Apply academic_period filter once and reuse in both branches
    filtered_enrollment_snapshot_df = enrollment_snapshot_with_cum_audit_df.filter(    F.col("academic_period") == current_academic_period)

    # If today's date equals PRD date, append snapshot to existing table
    if prd_date is not None and date.today() == prd_date:
        (
            filtered_enrollment_snapshot_df
            .distinct()
            .write
            .format("delta")
            .mode("append")
            .save(fact_enrollement_snapshot_path)
        )
    # Otherwise, run the standard write/merge logic
    else:
        write_delta_tbl(
            source_df=enrollment_snapshot_with_cum_audit_df.distinct(),
            target_table_path=fact_enrollement_snapshot_path,
            keys=[
                "student_id","program_key","academic_period_key","course_modality","academic_unit_id","credits","is_prd_snapshot"
            ],
        )
except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load fact_enrollment_snapshot_aidcode table: {e}")

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
