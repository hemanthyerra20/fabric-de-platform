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
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
# META         },
# META         {
# META           "id": "08846ef1-977d-4698-b06f-84fdbd834bc6"
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

# PARAMETERS CELL ********************

VAR_CS_LIB_NAME = "var_cs_process"
MASTER_RUN_ID = ""
RUN_ID = ""
PIPELINE_NAME = ""
NOTEBOOK_NAME = ""
DOMAIN = ""
SOURCE_NAME = ""
ACTIVITY_NAME = "fact_ipeds_graduation"
LEVEL = ""
LAYER = ""
ENVIRONMENT = ""
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "ipeds"
gold_schema_name = "ipeds"
ws_gold_schema_name="workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.fact_ipeds_graduation"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### fact_ipeds_graduation

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
try:
    # ============================================================
    # 1. CONFIGURATION
    # ============================================================
    DIM_INSTITUTION_TABLE = abfss_path_builder(
            workspace_id,
            gold_lakehouse_id,
            gold_schema_name,
            "dim_ipeds_institutions"
        )

    GR200_TABLE = abfss_path_builder(
            workspace_id,
            silver_lakehouse_id,
            silver_schema_name,
            "gr200_year"
        )

    DRVGR_TABLE = abfss_path_builder(
            workspace_id,
            silver_lakehouse_id,
            silver_schema_name,
            "drvgryear"
        )


    # GR200_TABLE = "lh_cs_silver.ipeds.gr200_year"
    # DRVGR_TABLE = "lh_cs_silver.ipeds.drvgryear"
    CREATED_BY = "IPEDS_RPT16"

    # ============================================================
    # 2. READ IPEDS INSTITUTION DIMENSION
    # ============================================================
    inst_df = (
        spark.read.format("delta")
        .load(DIM_INSTITUTION_TABLE)
        .select("unitid")
        .distinct()
    )
    
    # ============================================================
    # 3. READ GR200
    #    Survey year Y covers cohort Y-4
    # ============================================================
    gr200_source_df = (
        spark.read.format("delta")
        .load(GR200_TABLE)
    )

    #gr200_source_df = spark.table(GR200_TABLE)
    g2_df = (
        gr200_source_df
        .join(
            inst_df,
            on="unitid",
            how="inner"
        )
        .select(
            F.col("unitid"),
            F.col("year").cast("int").alias("survey_year"),
            (F.col("year") - 4)
                .cast("int")
                .alias("cohort_year"),
            F.when(
                F.col("l4gr100") >= 0,
                F.col("l4gr100").cast("decimal(5,1)")
            ).alias("gr_rate_100"),
            F.when(
                F.col("l4gr150") >= 0,
                F.col("l4gr150").cast("decimal(5,1)")
            ).alias("gr_rate_150_x"),
            F.when(
                F.col("l4gr200") >= 0,
                F.col("l4gr200").cast("decimal(5,1)")
            ).alias("gr_rate_200"),
            F.when(
                F.col("l4ac150") >= 0,
                F.col("l4ac150").cast("int")
            ).alias("adjusted_cohort_150"),
            F.when(
                F.col("l4ac200") >= 0,
                F.col("l4ac200").cast("int")
            ).alias("adjusted_cohort_200"),
            F.when(
                F.col("l4revct") >= 0,
                F.col("l4revct").cast("int")
            ).alias("revised_cohort"),
            F.when(
                F.col("l4exclu") >= 0,
                F.col("l4exclu").cast("int")
            ).alias("exclusions")
        )
    )
    # ============================================================
    # 4. READ DRVGR
    #    Survey year Y covers cohort Y-3
    # ============================================================
    drvgr_source_df = (
        spark.read.format("delta")
        .load(DRVGR_TABLE)
    )
    dg_df = (
        drvgr_source_df
        .join(
            inst_df,
            on="unitid",
            how="inner"
        )
        .select(
            F.col("unitid"),
            F.col("year").cast("int").alias("survey_year"),
            (F.col("year") - 3)
                .cast("int")
                .alias("cohort_year"),
            F.when(
                F.col("grrttot") >= 0,
                F.col("grrttot").cast("decimal(5,1)")
            ).alias("gr_rate_150"),
            F.when(
                F.col("trrttot") >= 0,
                F.col("trrttot").cast("decimal(5,1)")
            ).alias("transfer_out_rate"),
            F.when(
                F.col("grrtm") >= 0,
                F.col("grrtm").cast("decimal(5,1)")
            ).alias("gr_rate_150_male"),
            F.when(
                F.col("grrtw") >= 0,
                F.col("grrtw").cast("decimal(5,1)")
            ).alias("gr_rate_150_female"),
            F.when(
                F.col("grrtwh") >= 0,
                F.col("grrtwh").cast("decimal(5,1)")
            ).alias("gr_rate_150_white"),    
            F.when(
                F.col("grrtbk") >= 0,
                F.col("grrtbk").cast("decimal(5,1)")
            ).alias("gr_rate_150_black"),
            F.when(
                F.col("grrths") >= 0,
                F.col("grrths").cast("decimal(5,1)")
            ).alias("gr_rate_150_hisp"),    
            F.when(
                F.col("grrtas") >= 0,
                F.col("grrtas").cast("decimal(5,1)")
            ).alias("gr_rate_150_asian"),    
            F.when(
                F.col("grrtan") >= 0,
                F.col("grrtan").cast("decimal(5,1)")
            ).alias("gr_rate_150_aian"),    
            F.when(

                F.col("grrtnh") >= 0,

                F.col("grrtnh").cast("decimal(5,1)")

            ).alias("gr_rate_150_nhpi"),
    
            F.when(

                F.col("grrt2m") >= 0,

                F.col("grrt2m").cast("decimal(5,1)")

            ).alias("gr_rate_150_2more"),
    
            F.when(

                F.col("grrtun") >= 0,

                F.col("grrtun").cast("decimal(5,1)")

            ).alias("gr_rate_150_unknown"),
    
            F.when(

                F.col("pggrrtt") >= 0,

                F.col("pggrrtt").cast("decimal(5,1)")

            ).alias("gr_rate_150_pell"),
    
            F.when(

                F.col("nrgrrtt") >= 0,

                F.col("nrgrrtt").cast("decimal(5,1)")

            ).alias("gr_rate_150_nonpell")

        )

    )
    # ============================================================
    # 5. CREATE UNIQUE UNITID + COHORT_YEAR KEYS
    # ============================================================
    
    keys_df = (
        g2_df
        .select(
            "unitid",
            "cohort_year"
        )
        .union(
            dg_df.select(
                "unitid",
                "cohort_year"
            )
        )
        .distinct()
    )
    # ============================================================
    # 6. JOIN GR200 + DRVGR
    # ============================================================
    fact_df = (
        keys_df.alias("k")
        .join(
            g2_df.alias("g2"),
            (
                (F.col("k.unitid") == F.col("g2.unitid")) &
                (F.col("k.cohort_year") == F.col("g2.cohort_year"))
            ),
            "left"
        )
        .join(
            dg_df.alias("dg"),
            (
                (F.col("k.unitid") == F.col("dg.unitid")) &
                (F.col("k.cohort_year") == F.col("dg.cohort_year"))
            ),
            "left"
        )
        .select(
            F.col("k.unitid").cast("int").alias("unitid"),
            F.col("k.cohort_year")
                .cast("int")
                .alias("cohort_year"),
            F.col("g2.survey_year")
                .cast("int")
                .alias("survey_year_gr200"),
            F.col("dg.survey_year")
                .cast("int")
                .alias("survey_year_drvgr"),
            F.col("g2.gr_rate_100"),
            F.coalesce(
                F.col("g2.gr_rate_150_x"), 
                F.col("dg.gr_rate_150")
            ).cast("decimal(5,1)")
            .alias("gr_rate_150"),
            F.col("g2.gr_rate_200"),
            F.col("dg.transfer_out_rate"),
            F.col("g2.adjusted_cohort_150"),
            F.col("g2.adjusted_cohort_200"),
            F.col("g2.revised_cohort"),
            F.col("g2.exclusions"),
            F.col("dg.gr_rate_150_male"),
            F.col("dg.gr_rate_150_female"),
            F.col("dg.gr_rate_150_white"),
            F.col("dg.gr_rate_150_black"),
            F.col("dg.gr_rate_150_hisp"),
            F.col("dg.gr_rate_150_asian"),
            F.col("dg.gr_rate_150_aian"),
            F.col("dg.gr_rate_150_nhpi"),
            F.col("dg.gr_rate_150_2more"),
            F.col("dg.gr_rate_150_unknown"),
            F.col("dg.gr_rate_150_pell"),
            F.col("dg.gr_rate_150_nonpell")
        )
    )
    # ============================================================
    # 7. ADD AUDIT COLUMNS
    # ============================================================
    fact_df = (
        fact_df
        .withColumn(
            "created_by",
            F.lit(CREATED_BY)
        )
        .withColumn(
            "created_on",
            F.current_timestamp()
        )
        .withColumn(
            "modified_on",
            F.current_timestamp()
        )
    )
    
    fact_df = (
        fact_df
        .withColumn(
            "sk_fact_ipeds_graduation",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("unitid").cast("string"),
                    F.col("cohort_year").cast("string")
                ),
                256
            )
        )
    )
    # ============================================================
    # 8. FORCE FINAL COLUMN ORDER
    # ============================================================
    fact_df = fact_df.select(
        "sk_fact_ipeds_graduation",
        "unitid",
        "cohort_year",
        "survey_year_gr200",
        "survey_year_drvgr",
        "gr_rate_100",
        "gr_rate_150",
        "gr_rate_200",
        "transfer_out_rate",
        "adjusted_cohort_150",
        "adjusted_cohort_200",
        "revised_cohort",
        "exclusions",
        "gr_rate_150_male",
        "gr_rate_150_female",
        "gr_rate_150_white",
        "gr_rate_150_black",
        "gr_rate_150_hisp",
        "gr_rate_150_asian",
        "gr_rate_150_aian",
        "gr_rate_150_nhpi",
        "gr_rate_150_2more",
        "gr_rate_150_unknown",
        "gr_rate_150_pell",
        "gr_rate_150_nonpell",
        "created_by",
        "created_on",
        "modified_on"
    )
    # Audit Columns
    fact_audit_df = add_audit_column(
        fact_df,
        PIPELINE_NAME
    )
    fact_audit_df.cache()
    final_count = fact_audit_df.count()
    rows_read = final_count
    rows_written = final_count
    # Target Path
    fact_gold_path = abfss_path_builder(
        workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "fact_ipeds_graduation"
    )

    write_delta_tbl(
        fact_audit_df,
        fact_gold_path,
        ["sk_fact_ipeds_graduation"]
    )
    fact_audit_df.unpersist()
except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID),
        _safe_str(RUN_ID),
        _safe_str(PIPELINE_NAME),
        _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN),
        _safe_str(SOURCE_NAME),
        _safe_str(ACTIVITY_NAME),
        "failed",
        _safe_str(LEVEL),
        _safe_str(LAYER),
        _safe_str(source_table),
        _safe_str(target_table),
        0,
        0,
        0,
        start_time,
        end_time,
        str(e),
        str(e),
        _safe_str(ENVIRONMENT),
        _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID),
        _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(
        f"Failed to create/load fact_ipeds_graduation table: {e}"
    )
 

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
