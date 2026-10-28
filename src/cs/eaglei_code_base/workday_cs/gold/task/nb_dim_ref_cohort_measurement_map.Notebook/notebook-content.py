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
# META         },
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
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
PIPELINE_NAME = ""
MASTER_RUN_ID = ""
RUN_ID = ""
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
target_table = f"lh_cs_gold.{gold_schema_name}.dim_ref_cohort_measurement_map"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:

    # ----------------------------------------------------------
    # Build ABFSS Paths
    # ----------------------------------------------------------

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

    ref_cohort_measurement_map_path = abfss_path_builder(
        store_workspace_id,
        gold_lakehouse_id,
        gold_schema_name,
        "dim_ref_cohort_measurement_map"
    )

    # ----------------------------------------------------------
    # Read Silver Tables
    # ----------------------------------------------------------

    maintained_academic_periods_df = (
        spark.read
             .format("delta")
             .load(maintained_academic_periods_sil_path)

             # Keep the original academic_period_name but also create a period_name
             .withColumn("period_name", F.col("academic_period_name"))

             # Derive "session" from the end of the academic_period_name
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

             # Normalize academic_period_name by stripping the session suffix
             # and converting "J Term" to "Spring"
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

             # Flag whether this period is the "current" one based on today's date
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
    # Build Academic Period Dimension
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
            F.col("map.period_name").alias("period_name"),
            F.col("map.academic_period_name").alias("academic_period_name"),
            F.col("map.session").alias("session"),

            F.to_date("map.academic_period_start_date", "yyyy-MM-dd").alias("period_start_date"),
            F.to_date("map.academic_period_end_date", "yyyy-MM-dd").alias("period_end_date"),

            F.col("map.academic_period_weeks_of_instruction").alias("weeks_of_instruction"),
            F.col("map.academic_calendar_id").alias("academic_calendar_id"),

            F.col("pr.academic_year").alias("academic_year"),

            F.col("rd.registration_open_date"),
            F.col("rd.registration_close_date"),

            F.col("pr.pell_recalculation_date").alias("pell_recalculation_date"),

            F.col("map.current_flag").alias("current_flag")
        )

        .filter(F.col("academic_period_id").isNotNull())
        .dropDuplicates(["academic_period_id"])
    )

    # ----------------------------------------------------------
    # Build Base for Cohort Measurement Map
    # ----------------------------------------------------------
    
    base_df = (
        maintained_academic_periods_silver_df

        .withColumn(
            "term_type",
            F.when(F.col("academic_period_name").contains("Summer"), "Summer")
             .when(F.col("academic_period_name").contains("Fall"), "Fall")
             .when(F.col("academic_period_name").contains("Spring"), "Spring")
        )

        
        .withColumn(
            "academic_year_start",
            F.when(
                F.month("period_start_date") >= 6,
                F.year("period_start_date")
            ).otherwise(
                F.year("period_start_date") - 1
            )
        )

        # Position of the term within the academic year (Summer=1, Fall=2, Spring=3)
        .withColumn(
            "term_sequence_in_ay",
            F.when(F.col("term_type") == "Summer", 1)
             .when(F.col("term_type") == "Fall", 2)
             .when(F.col("term_type") == "Spring", 3)
        )

        .filter(F.col("term_type").isNotNull())
        .filter(F.col("session").isNull())          # <-- FIX 1
    )

    # NEW FIX - enforce one row per (term_type, academic_year_start) by
    # keeping the earliest period_start_date in each group. This prevents
    # duplicate "Summer / AY" or similar collisions from fanning out.
    base_window = Window.partitionBy("term_type", "academic_year_start") \
                           .orderBy(F.col("period_start_date").asc(),
                                    F.col("academic_period_id").asc())

    base_df = (
        base_df
        .withColumn("base_row_num", F.row_number().over(base_window))
        .filter(F.col("base_row_num") == 1)
        .drop("base_row_num")
    )

    # ----------------------------------------------------------
    # GUARD - one row per (term_type, academic_year_start)
    # ----------------------------------------------------------
    # If this fails the session filter has not fully de-duplicated the terms and
    # every mapping below will fan out. Fail loudly here rather than writing
    # duplicate keys downstream.

    base_rows = base_df.count()
    base_terms = base_df.select("term_type", "academic_year_start").distinct().count()

    print(f"base_df rows={base_rows} distinct (term_type, academic_year_start)={base_terms}")

    if base_rows != base_terms:
        dupes = (
            base_df.groupBy("term_type", "academic_year_start")
                   .count()
                   .filter(F.col("count") > 1)
        )
        dupes.show(50, truncate=False)
        raise Exception(
            f"dim_ref_cohort_measurement_map: {base_rows - base_terms} duplicate terms "
            "remain after the session filter - measurement mappings would fan out."
        )

    # ----------------------------------------------------------
    # Global term sequence
    # ----------------------------------------------------------
    #   global_seq = academic_year_start * 3 + (term_sequence_in_ay - 1)
    # Consecutive integers across all years and terms, so "N terms later"
    # becomes simple arithmetic and the academic year boundary needs no
    # special handling.
    #   +1 = next term
    #   +3 = same term type, one academic year later  (Term 1-4)
    #   +4 = the Summer alternate  (Summer entry -> following Fall)

    seq_df = (
        base_df
        .withColumn(
            "global_seq",
            F.col("academic_year_start") * F.lit(3) + (F.col("term_sequence_in_ay") - F.lit(1))
        )
        .select(
            F.col("academic_period_key").alias("entry_academic_period_key"),
            F.col("term_type").alias("entry_term_type"),
            F.col("global_seq")
        )
    )

    all_terms_df = (
        base_df
        .withColumn(
            "global_seq",
            F.col("academic_year_start") * F.lit(3) + (F.col("term_sequence_in_ay") - F.lit(1))
        )
        .select(
            F.col("academic_period_key").alias("period_key"),
            F.col("period_start_date").alias("period_start_date"),
            F.col("global_seq").alias("target_seq")
        )
    )

    def offset_mapping(offset, out_col):
        """Term at global_seq + offset, one row per entry_academic_period_key.

        The groupBy is defensive - after the session filter the join is already
        1:1 - and ordering by period_start_date keeps the result deterministic

        across runs, which F.first() alone does not guarantee in Spark.
        """
        return (
            seq_df.alias("s")
            .join(
                all_terms_df.alias("t"),
                F.col("t.target_seq") == F.col("s.global_seq") + F.lit(offset),
                "left"
            )
            .groupBy(F.col("s.entry_academic_period_key"))
            .agg(
                F.first(F.col("t.period_key"), ignorenulls=True).alias(out_col)
            )
        )

    # Term 1-2 : the next calendar term after entry
    term2_df = offset_mapping(1, "term2_academic_period_key")

    # Term 1-2 alternate: Spring entrants whose Term 2 lands on Summer
    # get an alternate measurement in the following Fall (entry_seq + 2).
    term2_alt_df = (
        offset_mapping(2, "term2_alt_academic_period_key")
        .alias("alt")
        .join(
            seq_df.select("entry_academic_period_key", "entry_term_type").alias("st"),
            F.col("alt.entry_academic_period_key") == F.col("st.entry_academic_period_key"),
            "inner"
        )
        .select(
            F.col("alt.entry_academic_period_key").alias("entry_academic_period_key"),
            F.when(
                F.col("st.entry_term_type") == "Spring",
                F.col("alt.term2_alt_academic_period_key")
            ).alias("term2_alt_academic_period_key")
        )
    )

    # Term 1-4 : same term type, one academic year after entry
    y1_df = offset_mapping(3, "y1_academic_period_key")

    # Longer windows for graduation rate reporting
    y2_df = offset_mapping(6, "y2_academic_period_key")
    y3_df = offset_mapping(9, "y3_academic_period_key")
    y4_df = offset_mapping(12, "y4_academic_period_key")

    # ----------------------------------------------------------
    # FIX 2 - Summer alternate measurement term
    # ----------------------------------------------------------
    # IR rule confirmed by NWTC: a Summer entrant is retained at Term 1-4 if
    # enrolled in the following Summer OR the following Fall, because summer
    # offerings are inconsistent and a strict Summer-to-Summer test would
    # understate retention. Populated for Summer entrants only; NULL elsewhere,
    # so the OR branch in the fact load is inert for Fall and Spring cohorts.

    y1_alt_df = (
        offset_mapping(4, "y1_alt_academic_period_key")
        .alias("alt")
        .join(
            seq_df.select("entry_academic_period_key", "entry_term_type").alias("st"),
            F.col("alt.entry_academic_period_key") == F.col("st.entry_academic_period_key"),
            "inner"
        )
        .select(
            F.col("alt.entry_academic_period_key").alias("entry_academic_period_key"),
            F.when(
                F.col("st.entry_term_type") == "Summer",
                F.col("alt.y1_alt_academic_period_key")
            ).alias("y1_alt_academic_period_key")
        )
    )

    # ----------------------------------------------------------
    # Final Ref Cohort Measurement Map
    # ----------------------------------------------------------

    final_df = (
        base_df.alias("b")

        .join(term2_df.alias("t2"),
              F.col("b.academic_period_key") == F.col("t2.entry_academic_period_key"), "left")

        .join(term2_alt_df.alias("t2a"),
              F.col("b.academic_period_key") == F.col("t2a.entry_academic_period_key"), "left")

        .join(y1_df.alias("y1"),
              F.col("b.academic_period_key") == F.col("y1.entry_academic_period_key"), "left")

        .join(y1_alt_df.alias("ya"),
              F.col("b.academic_period_key") == F.col("ya.entry_academic_period_key"), "left")

        .join(y2_df.alias("y2"),
              F.col("b.academic_period_key") == F.col("y2.entry_academic_period_key"), "left")

        .join(y3_df.alias("y3"),
              F.col("b.academic_period_key") == F.col("y3.entry_academic_period_key"), "left")

        .join(y4_df.alias("y4"),
              F.col("b.academic_period_key") == F.col("y4.entry_academic_period_key"), "left")

        .select(
            F.col("b.academic_period_key").alias("entry_academic_period_key"),
            F.col("b.term_type"),
            F.col("b.academic_year_start"),
            F.col("b.term_sequence_in_ay"),

            F.col("t2.term2_academic_period_key"),
            F.col("t2a.term2_alt_academic_period_key"),
            F.col("y1.y1_academic_period_key"),
            F.col("ya.y1_alt_academic_period_key"),
            F.col("ya.y1_alt_academic_period_key").alias("alt_academic_period_key"),
            F.col("y2.y2_academic_period_key"),
            F.col("y3.y3_academic_period_key"),
            F.col("y4.y4_academic_period_key"),

            # FIX 3 - measurement_basis was hardcoded "Fall-to-Fall", which is
            # wrong for Spring and Summer entrants. Term 1-4 is same term type,
            # one academic year later, whatever the entry season.
            F.concat(
                F.col("b.term_type"), F.lit("-to-"), F.col("b.term_type")
            ).alias("measurement_basis"),

            F.lit(
                "Term 1-2 = next calendar term. Term 1-4 = same term type, one academic year "
                "after entry. Summer entrants also accept the following Fall (y1_alt). "
                "y2-y4 = same term type, two, three and four academic years after entry."
            ).alias("rule_description"),

            F.current_timestamp().alias("created_on"),
            F.lit("COHORT_SETUP").alias("created_by")
        )
    )

    final_df = final_df.select(
        "entry_academic_period_key",
        "term_type",
        "academic_year_start",
        "term_sequence_in_ay",
        "term2_academic_period_key",
        "term2_alt_academic_period_key",
        "y1_academic_period_key",
        "y1_alt_academic_period_key",
        "alt_academic_period_key",
        "y2_academic_period_key",
        "y3_academic_period_key",
        "y4_academic_period_key",
        "measurement_basis",
        "rule_description",
        "created_on",
        "created_by"
    )

    # ----------------------------------------------------------
    # GUARD - the merge key must be unique before write
    # ----------------------------------------------------------

    final_count = final_df.count()
    key_count = final_df.select("entry_academic_period_key").distinct().count()

    print(f"final_df rows={final_count} distinct entry_academic_period_key={key_count}")

    if final_count != key_count:
        final_df.groupBy("entry_academic_period_key") \
                .count().filter(F.col("count") > 1).show(50, truncate=False)
        raise Exception(
            f"dim_ref_cohort_measurement_map: {final_count - key_count} duplicate "
            "entry_academic_period_key rows - write_delta_tbl merge would fail."
        )

    # ----------------------------------------------------------
    # Add Audit Columns
    # ----------------------------------------------------------

    final_df = add_audit_column(final_df, PIPELINE_NAME)
    final_df.cache()

    final_count = final_df.count()
    rows_read = final_count
    rows_written = final_count

    write_delta_tbl(
        source_df=final_df,
        target_table_path=ref_cohort_measurement_map_path,
        keys=["entry_academic_period_key"]
    )

    final_df.unpersist()
    print("ref_cohort_measurement_map written successfully - "
          "term2, term2_alt, y1, y1_alt and y2-y4 mappings, main terms only")

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed to create/load dim_ref_cohort_measurement_map table: {e}")

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
