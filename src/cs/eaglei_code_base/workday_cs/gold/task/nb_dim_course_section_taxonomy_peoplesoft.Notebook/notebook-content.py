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

# MARKDOWN ********************

# # 

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
PIPELINE_NAME = "ManualRun"
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

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
bronze_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_bronze_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.dim_course_section_taxonomy"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Dim Course Sections Taxonomy

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Program Fee Code historical backfill: PeopleSoft PRGC -> dim_course_section_taxonomy
#
# Why: CTED depends on Program Fee Code 28 / 32 / 43. Fee codes exist on
#      Workday-native sections only (0% coverage before AY2026), so CTED
#      reads "No" for all PeopleSoft-era history.
#
# Source: PS_CLASS_ATTRIBUTE, CRSE_ATTR = 'PRGC'. Validated for the CTED
#         codes against SC_PROG_FEE_CD1-3 on PS_NW_CLI_ENRL_DW (Fall 2023 -
#         Spring 2025): enrollment-only = 0 / 0 / 1 for 28 / 32 / 43, so
#         these codes are section-level and PRGC is complete for them.
#
# Differences from the Aid Code backfill:
#   - a section can carry MORE THAN ONE fee code -> keep all of them
#   - taxonomy_key includes the code, so multiple codes per section stay unique
#   - no registration-coverage gate (not every section has a fee code);
#     CTED counts by year are displayed for review instead
#
# Re-runnable: deletes its own previous output (source = 'peoplesoft',
#              scheme = 'Program Fee Code') before appending.
# ============================================================================

FEE_SCHEME       = "Program Fee Code"
PS_ATTR          = "PRGC"
SOURCE_TAG       = "peoplesoft"
CTED_WAT         = ["28"]
CTED_NON_WAT     = ["32", "43"]
CONTRACT_CODES   = CTED_WAT + CTED_NON_WAT
INCLUDE_UNMAPPED = True   # write PS-only fee codes with PS description;
                          # set False to keep only codes in Workday's list

SECTION_KEYS = ["CRSE_ID", "CRSE_OFFER_NBR", "STRM", "SESSION_CODE", "CLASS_SECTION"]

try:
    # ------------------------------------------------------------------
    # 0. Paths
    # ------------------------------------------------------------------
    class_tbl_path  = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CLASS_TBL_SE_VW")
    class_attr_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CLASS_ATTRIBUTE")
    attr_val_path   = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CRSE_ATTR_VALUE")
    ref_path        = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "educational_taxonomy_code_references")
    scheme_path     = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "educational_taxonomy_schemes")
    cs_path         = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_sections")
    tax_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_section_taxonomy")
    reg_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_registration")
    ap_path         = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")

    def trimmed(col):
        return F.trim(F.col(col).cast("string")).alias(col)

    # ------------------------------------------------------------------
    # 1. PeopleSoft sections (one row per section)
    #    STRM guard: only '2' + YY + 2/5/8 -- ignores old-scheme 0xxx terms
    # ------------------------------------------------------------------
    sections = (
        spark.read.format("delta").load(class_tbl_path)
        .select(*[trimmed(k) for k in SECTION_KEYS],
                F.trim(F.col("CATALOG_NBR")).alias("catalog_nbr"),
                F.trim(F.col("CLASS_NBR").cast("string")).alias("class_nbr"))
        .filter(F.col("STRM").rlike("^2[0-9]{2}[258]$"))
        .distinct()
    )

    # ------------------------------------------------------------------
    # 2. Fee code(s) per section from PS_CLASS_ATTRIBUTE (PRGC)
    #    Keep EVERY code -- a section can carry more than one.
    # ------------------------------------------------------------------
    ps_fee = (
        spark.read.format("delta").load(class_attr_path)
        .filter(F.trim(F.col("CRSE_ATTR")) == PS_ATTR)
        .select(*[trimmed(k) for k in SECTION_KEYS],
                F.trim(F.col("CRSE_ATTR_VALUE")).alias("fee_code"))
        .filter(F.col("fee_code").isNotNull() & (F.col("fee_code") != ""))
        .distinct()
    )

    sec_id_cols = ["STRM", "catalog_nbr", "class_nbr"]
    sec_fee = (sections.join(ps_fee, SECTION_KEYS, "inner")
               .select(*sec_id_cols, "fee_code")
               .distinct())

    multi = (sec_fee.groupBy(*sec_id_cols).agg(F.countDistinct("fee_code").alias("codes"))
             .filter("codes > 1").count())
    print(f"Sections with more than one fee code (all kept): {multi}")

    # ------------------------------------------------------------------
    # 3. Build the PS section ID from the TERM'S CALENDAR YEAR
    # ------------------------------------------------------------------
    term_digit = F.substring("STRM", 4, 1)
    sec_fee = (
        sec_fee
        .withColumn("term_year",
                    (F.lit(2000) + F.substring("STRM", 2, 2).cast("int")).cast("string"))
        .withColumn("term_name",
                    F.when(term_digit == "2", "Spring")
                     .when(term_digit == "5", "Summer")
                     .when(term_digit == "8", "Fall"))
        .withColumn("ps_section_id",
                    F.concat_ws("_", "catalog_nbr", "class_nbr", "term_year", "term_name"))
    )

    # ------------------------------------------------------------------
    # 4. Link to Workday sections (strip '_Session_N' from Workday IDs)
    # ------------------------------------------------------------------
    cs = (spark.read.format("delta").load(cs_path)
          .select("course_section_key", "course_section_id")
          .withColumn("base_section_id",
                      F.regexp_replace("course_section_id", r"_Session_[0-9]+$", "")))

    dup_base = cs.groupBy("base_section_id").count().filter("count > 1")
    dup_count = dup_base.count()
    if dup_count > 0:
        print(f"Note: {dup_count} base section IDs map to more than one Workday section; "
              f"the same fee code(s) will be written to each.")

    linked = sec_fee.join(cs, F.col("ps_section_id") == F.col("base_section_id"), "left")

    total  = linked.count()
    linked = linked.filter(F.col("course_section_key").isNotNull())
    if linked.count() == 0:
        raise Exception("No PeopleSoft fee-coded sections linked to Workday sections; check section ID format.")

    # ------------------------------------------------------------------
    # 5. Map code -> Workday reference WITHIN the Program Fee Code scheme
    # ------------------------------------------------------------------
    scheme = (spark.read.format("delta").load(scheme_path)
              .filter(F.col("name") == FEE_SCHEME)
              .select(F.col("reference_id").alias("scheme_id")))
    scheme_id_value = scheme.first()["scheme_id"]

    wd_ref = (spark.read.format("delta").load(ref_path)
              .join(scheme, F.col("educational_taxonomy_id") == F.col("scheme_id"), "inner")
              .select(F.trim(F.col("code")).alias("fee_code"),
                      F.col("name").alias("wd_name"),
                      F.col("educational_taxonomy_id").alias("wd_id"))
              .dropDuplicates(["fee_code"]))

    w_desc = Window.partitionBy("fee_code").orderBy(F.col("EFFDT").desc())
    ps_desc = (spark.read.format("delta").load(attr_val_path)
               .filter(F.trim(F.col("CRSE_ATTR")) == PS_ATTR)
               .select(F.trim(F.col("CRSE_ATTR_VALUE")).alias("fee_code"),
                       F.col("DESCR").alias("ps_name"), "EFFDT")
               .withColumn("rn", F.row_number().over(w_desc))
               .filter("rn = 1").drop("rn", "EFFDT"))

    mapped = (linked
              .join(wd_ref, "fee_code", "left")
              .join(ps_desc, "fee_code", "left")
              .withColumn("name", F.coalesce("wd_name", "ps_name"))
              .withColumn("id",   F.coalesce("wd_id", F.lit(scheme_id_value)))
              .withColumn("in_workday_ref", F.col("wd_name").isNotNull()))

    print("Fee codes with no Workday reference (PS description used):")
    # display(mapped.filter(~F.col("in_workday_ref"))
    #               .groupBy("fee_code", "ps_name").count().orderBy("fee_code"))

    missing_cted = (mapped.filter(F.col("fee_code").isin(CONTRACT_CODES) & ~F.col("in_workday_ref"))
                    .select("fee_code").distinct().collect())
    if missing_cted:
        raise Exception(f"CTED codes {[r['fee_code'] for r in missing_cted]} are not in Workday's "
                        f"Program Fee Code list; CTED DAX filters on these codes, so fix before loading.")

    if not INCLUDE_UNMAPPED:
        mapped = mapped.filter(F.col("in_workday_ref"))

    # ------------------------------------------------------------------
    # 6. Never overwrite Workday: skip sections that already have a
    #    Program Fee Code row from Workday.
    # ------------------------------------------------------------------
    wd_has_fee = (spark.read.format("delta").load(tax_path)
                  .filter((F.col("scheme") == FEE_SCHEME) & (F.col("source") == "workday"))
                  .select("course_section_key").distinct())

    final_df = mapped.join(wd_has_fee, "course_section_key", "left_anti")

    # ------------------------------------------------------------------
    # 7. Shape to the dim_course_section_taxonomy schema.
    #    Key includes the CODE so multiple fee codes per section stay unique.
    #    contract_status follows the codes CTED treats as contract/grant.
    # ------------------------------------------------------------------
    final_df = final_df.select(
        F.sha2(F.concat_ws("||",
                           F.coalesce(F.col("course_section_key").cast("string"), F.lit("")),
                           F.coalesce(F.col("id").cast("string"), F.lit("")),
                           F.coalesce(F.col("fee_code"), F.lit(""))), 256).alias("taxonomy_key"),
        "course_section_key",
        "course_section_id",
        F.col("fee_code").alias("code"),
        "name",
        "id",
        F.lit(FEE_SCHEME).alias("scheme"),
        F.when(F.col("fee_code").isin(CONTRACT_CODES), "In-Contract")
         .otherwise("Non-Contract").alias("contract_status"),
        F.lit(SOURCE_TAG).alias("source"),
    ).dropDuplicates(["taxonomy_key"])

    # ------------------------------------------------------------------
    # 8. Review: CTED registrations by academic year from this backfill
    # ------------------------------------------------------------------
    cted_sections = (final_df.filter(F.col("code").isin(CONTRACT_CODES))
                     .groupBy("course_section_key")
                     .agg(F.max(F.when(F.col("code").isin(CTED_WAT), 1).otherwise(0)).alias("has_wat")))

    cted_review = (spark.read.format("delta").load(reg_path)
                   .filter(F.col("registration_status").isin("Completed", "Registered", "Withdrawn"))
                   .select("course_section_key", "academic_period_key")
                   .join(cted_sections, "course_section_key", "inner")
                   .join(spark.read.format("delta").load(ap_path)
                         .select("academic_period_key", "academic_year"), "academic_period_key")
                   .groupBy("academic_year")
                   .agg(F.count("*").alias("cted_registrations"),
                        F.sum("has_wat").alias("wat_registrations"))
                   .orderBy("academic_year"))
    print("CTED registrations by academic year (review before and after load):")
    # display(cted_review)

    # ------------------------------------------------------------------
    # 9. Idempotent write: remove previous PS fee code backfill, then append
    # ------------------------------------------------------------------
    pl_audit_df = add_audit_column(final_df, PIPELINE_NAME)
    final_count = pl_audit_df.count()

    DeltaTable.forPath(spark, tax_path).delete(
        (F.col("source") == SOURCE_TAG) & (F.col("scheme") == FEE_SCHEME)
    )
    pl_audit_df.write.format("delta").mode("append").option("mergeSchema", True).save(tax_path)

    rows_read    = total
    rows_written = final_count
    print(f"Program Fee Code backfill complete: {rows_written} section-code rows written.")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed processing fee_code_backfill: {e}")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Aid Code historical backfill: PeopleSoft -> dim_course_section_taxonomy
#
# Why: Aid Code exists on Workday-native sections only (0% coverage before
#      AY2025). PeopleSoft holds it as class attribute CRSE_ATTR = 'CODE'
#      (validated: 99.6% match to SC_AID_CD on PS_NW_CLI_ENRL_DW).
#
# Grain: one row per course section. Aid Code is a course-level attribute,
#        so section grain is correct (no student-level split as with RCC).
#
# Re-runnable: deletes its own previous output (source = 'peoplesoft',
#              scheme = 'Aid Code') before appending.
# ============================================================================

AID_SCHEME       = "Aid Code"
PS_ATTR          = "CODE"
SOURCE_TAG       = "peoplesoft"
MIN_REG_COVERAGE = 0.85   # stop if fewer historical registrations than this get an aid code
PS_FIRST_AY      = "2001-2002"   # first academic year PS CODE is reliably populated
PS_LAST_AY       = "2024-2025"   # last PeopleSoft-era academic year
INCLUDE_UNMAPPED = True   # write PS-only codes (49, 77, 78) with PS description
                          # -> pending NWTC decision; set False to exclude them

SECTION_KEYS = ["CRSE_ID", "CRSE_OFFER_NBR", "STRM", "SESSION_CODE", "CLASS_SECTION"]

try:
    # ------------------------------------------------------------------
    # 0. Paths
    # ------------------------------------------------------------------
    class_tbl_path  = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CLASS_TBL_SE_VW")
    class_attr_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CLASS_ATTRIBUTE")
    attr_val_path   = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CRSE_ATTR_VALUE")
    ref_path        = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "educational_taxonomy_code_references")
    scheme_path     = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "educational_taxonomy_schemes")
    cs_path         = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_sections")
    tax_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_section_taxonomy")
    reg_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_registration")
    ap_path         = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")

    def trimmed(col):
        return F.trim(F.col(col).cast("string")).alias(col)

    # ------------------------------------------------------------------
    # 1. PeopleSoft sections (one row per section)
    #    STRM guard: only '2' + YY + 2/5/8 -- ignores old-scheme 0xxx terms
    # ------------------------------------------------------------------
    sections = (
        spark.read.format("delta").load(class_tbl_path)
        .select(*[trimmed(k) for k in SECTION_KEYS],
                F.trim(F.col("CATALOG_NBR")).alias("catalog_nbr"),
                F.trim(F.col("CLASS_NBR").cast("string")).alias("class_nbr"))
        .filter(F.col("STRM").rlike("^2[0-9]{2}[258]$"))
        .distinct()
    )

    # ------------------------------------------------------------------
    # 2. Aid Code per section from PS_CLASS_ATTRIBUTE
    # ------------------------------------------------------------------
    ps_aid = (
        spark.read.format("delta").load(class_attr_path)
        .filter(F.trim(F.col("CRSE_ATTR")) == PS_ATTR)
        .select(*[trimmed(k) for k in SECTION_KEYS],
                F.trim(F.col("CRSE_ATTR_VALUE")).alias("aid_code"))
        .filter(F.col("aid_code").isNotNull() & (F.col("aid_code") != ""))
        .distinct()
    )

    sec_aid = sections.join(ps_aid, SECTION_KEYS, "inner")

    # ------------------------------------------------------------------
    # 3. One aid code per section. Report conflicts, keep lowest code.
    # ------------------------------------------------------------------
    sec_id_cols = ["STRM", "catalog_nbr", "class_nbr"]

    conflicts = (sec_aid.groupBy(*sec_id_cols)
                 .agg(F.countDistinct("aid_code").alias("codes"))
                 .filter("codes > 1"))
    print(f"Sections with more than one aid code: {conflicts.count()}")

    w_pick = Window.partitionBy(*sec_id_cols).orderBy("aid_code")
    sec_aid = (sec_aid
               .withColumn("rn", F.row_number().over(w_pick))
               .filter("rn = 1").drop("rn")
               .select(*sec_id_cols, "aid_code"))

    # ------------------------------------------------------------------
    # 4. Build the Workday course_section_id.
    #    Use the TERM'S CALENDAR YEAR (Fall 2023 -> '2023_Fall'),
    #    never the academic year.
    # ------------------------------------------------------------------
    term_digit = F.substring("STRM", 4, 1)
    sec_aid = (
        sec_aid
        .withColumn("term_year",
                    (F.lit(2000) + F.substring("STRM", 2, 2).cast("int")).cast("string"))
        .withColumn("term_name",
                    F.when(term_digit == "2", "Spring")
                     .when(term_digit == "5", "Summer")
                     .when(term_digit == "8", "Fall"))
        .withColumn("ps_section_id",
                    F.concat_ws("_", "catalog_nbr", "class_nbr", "term_year", "term_name"))
    )

    # ------------------------------------------------------------------
    # 5. Link to Workday sections.
    #    From 2023 onward Workday IDs carry a session suffix:
    #      PS built : 10809198_81725_2023_Fall
    #      Workday  : 10809198_81725_2023_Fall_Session_1
    #    Strip the suffix on the Workday side, match, and keep the
    #    Workday course_section_id for output.
    # ------------------------------------------------------------------
    cs = (spark.read.format("delta").load(cs_path)
          .select("course_section_key", "course_section_id")
          .withColumn("base_section_id",
                      F.regexp_replace("course_section_id", r"_Session_[0-9]+$", "")))

    # Two Workday sections can share a base ID (seen around the 2024-25
    # cutover). Aid Code is a course attribute and both sections are the
    # same course/class/term, so each gets the same aid code. That is one
    # taxonomy row per section -- correct, not a double count. Report only.
    dup_base = cs.groupBy("base_section_id").count().filter("count > 1")
    dup_count = dup_base.count()
    if dup_count > 0:
        print(f"Note: {dup_count} base section IDs map to more than one Workday section; "
              f"the same aid code will be written to each. Pairs listed below.")
        # display(cs.join(dup_base.select("base_section_id"), "base_section_id")
        #           .orderBy("base_section_id", "course_section_id"))

    linked = sec_aid.join(cs, F.col("ps_section_id") == F.col("base_section_id"), "left")

    # Informational only: PS sections with no Workday section are usually
    # sections nobody was registered in, so they don't affect reporting.
    match_by_term = (linked.groupBy("STRM")
                     .agg(F.count("*").alias("ps_sections"),
                          F.count("course_section_key").alias("linked"))
                     .withColumn("match_pct", F.round(100 * F.col("linked") / F.col("ps_sections"), 1))
                     .orderBy("STRM"))
    # display(match_by_term)

    total  = linked.count()
    linked = linked.filter(F.col("course_section_key").isNotNull())

    # ------------------------------------------------------------------
    # 5b. Gate on what matters: share of historical REGISTRATIONS whose
    #     section will now carry an aid code (PS backfill or Workday).
    # ------------------------------------------------------------------
    covered_keys = (linked.select("course_section_key")
                    .union(spark.read.format("delta").load(tax_path)
                           .filter((F.col("scheme") == AID_SCHEME) & (F.col("source") == "workday"))
                           .select("course_section_key"))
                    .distinct()
                    .withColumn("covered", F.lit(1)))

    regs = (spark.read.format("delta").load(reg_path)
            .filter(F.col("registration_status").isin("Completed", "Registered", "Withdrawn"))
            .select("course_section_key", "academic_period_key")
            .join(spark.read.format("delta").load(ap_path)
                  .select("academic_period_key", "academic_year"), "academic_period_key")
            .filter((F.col("academic_year") >= PS_FIRST_AY) & (F.col("academic_year") <= PS_LAST_AY))
            .join(covered_keys, "course_section_key", "left"))

    cov_by_year = (regs.groupBy("academic_year")
                   .agg(F.count("*").alias("registrations"),
                        F.sum(F.coalesce("covered", F.lit(0))).alias("with_aid_code"))
                   .withColumn("pct", F.round(100 * F.col("with_aid_code") / F.col("registrations"), 1))
                   .orderBy("academic_year"))
    # display(cov_by_year)

    tot = cov_by_year.agg(F.sum("registrations").alias("r"), F.sum("with_aid_code").alias("c")).first()
    reg_rate = (tot["c"] / tot["r"]) if tot["r"] else 0.0
    print(f"Historical registration coverage {PS_FIRST_AY}..{PS_LAST_AY}: {reg_rate:.1%}")

    if reg_rate < MIN_REG_COVERAGE:
        raise Exception(f"Registration coverage {reg_rate:.1%} is below {MIN_REG_COVERAGE:.0%}. "
                        f"Check the low years above before writing.")

    # ------------------------------------------------------------------
    # 6. Map code -> Workday reference, WITHIN the Aid Code scheme only.
    #    (Fee codes share numeric values, e.g. 43 matches on MFEE.)
    # ------------------------------------------------------------------
    scheme = (spark.read.format("delta").load(scheme_path)
              .filter(F.col("name") == AID_SCHEME)
              .select(F.col("reference_id").alias("scheme_id")))

    scheme_id_value = scheme.first()["scheme_id"]

    wd_ref = (spark.read.format("delta").load(ref_path)
              .join(scheme, F.col("educational_taxonomy_id") == F.col("scheme_id"), "inner")
              .select(F.trim(F.col("code")).alias("aid_code"),
                      F.col("name").alias("wd_name"),
                      F.col("educational_taxonomy_id").alias("wd_id"))
              .dropDuplicates(["aid_code"]))

    # PeopleSoft descriptions (latest EFFDT) for codes Workday doesn't have
    w_desc = Window.partitionBy("aid_code").orderBy(F.col("EFFDT").desc())
    ps_desc = (spark.read.format("delta").load(attr_val_path)
               .filter(F.trim(F.col("CRSE_ATTR")) == PS_ATTR)
               .select(F.trim(F.col("CRSE_ATTR_VALUE")).alias("aid_code"),
                       F.col("DESCR").alias("ps_name"), "EFFDT")
               .withColumn("rn", F.row_number().over(w_desc))
               .filter("rn = 1").drop("rn", "EFFDT"))

    mapped = (linked
              .join(wd_ref, "aid_code", "left")
              .join(ps_desc, "aid_code", "left")
              .withColumn("name",    F.coalesce("wd_name", "ps_name"))
              .withColumn("id",      F.coalesce("wd_id", F.lit(scheme_id_value)))
              .withColumn("in_workday_ref", F.col("wd_name").isNotNull()))

    print("Aid codes with no Workday reference (PS description used):")
    # display(mapped.filter(~F.col("in_workday_ref"))
    #               .groupBy("aid_code", "ps_name").count().orderBy("aid_code"))

    if not INCLUDE_UNMAPPED:
        mapped = mapped.filter(F.col("in_workday_ref"))

    # ------------------------------------------------------------------
    # 7. Never overwrite Workday: skip sections that already have an
    #    Aid Code row from Workday.
    # ------------------------------------------------------------------
    wd_has_aid = (spark.read.format("delta").load(tax_path)
                  .filter((F.col("scheme") == AID_SCHEME) & (F.col("source") == "workday"))
                  .select("course_section_key").distinct())

    final_df = mapped.join(wd_has_aid, "course_section_key", "left_anti")

    # ------------------------------------------------------------------
    # 8. Shape to the dim_course_section_taxonomy schema
    #    (same columns and key rule as the existing taxonomy build)
    # ------------------------------------------------------------------
    final_df = final_df.select(
        F.sha2(F.concat_ws("||",
                           F.coalesce(F.col("course_section_key").cast("string"), F.lit("")),
                           F.coalesce(F.col("id").cast("string"), F.lit(""))), 256).alias("taxonomy_key"),
        "course_section_key",
        "course_section_id",
        F.col("aid_code").alias("code"),
        "name",
        "id",
        F.lit(AID_SCHEME).alias("scheme"),
        F.lit(None).cast("string").alias("contract_status"),   # align with Workday Aid Code rows
        F.lit(SOURCE_TAG).alias("source"),
    )

    pl_audit_df = add_audit_column(final_df, PIPELINE_NAME)
    final_count = pl_audit_df.count()

    # display(pl_audit_df)

    # ------------------------------------------------------------------
    # 9. Idempotent write: remove previous PS Aid Code backfill, then append
    # ------------------------------------------------------------------
    DeltaTable.forPath(spark, tax_path).delete(
        (F.col("source") == SOURCE_TAG) & (F.col("scheme") == AID_SCHEME)
    )
    pl_audit_df.write.format("delta").mode("append").option("mergeSchema", True).save(tax_path)

    rows_read    = total
    rows_written = final_count
    print(f"Aid Code backfill complete: {rows_written} section rows written.")

except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed processing aid_code_backfill: {e}")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================================
# Recognized Credit Code (RCC) historical backfill:
#   PeopleSoft RCC -> dim_course_section_taxonomy
#
# Why: RCC codes (incl. 8A Transcript Credit) exist on Workday sections only
#      from AY2025-26. NWTC's CSR excludes 8A (high-school-taught transcript
#      credit) and keeps all other dual credit RCCs, so 8A is needed for
#      every historical year.
#
# Source: PS_CLASS_ATTRIBUTE, CRSE_ATTR = 'RCC' (8A present every term
#         2005-2025, ~600-680 classes a year recently).
#
# Same pattern as the Program Fee Code backfill:
#   - a section can carry MORE THAN ONE RCC -> keep all of them
#   - taxonomy_key includes the code, so multiple codes per section stay unique
#   - Workday wins: sections that already have an RCC from Workday are skipped
#
# Re-runnable: deletes its own previous output (source = 'peoplesoft',
#              scheme = 'Recognized Credit Code') before appending.
#
# Add this as a new cell in nb_dim_course_section_taxonomy_peoplesoft,
# after the Aid Code cell and before the success-logging cell.
# ============================================================================
 
RCC_SCHEME       = "Recognized Credit Code"
PS_ATTR          = "RCC"
SOURCE_TAG       = "peoplesoft"
TRANSCRIPT_CODE  = "8A"
INCLUDE_UNMAPPED = True   # write PS-only RCC values with PS description
 
SECTION_KEYS = ["CRSE_ID", "CRSE_OFFER_NBR", "STRM", "SESSION_CODE", "CLASS_SECTION"]
 
try:
    # ------------------------------------------------------------------
    # 0. Paths
    # ------------------------------------------------------------------
    class_tbl_path  = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CLASS_TBL_SE_VW")
    class_attr_path = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CLASS_ATTRIBUTE")
    attr_val_path   = abfss_path_builder(workspace_id, bronze_lakehouse_id, "peoplesoft", "PS_CRSE_ATTR_VALUE")
    ref_path        = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "educational_taxonomy_code_references")
    scheme_path     = abfss_path_builder(workspace_id, silver_lakehouse_id, silver_schema_name, "educational_taxonomy_schemes")
    cs_path         = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_sections")
    tax_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_course_section_taxonomy")
    reg_path        = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "fact_registration")
    ap_path         = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "dim_academic_period")
 
    def trimmed(col):
        return F.trim(F.col(col).cast("string")).alias(col)
 
    # ------------------------------------------------------------------
    # 1. PeopleSoft sections (one row per section)
    #    STRM guard: only '2' + YY + 2/5/8 -- ignores old-scheme 0xxx terms
    # ------------------------------------------------------------------
    sections = (
        spark.read.format("delta").load(class_tbl_path)
        .select(*[trimmed(k) for k in SECTION_KEYS],
                F.trim(F.col("CATALOG_NBR")).alias("catalog_nbr"),
                F.trim(F.col("CLASS_NBR").cast("string")).alias("class_nbr"))
        .filter(F.col("STRM").rlike("^2[0-9]{2}[258]$"))
        .distinct()
    )
 
    # ------------------------------------------------------------------
    # 2. RCC value(s) per section from PS_CLASS_ATTRIBUTE
    #    Keep EVERY code -- a section can carry more than one.
    # ------------------------------------------------------------------
    ps_rcc = (
        spark.read.format("delta").load(class_attr_path)
        .filter(F.trim(F.col("CRSE_ATTR")) == PS_ATTR)
        .select(*[trimmed(k) for k in SECTION_KEYS],
                F.upper(F.trim(F.col("CRSE_ATTR_VALUE"))).alias("rcc_code"))
        .filter(F.col("rcc_code").isNotNull() & (F.col("rcc_code") != ""))
        .distinct()
    )
 
    sec_id_cols = ["STRM", "catalog_nbr", "class_nbr"]
    sec_rcc = (sections.join(ps_rcc, SECTION_KEYS, "inner")
               .select(*sec_id_cols, "rcc_code")
               .distinct())
 
    multi = (sec_rcc.groupBy(*sec_id_cols).agg(F.countDistinct("rcc_code").alias("codes"))
             .filter("codes > 1").count())
    print(f"Sections with more than one RCC (all kept): {multi}")
 
    # ------------------------------------------------------------------
    # 3. Build the PS section ID from the TERM'S CALENDAR YEAR
    # ------------------------------------------------------------------
    term_digit = F.substring("STRM", 4, 1)
    sec_rcc = (
        sec_rcc
        .withColumn("term_year",
                    (F.lit(2000) + F.substring("STRM", 2, 2).cast("int")).cast("string"))
        .withColumn("term_name",
                    F.when(term_digit == "2", "Spring")
                     .when(term_digit == "5", "Summer")
                     .when(term_digit == "8", "Fall"))
        .withColumn("ps_section_id",
                    F.concat_ws("_", "catalog_nbr", "class_nbr", "term_year", "term_name"))
    )
 
    # ------------------------------------------------------------------
    # 4. Link to Workday sections (strip '_Session_N' from Workday IDs)
    # ------------------------------------------------------------------
    cs = (spark.read.format("delta").load(cs_path)
          .select("course_section_key", "course_section_id")
          .withColumn("base_section_id",
                      F.regexp_replace("course_section_id", r"_Session_[0-9]+$", "")))
 
    linked = sec_rcc.join(cs, F.col("ps_section_id") == F.col("base_section_id"), "left")
 
    # Match rate by term, for 8A specifically (what CSR depends on)
    match_8a = (linked.filter(F.col("rcc_code") == TRANSCRIPT_CODE)
                .groupBy("STRM")
                .agg(F.count("*").alias("ps_8a_sections"),
                     F.count("course_section_key").alias("linked"))
                .withColumn("match_pct", F.round(100 * F.col("linked") / F.col("ps_8a_sections"), 1))
                .orderBy("STRM"))
    print("8A section match rate by term (PeopleSoft -> Workday):")
    display(match_8a)
 
    total  = linked.count()
    linked = linked.filter(F.col("course_section_key").isNotNull())
    if linked.filter(F.col("rcc_code") == TRANSCRIPT_CODE).count() == 0:
        raise Exception("No PeopleSoft 8A sections linked to Workday sections; check section ID format.")
 
    # ------------------------------------------------------------------
    # 5. Map code -> Workday reference WITHIN the RCC scheme
    # ------------------------------------------------------------------
    scheme = (spark.read.format("delta").load(scheme_path)
              .filter(F.col("name") == RCC_SCHEME)
              .select(F.col("reference_id").alias("scheme_id")))
    scheme_id_value = scheme.first()["scheme_id"]
 
    wd_ref = (spark.read.format("delta").load(ref_path)
              .join(scheme, F.col("educational_taxonomy_id") == F.col("scheme_id"), "inner")
              .select(F.upper(F.trim(F.col("code"))).alias("rcc_code"),
                      F.col("name").alias("wd_name"),
                      F.col("educational_taxonomy_id").alias("wd_id"))
              .dropDuplicates(["rcc_code"]))
 
    w_desc = Window.partitionBy("rcc_code").orderBy(F.col("EFFDT").desc())
    ps_desc = (spark.read.format("delta").load(attr_val_path)
               .filter(F.trim(F.col("CRSE_ATTR")) == PS_ATTR)
               .select(F.upper(F.trim(F.col("CRSE_ATTR_VALUE"))).alias("rcc_code"),
                       F.col("DESCR").alias("ps_name"), "EFFDT")
               .withColumn("rn", F.row_number().over(w_desc))
               .filter("rn = 1").drop("rn", "EFFDT"))
 
    mapped = (linked
              .join(wd_ref, "rcc_code", "left")
              .join(ps_desc, "rcc_code", "left")
              .withColumn("name", F.coalesce("wd_name", "ps_name"))
              .withColumn("id",   F.coalesce("wd_id", F.lit(scheme_id_value)))
              .withColumn("in_workday_ref", F.col("wd_name").isNotNull()))
 
    # 8A must resolve to Workday's 8A reference, or BI filters won't line up
    if mapped.filter((F.col("rcc_code") == TRANSCRIPT_CODE) & ~F.col("in_workday_ref")).limit(1).count() > 0:
        raise Exception("8A is not in Workday's Recognized Credit Code list; fix the reference mapping before loading.")
 
    print("RCC values with no Workday reference (PS description used):")
    display(mapped.filter(~F.col("in_workday_ref"))
                  .groupBy("rcc_code", "ps_name").count().orderBy("rcc_code"))
 
    if not INCLUDE_UNMAPPED:
        mapped = mapped.filter(F.col("in_workday_ref"))
 
    # ------------------------------------------------------------------
    # 6. Never overwrite Workday: skip sections that already have an
    #    RCC row from Workday.
    # ------------------------------------------------------------------
    wd_has_rcc = (spark.read.format("delta").load(tax_path)
                  .filter((F.col("scheme") == RCC_SCHEME) & (F.col("source") == "workday"))
                  .select("course_section_key").distinct())
 
    final_df = mapped.join(wd_has_rcc, "course_section_key", "left_anti")
 
    # ------------------------------------------------------------------
    # 7. Shape to the dim_course_section_taxonomy schema.
    #    Key includes the CODE so multiple RCCs per section stay unique.
    #    contract_status follows the existing Workday rule (RCCs are never
    #    28/32/43, so always Non-Contract).
    # ------------------------------------------------------------------
    final_df = final_df.select(
        F.sha2(F.concat_ws("||",
                           F.coalesce(F.col("course_section_key").cast("string"), F.lit("")),
                           F.coalesce(F.col("id").cast("string"), F.lit("")),
                           F.coalesce(F.col("rcc_code"), F.lit(""))), 256).alias("taxonomy_key"),
        "course_section_key",
        "course_section_id",
        F.col("rcc_code").alias("code"),
        "name",
        "id",
        F.lit(RCC_SCHEME).alias("scheme"),
        F.lit("Non-Contract").alias("contract_status"),
        F.lit(SOURCE_TAG).alias("source"),
    ).dropDuplicates(["taxonomy_key"])
 
    # ------------------------------------------------------------------
    # 8. Review: CSR-eligible registrations in 8A sections by academic year
    #    (compare with NWTC: all-dual vs 8A-only gap is ~10K per year)
    # ------------------------------------------------------------------
    sec_8a = (final_df.filter(F.col("code") == TRANSCRIPT_CODE)
              .select("course_section_key")
              .union(spark.read.format("delta").load(tax_path)
                     .filter((F.col("scheme") == RCC_SCHEME) & (F.col("source") == "workday")
& (F.col("code") == TRANSCRIPT_CODE))
                     .select("course_section_key"))
              .distinct())
 
    review_8a = (spark.read.format("delta").load(reg_path)
                 .select("course_section_key", "academic_period_key")
                 .join(sec_8a, "course_section_key", "inner")
                 .join(spark.read.format("delta").load(ap_path)
                       .select("academic_period_key", "academic_year"), "academic_period_key")
                 .groupBy("academic_year")
                 .agg(F.count("*").alias("registrations_in_8a_sections"))
                 .orderBy("academic_year"))
    print("Registrations in 8A sections by academic year (PeopleSoft + Workday):")
    display(review_8a)
 
    # ------------------------------------------------------------------
    # 9. Idempotent write: remove previous PS RCC backfill, then append
    # ------------------------------------------------------------------
    pl_audit_df = add_audit_column(final_df, PIPELINE_NAME)
    final_count = pl_audit_df.count()
 
    DeltaTable.forPath(spark, tax_path).delete(
        (F.col("source") == SOURCE_TAG) & (F.col("scheme") == RCC_SCHEME)
    )
    pl_audit_df.write.format("delta").mode("append").option("mergeSchema", True).save(tax_path)
 
    rows_read    = total
    rows_written = final_count
    print(f"RCC backfill complete: {rows_written} section-code rows written.")
 
except Exception as e:
    end_time = datetime.now()
    insert_pipeline_monitoring_record(
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME),
        _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(source_table), _safe_str(target_table), 0, 0, 0,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME),
        _safe_str(WORKSPACE_ID), _safe_str(PIPELINE_NAME),
        rerun_count=0
    )
    raise Exception(f"Failed processing rcc_backfill: {e}")

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
