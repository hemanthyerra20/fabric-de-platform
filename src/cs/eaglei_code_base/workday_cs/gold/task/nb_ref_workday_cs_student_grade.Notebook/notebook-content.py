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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = get_variable(VAR_CS_LIB_NAME, "cs_store_workspace_id")
silver_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_silver_id")
bronze_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_bronze_id")
gold_lakehouse_id = get_variable(VAR_CS_LIB_NAME, "lh_cs_gold_id")
silver_schema_name = "workday_cs"
gold_schema_name = "workday_cs"
start_time = datetime.now()
source_table = "silver_tables"
target_table = f"lh_cs_gold.{gold_schema_name}.ref_student_grade"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    # ------------------------------------------------------------
    # Load Excel files from lh_cs_gold Files/student_grade
    # Pattern: NWTC_GRADE_CHANGES_<suffix>.xlsx  (e.g. AY20, AY21, ... AY25)
    # Union all matched files into one DataFrame and overwrite the target
    # gold reference table.
    # ------------------------------------------------------------

    import io
    import re
    import pandas as pd
    from functools import reduce

    # Build the abfss path to the Excel folder (Files/student_grade) and to
    # the physical target ref table (Tables/<schema>/ref_student_grade),
    # using the same abfss_path_builder pattern already proven elsewhere in
    # this pipeline, instead of saveAsTable with a 3-part catalog name.
    onelake_endpoint = notebookutils.conf.get("trident.onelake.endpoint").replace("https://", "")
    excel_dir = f"abfss://{workspace_id}@{onelake_endpoint}/{gold_lakehouse_id}/Files/student_grade"
    excel_pattern_path = f"{excel_dir}/NWTC_GRADE_CHANGES_*.xlsx"
    gold_abfss_path = abfss_path_builder(workspace_id, gold_lakehouse_id, gold_schema_name, "ref_student_grade")

    # com.crealytics.spark.excel isn't available in this Fabric environment,
    # so each matched file is read as raw bytes with Spark, parsed with
    # pandas, then converted to a Spark DataFrame.
    binary_df = spark.read.format("binaryFile").load(excel_pattern_path)
    file_rows = binary_df.select("path", "content").collect()

    if not file_rows:
        raise FileNotFoundError(f"No Excel files found for pattern: {excel_pattern_path}")

    print(f"Found {len(file_rows)} Excel file(s) matching {excel_pattern_path}")

    # Extracts the portion of the filename that identifies the academic year,
    # e.g. "AY24" from "NWTC_GRADE_CHANGES_AY24.xlsx".
    FILENAME_PATTERN = re.compile(r"NWTC_GRADE_CHANGES_(.+)\.xlsx$", re.IGNORECASE)

    def _sanitize_column_names(columns):
        """
        Delta does not allow spaces or the characters ' ,;{}()\n\t=' in
        column names (DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES). Excel
        headers routinely contain spaces (e.g. "Student ID"), which pandas
        and Spark both tolerate right up until the Delta write. Replace any
        run of those characters with a single underscore, trim leading/
        trailing underscores, fall back to a positional name if a header is
        empty/all-invalid after sanitizing, and de-duplicate in case two
        different original headers collapse to the same sanitized name.
        """
        invalid_chars_pattern = re.compile(r"[ ,;{}()\n\t=]+")
        sanitized = []
        seen = {}
        for idx, col in enumerate(columns):
            name = invalid_chars_pattern.sub("_", str(col).strip()).strip("_")
            if not name:
                name = f"column_{idx}"
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 0
            sanitized.append(name)
        return sanitized


    pandas_frames = []
    read_errors = []

    for row in file_rows:
        file_path = row["path"]
        file_name = file_path.rsplit("/", 1)[-1]
        match = FILENAME_PATTERN.search(file_name)
        academic_year = match.group(1) if match else None

        try:
            # Read whichever sheet is actually first in the file rather than
            # assuming every year's workbook names it "Sheet1".
            excel_file = pd.ExcelFile(io.BytesIO(row["content"]), engine="openpyxl")
            sheet_name = excel_file.sheet_names[0]
            pdf = excel_file.parse(sheet_name=sheet_name)
        except Exception as file_err:
            read_errors.append(f"{file_name}: {file_err}")
            continue

        # Normalize headers: sanitize invalid Delta column-name characters
        # (spaces, etc.) and strip stray whitespace, so the same logical
        # column isn't treated as different columns across years and the
        # write doesn't fail on DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES.
        pdf.columns = _sanitize_column_names(pdf.columns)

        # Coerce every column to string before handing off to Spark. Pandas'
        # type inference from Excel frequently disagrees across yearly files
        # for the exact same logical column (e.g. int64 vs object once a
        # later year has a blank cell), which breaks unionByName/Arrow
        # conversion. Stringifying here is the safe, lossless choice that
        # guarantees a stable union; downstream consumers can cast specific
        # columns to their proper type once the real schema is known.
        pdf = pdf.astype("string")

        pdf["source_file_name"] = file_name
        pdf["academic_year"] = academic_year

        pandas_frames.append(pdf)

    if not pandas_frames:
        raise RuntimeError(
            f"None of the {len(file_rows)} matched Excel file(s) could be read. "
            f"Errors: {read_errors}"
        )

    if read_errors:
        print(f"WARNING: {len(read_errors)} file(s) could not be read and were skipped: {read_errors}")

    spark_dfs = [spark.createDataFrame(pdf) for pdf in pandas_frames]

    # allowMissingColumns=True tolerates a column being added/removed in a
    # given year's file instead of hard-failing the whole load.
    excel_df = reduce(
        lambda d1, d2: d1.unionByName(d2, allowMissingColumns=True),
        spark_dfs,
    )

    # Add audit columns
    pl_audit_df = add_audit_column(excel_df, PIPELINE_NAME)

    # Convert all column names to lowercase for consistency
    for c in pl_audit_df.columns:
        pl_audit_df = pl_audit_df.withColumnRenamed(c, c.lower())

    # Drop Student_Name column (now 'student_name' after lowercasing)
    pl_audit_df_final = pl_audit_df\
                        .withColumn("term", F.coalesce(F.col("term"), F.col("term_updated")))\
                        .drop("student_name", "term_updated")

    # Row counts for monitoring
    final_count = pl_audit_df_final.count()
    rows_read = final_count
    rows_written = final_count

    # Overwrite the target ref table
    (
        pl_audit_df_final.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(gold_abfss_path)
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
    raise Exception(f"Failed processing ref_student_grade: {e}")


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
