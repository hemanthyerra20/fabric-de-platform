# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0277ffad-3784-4551-8d7d-66d1b8ff633a",
# META       "default_lakehouse_name": "lh_cs_bronze",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "fd975be1-c831-4267-b419-318d73f5b16b"
# META         },
# META         {
# META           "id": "0277ffad-3784-4551-8d7d-66d1b8ff633a"
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

# CELL ********************

JOB_NAME = "fact_program_data_load"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

JOB_NAME = "nb_load_survey_data"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
silver_schema_name = 'survey'
gold_schema_name = 'workday_cs'
bronze_lakehouse_name='lh_cs_bronze'
silver_lakehouse_name = 'lh_cs_silver'
gold_lakehouse_name = 'lh_cs_gold'
notebook_name = notebookutils.runtime.context["currentNotebookName"]
config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
#config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
store_workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
bronze_lakehouse_id=get_lakehouse_id(var_cs_lib_name, bronze_lakehouse_name)
silver_lakehouse_id = get_lakehouse_id(var_cs_lib_name, silver_lakehouse_name)
gold_lakehouse_id = get_lakehouse_id(var_cs_lib_name, gold_lakehouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
file_path = "/lakehouse/default/Files/survey/Graduate Outcomes Summary Data Updated 10.15.2026.xlsx"
pdf = pd.read_excel(
    file_path,
    sheet_name="2024-2025 Program Data",
    dtype=str,
    engine="openpyxl"
)
pdf.head()

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import *
import re
# --------------------------------------------------------
# Build ABFSS Paths
# --------------------------------------------------------
survey_silver_path = abfss_path_builder(
    store_workspace_id,
    silver_lakehouse_id,
    silver_schema_name,
    "graduate_outcomes_program_summary"
)
 
dim_program_path = abfss_path_builder(
    store_workspace_id,
    gold_lakehouse_id,
    gold_schema_name,
    "dim_program"
)
#---------------------------------------------------------
# Bronze Excel Path
#---------------------------------------------------------
survey_df = spark.createDataFrame(pdf) 
#display(survey_df)
# --------------------------------------------------------
# Read Gold Dimension
# --------------------------------------------------------
dim_program_df = (
    spark.read
         .format("delta")
         .load(dim_program_path)
         .select(
             "program_key",
             "program_code",
             "program_name"
         )
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def standardize_column(col_name):
    col_name = col_name.lower().strip()
    col_name = col_name.replace("#","")
    col_name = col_name.replace("%","percent")
    col_name = re.sub(r"[()]", "", col_name)
    col_name = re.sub(r"[/-]", "_", col_name)
    col_name = re.sub(r"\s+", "_", col_name)
    col_name = re.sub(r"_+", "_", col_name)
    return col_name
survey_df = survey_df.toDF(
    *[standardize_column(c) for c in survey_df.columns]
)
#display(survey_df)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
 
# Maintain row order
survey_df = survey_df.withColumn(
    "row_id",
    F.monotonically_increasing_id()
)
 
# Identify college header rows
survey_df = survey_df.withColumn(
    "college_header",
    F.when(
        F.col("program_code").rlike("(?i)^college"),
        F.col("program_code")
    )
)
 
# Forward fill college name
w = Window.orderBy("row_id") \
          .rowsBetween(Window.unboundedPreceding, 0)
 
survey_df = survey_df.withColumn(
    "college_name",
    F.last("college_header", True).over(w)
)
 
# Remove the header rows themselves
survey_df = survey_df.filter(
    ~F.col("program_code").rlike("(?i)^college")
)
 
survey_df = survey_df.drop(
    "college_header",
    "row_id"
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

survey_df = (
    survey_df
    .withColumnRenamed("program_code","program_code")
    .withColumnRenamed("graduate_count","graduate_count")
    .withColumnRenamed("completed_survey_count","completed_survey_count")
    .withColumnRenamed("_in_labor_force","labor_force_count")
    .withColumnRenamed("employed_count","employed_count")
    .withColumnRenamed("employed_in_field_count","employed_in_field_count")
    .withColumnRenamed("employed_in_field_ft_count","employed_in_field_ft_count")
    .withColumnRenamed(
        "employed_in_field_ft_wage_reported_count",
        "employed_in_field_ft_wage_reported_count"
    )
    .withColumnRenamed(
        "median_annual_wage_for_ft_employed_in_field",
        "median_annual_wage_3yr_ft_in_field"
    )
)

#display(survey_df)

#median_annual_wage_for_ft_employed_in_field



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
#if "labor_force_count" not in survey_df.columns:
    #survey_df = survey_df.withColumn("labor_force_count", F.lit(0).cast("int"))
survey_df = (
    survey_df
    .withColumnRenamed("program_code","program_code")
    .withColumnRenamed("graduate_count","graduate_count")
    .withColumnRenamed("completed_survey_count","completed_survey_count")
    .withColumnRenamed("in_labor_force","labor_force_count")
    .withColumnRenamed("employed_count","employed_count")
    .withColumnRenamed("employed_in_field_count","employed_in_field_count")
    .withColumnRenamed("employed_in_field_ft_count","employed_in_field_ft_count")
    .withColumnRenamed(
        "employed_in_field_ft_wage_reported_count",
        "employed_in_field_ft_wage_reported_count"
    )
    .withColumnRenamed(
        "median_annual_wage_3yr_ft_in_field",
        "median_annual_wage_3yr_ft_in_field"
    )
)
#display(survey_df)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

graduate_col = [c for c in survey_df.columns if c.startswith("_of_graduates_ay_")][0]
survey_df = survey_df.withColumnRenamed(
    graduate_col,
    "graduate_count"
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

survey_df = (
    survey_df
    .withColumnRenamed("_of_completed_surveys", "completed_survey_count")
    #.withColumnRenamed("in_labor_force", "labor_force_count")
    .withColumnRenamed("_employed", "employed_count")
    .withColumnRenamed("_employed_in_field", "employed_in_field_count")
    .withColumnRenamed("_employed_in_field_full_time", "employed_in_field_ft_count")
    .withColumnRenamed(
        "_employed_in_field_full_time_with_reported_wages",
        "employed_in_field_ft_wage_reported_count"
        
    )

    .withColumnRenamed(
        "3_year_median_annual_wage_for_full_time_employed_in_field",
        #"median_annual_wage_for_full_time_employed_in_field",
        "median_annual_wage_3yr_ft_in_field"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

numeric_columns = [
    "graduate_count",
    "completed_survey_count",
    "labor_force_count",
    "employed_count",
    "employed_in_field_count",
    "employed_in_field_ft_count",
    "employed_in_field_ft_wage_reported_count"
]
 
for c in numeric_columns:
    survey_df = survey_df.withColumn(
        c,
        F.when(F.col(c)=="*",0)
         .otherwise(F.col(c))
    )
survey_df = survey_df.withColumn(
    "median_annual_wage_3yr_ft_in_field",
    F.when(
        F.col("median_annual_wage_3yr_ft_in_field")=="*",
        None
    ).otherwise(F.col("median_annual_wage_3yr_ft_in_field"))
)

 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

survey_df = (
    survey_df
    .withColumn(
        "graduate_count",
        F.col("graduate_count").cast("int")
    )
    .withColumn(
        "completed_survey_count",
        F.col("completed_survey_count").cast("int")

    )
    .withColumn(
        "labor_force_count",
        F.col("labor_force_count").cast("int")
    )
    .withColumn(
        "employed_count",
        F.col("employed_count").cast("int")
    )
    .withColumn(
        "employed_in_field_count",
        F.col("employed_in_field_count").cast("int")
    )
    .withColumn(
        "employed_in_field_ft_count",
        F.col("employed_in_field_ft_count").cast("int")
    )
    .withColumn(
        "employed_in_field_ft_wage_reported_count",
        F.col("employed_in_field_ft_wage_reported_count").cast("int")
    )
    .withColumn(
        "median_annual_wage_3yr_ft_in_field",
        F.regexp_replace(
            F.col("median_annual_wage_3yr_ft_in_field"),
            ",",
            ""
        ).cast("decimal(12,2)")
    )
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
sheet_name = "2024-2025 Program Data"
match = re.match(r'(\d{4})-(\d{4})', sheet_name)
start_year = int(match.group(1))  # 2024
end_year = int(match.group(2))    # 2025
academic_year = f"{start_year}-{end_year}"
wage_window_start_ay = start_year - 1
wage_window_end_ay = end_year
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

survey_silver_df = (
    survey_df
    .withColumn("academic_year", F.lit(academic_year))
    .withColumn("wage_window_start_ay", F.lit(wage_window_start_ay))
    .withColumn("wage_window_end_ay", F.lit(wage_window_end_ay))
    .withColumn(
        "is_wage_suppressed",
        F.when(
            F.col("median_annual_wage_3yr_ft_in_field").isNull(),
            True
        ).otherwise(False)
    )
    .withColumn("source_system", F.lit("survey"))
    .withColumn("load_date", F.current_date())
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

survey_silver_df = (
    survey_silver_df.select(
        "program_code",
        "program",
        "college_name",
        "academic_year",
        "graduate_count",
        "completed_survey_count",
        "labor_force_count",
        "employed_count",
        "employed_in_field_count",
        "employed_in_field_ft_count",
        "employed_in_field_ft_wage_reported_count",
        "median_annual_wage_3yr_ft_in_field",
        "wage_window_start_ay",
        "wage_window_end_ay",
        "is_wage_suppressed",
        "source_system",
        "load_date"
    )
)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS lh_cs_silver.survey;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    survey_silver_df.write
    .format("delta")
    .mode("append")
    #.option("overwriteSchema", "true")
    .saveAsTable("lh_cs_silver.survey.graduate_outcomes_program_summary")

)
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
