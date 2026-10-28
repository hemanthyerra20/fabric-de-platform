# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d35b1ce6-d133-4623-9d58-8c88aba0654b",
# META       "default_lakehouse_name": "lh_platform_monitoring",
# META       "default_lakehouse_workspace_id": "b0bd8a9b-9bdf-4a90-99cf-fed9e6800f1a",
# META       "known_lakehouses": [
# META         {
# META           "id": "d35b1ce6-d133-4623-9d58-8c88aba0654b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS config;
# MAGIC CREATE TABLE IF NOT EXISTS config.config_metadata_starfish
# MAGIC (
# MAGIC     source_system           STRING,
# MAGIC     schema_name             STRING,
# MAGIC     source_file_name        STRING,
# MAGIC     layer                   STRING,
# MAGIC     destination_table_name  STRING,
# MAGIC     load_required_flag      INT
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO config.config_metadata_starfish
# MAGIC VALUES
# MAGIC ('starfish','starfish','academiccalendar-calendar-nwtc.csv','bronze','academic_calendar',1),
# MAGIC ('starfish','starfish','survey-completedsurveys-nwtc.csv','bronze','completed_surveys',1),
# MAGIC ('starfish','starfish','kioskusage-fulldetail-nwtc.csv','bronze','kioskusage_fulldetail',1),
# MAGIC ('starfish','starfish','demographics-fulldetail-nwtc.csv','bronze','demographics_fulldetail',1),
# MAGIC ('starfish','starfish','meeting-fulldetail-nwtc.csv','bronze','meeting_fulldetail',1),
# MAGIC ('starfish','starfish','survey-trackitemsbysurveys-nwtc.csv','bronze','track_items_by_surveys',1),
# MAGIC ('starfish','starfish','studenttermstatus-fulldetail-nwtc.csv','bronze','student_term_status',1),
# MAGIC ('starfish','starfish','activity-appointment-nwtc.csv','bronze','activity_appointment',1),
# MAGIC ('starfish','starfish','tracking-fulldetail-nwtc.csv','bronze','tracking_fulldetail',1),
# MAGIC ('starfish','starfish','courseoutcomes-fulldetail-nwtc.csv','bronze','course_outcomes_fulldetail',1);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC update config.config_metadata_starfish
# MAGIC set load_required_flag=0
# MAGIC where destination_table_name='academic_calendar'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
