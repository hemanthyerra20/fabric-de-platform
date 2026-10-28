# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from datetime import datetime
import time
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType,
    IntegerType, TimestampType
)
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
