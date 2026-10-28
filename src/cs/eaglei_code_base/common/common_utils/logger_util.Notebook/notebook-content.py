# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import logging
import sys
from typing import Optional

def get_logger(
    name: str,
    level: int = logging.INFO,
    job_name: Optional[str] = None
) -> logging.Logger:
    """
    Creates and returns a configured logger.

    Parameters:
        name (str): Logger name (usually __name__)
        level (int): Logging level
        job_name (str): Optional job or pipeline name

    Returns:
        logging.Logger
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate logs in Databricks notebooks
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)

    log_format = (
        "%(asctime)s | %(levelname)s | %(name)s | "
        + (f"Job Name: {job_name} | " if job_name else "")
        + "%(message)s"
    )

    formatter = logging.Formatter(
        fmt=log_format,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    return logger

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
