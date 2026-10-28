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
# META     },
# META     "environment": {
# META       "environmentId": "f45c2c8e-a6e5-4098-aaf6-567ab6b6ab98",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Canvas CD2 → Azure Fabric Landing Ingestion
# 
# **Purpose:** Orchestrate full-sync (snapshot) and incremental extraction from the Instructure Canvas Data 2 (CD2) REST API, decompress gzip-compressed **JSONL** files, and land data into Delta Lake tables following the canonical schema:
# 
# **Output format:** `jsonl` (newline-delimited JSON, gzip-compressed).  
# Each line in a downloaded file is a JSON object with three top-level keys:
# - `key` — primary-key fields used to derive `hashkey`
# - `value` — full data payload serialised into `rowdata`
# - `meta` — contains `action` (`upsert` | `delete`) and `ts` (event timestamp)
# 
# **Target naming convention:** `canv_{tablename}` inside the configured Lakehouse schema.
# 
# **Tables covered:**
# - `courses`, `course_sections`, `enrollments`, `enrollment_terms`
# - `learning_outcomes`, `learning_outcome_results`, `learning_outcome_groups`
# - `assignments`, `submissions`, `rubric_assessments`
# - `users`, `pseudonyms`
# 
# ---
# > **API Flow :**  
# > `GetJWT` → `Dispatch Query (jsonl)` → `Poll Job Status` → `Get Object URLs` → `Download & Decompress JSONL` → `Land to Delta`


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

# MARKDOWN ********************

# ## Cell 2 — Imports

# CELL ********************

# ============================================================================
# Core imports and logging setup for Canvas CD2 ingestion
# This cell initialises all Python, PySpark, Delta Lake and Fabric utilities
# required by the downstream Canvas CD2 data ingestion pipeline.
# ============================================================================
import gzip
import hashlib
import io
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import requests

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Microsoft Fabric utilities (notebookutils is injected automatically in Fabric)
import notebookutils

print("✅  Imports complete.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    force=True
)
logger = logging.getLogger("eagle_i.canvas.full_sync")
logger.info("Canvas Full Sync — initialised")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

MASTER_RUN_ID = "M123"
RUN_ID = ""
PIPELINE_ID = ""
PIPELINE_NAME = ""
DOMAIN = "cs"
SOURCE_NAME = "canvas"
TRIGGERED_BY = ""
ENVIRONMENT = "dev"
SOURCE_TABLE="users"
TARGET_TABLE="users"
SYNCMODE = "FULL"
# "FULL" | "INCREMENTAL"
INCREMENTALSINCE= "2026-09-04T16:20:46Z"
ACTIVITY_NAME = "notebook activity"
LEVEL="notebook"
LAYER="landing"
WORKSPACE_NAME= notebookutils.runtime.context["currentWorkspaceName"]
WORKSPACE_ID = notebookutils.runtime.context["currentWorkspaceId"]
NOTEBOOK_NAME = notebookutils.runtime.context["currentNotebookName"]
print(INCREMENTALSINCE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# JOB_NAME = "canvas_testing"
# var_cs_lib_name = "var_cs_process"
# audit_lakehouse_name = "lh_platform_monitoring"
# config_schema_name = "config"
# landing_schema_name = 'canvas'
# landing_lakehouse_name = 'lh_cs_landing'
# notebook_name = notebookutils.runtime.context["currentNotebookName"]
# config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
# config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)
# #config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)
# store_workspace_id = get_variable(var_cs_lib_name, "cs_store_workspace_id")
# landing_lakehouse_id = get_lakehouse_id(var_cs_lib_name,landing_lakehouse_name)
# landing_table_name =f"canv_{TARGET_TABLE}"

# canvas_target_landing_path = abfss_path_builder(
#     store_workspace_id,
#     landing_lakehouse_id,
#     landing_schema_name,
#     landing_table_name
# )
# canvas_df = (
#     spark.read.format("delta")
#     .load(canvas_target_landing_path)
# )
# incremental = (
#     canvas_df
#         .agg({"modified_at": "max"})
#         .collect()[0][0]
# )
# incremental_str = str(incremental)  # e.g. '2025-01-15 12:34:56'

# INCREMENTALSINCE = (
#     incremental_str[:10]      # '2025-01-15'
#     + "T" +
#     incremental_str[11:19]    # '12:34:56'
#     + "Z"
# )

# print(INCREMENTALSINCE)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

lh_name = "lh_cs_landing"
job_name = notebookutils.runtime.context.get("currentNotebookName")
source_name = "canvas"
var_cs_lib_name = "var_cs_process"
audit_lakehouse_name = "lh_platform_monitoring"
config_schema_name = "config"
config_table_name = "config_param"


config_workspace_id = get_variable(var_cs_lib_name, "platform_monitoring_workspace_id")
config_lakehouse_id = get_lakehouse_id(var_cs_lib_name, audit_lakehouse_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

config_abfss_path = abfss_path_builder(config_workspace_id, config_lakehouse_id, config_schema_name, config_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Fetching Config object values
config_df = spark.read.format("delta").load(config_abfss_path)
# display(config_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ⚙️ Cell 3 — Configuration & Parameters
# > Set `SYNC_MODE` to `"FULL"` for a complete snapshot or `"INCREMENTAL"` for delta since `INCREMENTAL_SINCE`.

# CELL ********************

# ============================================================================
# Configuration & notebook parameters
# This cell defines all runtime parameters and environment configuration for the
# Canvas CD2 ingestion pipeline, including:
#   * sync mode (FULL vs INCREMENTAL)
#   * Canvas CD2 API endpoints and format
#   * Key Vault secrets for authentication
#   * OneLake / Lakehouse locations for raw and curated data
#   * table list to ingest
# These values can be overridden via Fabric pipelines or notebook parameters.
# ============================================================================
# ── Notebook Parameters (overridable via Fabric Pipeline) ──────────────────────
SYNC_MODE          = SYNCMODE  # "FULL" | "INCREMENTAL"
INCREMENTAL_SINCE  = INCREMENTALSINCE # ISO-8601 UTC; used only for INCREMENTAL

# ── Canvas CD2 API ─────────────────────────────────────────────────────────────
BASE_URL           = get_param_value(config_df,source_name,"base_url")  # CD2 base URL
CD2_FORMAT         = get_param_value(config_df,source_name,"cd2_format") # jsonl output (gzip-compressed, one JSON object per line)

# ── Azure Key Vault ────────────────────────────────
KEY_VAULT_URL      = get_param_value(config_df,source_name,"kv_url")
SECRET_CLIENT_ID   = get_param_value(config_df,source_name,"kv_client_key_name")   # KV secret name
SECRET_CLIENT_SEC  = get_param_value(config_df,source_name,"kv_secret_key_name")   # KV secret name

# ── Landing Lakehouse ─────────────────────────────────────────────────────────
LANDING_SCHEMA     = get_param_value(config_df,source_name,"landing_schema_name")  # Fabric Lakehouse schema

# ── File Output (Lakehouse Files zone) ────────────────────────────────────────
# Fabric mounts the Lakehouse Files root at /lakehouse/default/Files on the driver.
# Both Python I/O (raw records) and Spark writes (landing data) resolve to the
# same OneLake path, visible in the Lakehouse Files explorer.
#
# Directory layout produced per run:
#   Files/
#     canvas_raw/
#       {table_name}/{SYNC_MODE}/run_{timestamp}/
#         canv_{table_name}_raw.jsonl.gz       ← original CD2 records, gzip-compressed
#     canvas_landing/
#       {table_name}/{SYNC_MODE}/run_{timestamp}/
#         canv_{table_name}_landing.jsonl      ← hashkey/rowdata/timestamps (Spark JSON write)
#

STORE_WORKSPACE_ID = get_workspace_id(var_cs_lib_name, DOMAIN.lower(), "store")
STORE_LAKEHOUSE_ID = get_lakehouse_id(var_cs_lib_name, lh_name)
root_path_suffix =  get_param_value(config_df,source_name,"root_path_suffix")

# print("LHID %s , WS ID %s",LANDING_WS.id,LANDING_LH.workspaceId)

# One lake abfss root for the Store Lakehouse
_ONELAKE_ROOT = f"abfss://{STORE_WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{STORE_LAKEHOUSE_ID}"
RAW_FILES_ROOT = f"{_ONELAKE_ROOT}/{root_path_suffix}"
RAW_TABLES_ROOT = f"{_ONELAKE_ROOT}/Tables"

# ── Lakehouse connectivity guard — fail fast before any write ────────────────
try:
    from notebookutils import mssparkutils as _mu
    _mu.fs.ls(f"{_ONELAKE_ROOT}/Files")
    logger.info("Store Lakehouse reachable — workspace=%s lakehouse=%s",
                STORE_WORKSPACE_ID, STORE_LAKEHOUSE_ID)
except Exception as _lh_err:
    raise EnvironmentError(
        f"Cannot reach Store Lakehouse (workspace={STORE_WORKSPACE_ID}, "
        f"lakehouse={STORE_LAKEHOUSE_ID}): {_lh_err}\n"
        "Check that the Fabric compute identity has Contributor access on the "
        "Store workspace and that the IDs in Key Vault are correct."
    ) from _lh_err

FILE_OUTPUT_BASE   = RAW_FILES_ROOT  # root of the Lakehouse Files zone
SAVE_RAW_FILE      = False   # write raw downloaded CD2 records to Files/canvas_raw/
SAVE_LANDING_FILE  = True   # write landing-schema data to Files/canvas_landing/

# ── Job Polling ───────────────────────────────────────────────────────────────
POLL_INTERVAL_SEC  = get_param_value(config_df,source_name,"poll_interval_sec") # seconds between status checks
MAX_WAIT_SEC       = 1800                    # maximum wait per table (30 min)

# ── Tables to process ─────────────────────────────────────────────────────────
# CANVAS_TABLES: List[str] = [
#     # Enrollment domain
#     # "courses",
#     # "course_sections",
#     # "enrollments",
#     # "enrollment_terms",
#     # # Learning Outcomes domain
#     # "learning_outcomes",
#     # "learning_outcome_results",
#     # "learning_outcome_groups",
#     # # Assignments domain
#     # "assignments",
#     # "submissions",
#     # "rubric_assessments",
#     # # User domain
#     # "users",
#     # "pseudonyms",
# ]

CANVAS_TABLES: List[str] = [SOURCE_TABLE]

print(f"✅  Config loaded  |  SYNC_MODE={SYNC_MODE}  |  Tables={len(CANVAS_TABLES)}")
print(BASE_URL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔐 Cell 4 — Retrieve Credentials & Authenticate

# CELL ********************

# ============================================================================
# Cell 4 – Credential retrieval and Canvas CD2 authentication
# This cell retrieves the OAuth client credentials from Azure Key Vault and
# obtains a short-lived JWT token from the Instructure IDS service. The token
# is then used to authenticate all subsequent Canvas CD2 API calls.
# ============================================================================
# ── Fetch secrets from Azure Key Vault ────────────────────────────────────────
CLIENT_ID     = notebookutils.credentials.getSecret(KEY_VAULT_URL, SECRET_CLIENT_ID)
CLIENT_SECRET = notebookutils.credentials.getSecret(KEY_VAULT_URL, SECRET_CLIENT_SEC)

# CLIENT_ID='us-west-2#73ca125a-418f-4c5d-a26b-b138cad7789c'
# CLIENT_SECRET='3OOBr3e7QiCEW3NqBrBX64NEtFqNane8YvmRCNRdfuk'

def get_auth_token(base_url: str, client_id: str, client_secret: str) -> str:
    """Authenticate against the Canvas IDS endpoint and return a JWT.

    Uses OAuth2 client_credentials:
      * POST {base_url}/ids/auth/login
      * Basic authentication with client_id and client_secret
      * Request body: grant_type=client_credentials

    The resulting access_token is required in the x-instauth header for all
    Canvas CD2 (DAP) API requests.
    """
    url = f"{base_url}/ids/auth/login"
    response = requests.post(
        url,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials"},
        headers={"Accept-Encoding": "identity"},  # disable compression on auth response
        timeout=30,
    )
    response.raise_for_status()
    return response.json()["access_token"]


AUTH_TOKEN = get_auth_token(BASE_URL, CLIENT_ID, CLIENT_SECRET)
print("✅  Authentication successful — JWT token obtained.")
# print(AUTH_TOKEN)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

def get_latest_canvas_watermark_since(
    spark,
    schema: str,
    target_table: str,
) -> Optional[str]:
    """
    Read the canvas_watermark Delta data and return the most recent last_refresh
    for the given target_table, as an ISO-8601 'since' string (UTC, with 'Z').
    """
    wm_path = os.path.join(
        RAW_TABLES_ROOT,
        schema,
        "canvas_watermark",
    )

    if not notebookutils.fs.exists(wm_path):
        # no watermark yet
        return None

    wm_df = (
        spark.read.format("delta").load(wm_path)
        .filter(F.col("table_name") == target_table)
        .orderBy(F.col("modified_at").desc())
        .limit(1)
    )

    rows = wm_df.collect()
    if not rows:
        return None

    last_refresh_str = rows[0]["last_refresh"]  # stored as string from poll_job_status
    if not last_refresh_str:
        return None

    # Ensure it's in 'YYYY-MM-DDTHH:MM:SSZ' format for Canvas "since"
    # If the stored value already has 'Z', keep it; otherwise add 'Z'.
    if last_refresh_str.endswith("Z"):
        return last_refresh_str
    else:
        # strip fractional seconds/timezone if present, keep first 19 chars
        core = last_refresh_str[:19]
        return core + "Z"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# if SYNCMODE == "INCREMENTAL":
#     since_val = get_latest_canvas_watermark_since(spark, LANDING_SCHEMA, TARGET_TABLE)
#     if since_val is None:
#         # fallback: treat as FULL or use now; choose behaviour
#         print(f"No watermark found for table '{TARGET_TABLE}', running as FULL.")
#         SYNCMODE = "FULL"
#         INCREMENTALSINCE = None
#     else:
#         INCREMENTALSINCE = since_val
#         print(f"Using incremental since = {INCREMENTALSINCE} for table {TARGET_TABLE}")
# else:
#     INCREMENTALSINCE = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔌 Cell 5 — Canvas CD2 API Helper Functions

# CELL ********************

# ============================================================================
# Cell 5 – Canvas CD2 API helper functions
# This cell contains low-level HTTP helpers that orchestrate the Canvas CD2
# data extraction lifecycle:
#   * dispatch_snapshot_query / dispatch_incremental_query – start jobs
#   * poll_job_status – wait for completion
#   * get_object_download_urls – obtain pre-signed S3 URLs
#   * download_jsonl_lines – download and parse gzip JSONL payloads
# These helpers are reused by the orchestration logic in later cells.
# ============================================================================
# ── Low-level HTTP helpers ─────────────────────────────────────────────────────


def _headers(token: str) -> Dict[str, str]:
    """Return standard CD2 request headers, including the auth token."""
    return {"x-instauth": token, "Content-Type": "application/json"}


def _json_safe(obj):
    """Recursively convert any non-JSON-serialisable values (e.g. datetime)
    into strings so they can be sent via ``requests.post(json=...)``.

    Canvas should normally return only JSON-native types, but this makes the
    client robust in case any library or future change introduces Python
    ``datetime`` (or similar) objects in the ``objects`` payload.
    """
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj
    if isinstance(obj, datetime):
        # Preserve full precision including timezone where present
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [ _json_safe(v) for v in obj ]
    # Fallback: stringify anything else
    return str(obj)


def dispatch_snapshot_query(base_url: str, token: str, table_name: str) -> str:
    """Dispatch a FULL snapshot (no 'since') query for *table_name*.

    POST {BASE_URL}/dap/query/canvas/table/{TABLE_NAME}/data
    Body: {"format": "jsonl"}

    Returns the job ID string issued by the DAP service.
    """
    url      = f"{base_url}/dap/query/canvas/table/{table_name}/data"
    payload  = {"format": CD2_FORMAT}
    response = requests.post(url, headers=_headers(token), json=payload, timeout=60)
    response.raise_for_status()
    job_id = response.json()["id"]
    print(f"    [FULL]        Job dispatched  → {job_id}")
    return job_id


def dispatch_incremental_query(
    base_url: str, token: str, table_name: str, since: str
) -> str:
    """Dispatch an INCREMENTAL (delta) query for *table_name* since *since*.

    POST {BASE_URL}/dap/query/canvas/table/{TABLE_NAME}/data
    Body: {"format": "jsonl", "since": since}

    Returns the job ID string.
    """
    url      = f"{base_url}/dap/query/canvas/table/{table_name}/data"
    payload  = {"format": CD2_FORMAT, "since": since}
    response = requests.post(url, headers=_headers(token), json=payload, timeout=60)
    response.raise_for_status()
    job_id = response.json()["id"]
    print(f"    [INCREMENTAL] Job dispatched  → {job_id}  (since={since})")
    return job_id


def poll_job_status(
    base_url: str,
    token: str,
    job_id: str,
    max_wait: int = MAX_WAIT_SEC,
    poll_interval: int = POLL_INTERVAL_SEC,
) -> Dict[str, object]:
    """Poll the CD2 job status until completion or timeout.

    Repeatedly calls GET /dap/job/{job_id} and returns a dict containing:
      * "objects"      – the objects collection
      * "completed_at" – the job-level "at" timestamp from the response

    when the job reaches status 'complete'. Raises TimeoutError or RuntimeError
    if the job does not finish successfully.
    """
    url     = f"{base_url}/dap/job/{job_id}"
    elapsed = 0

    while elapsed < max_wait:
        resp = requests.get(url, headers=_headers(token), timeout=30)
        resp.raise_for_status()
        job_data = resp.json()
        status   = job_data.get("status", "unknown")

        print(f"    Polling job {job_id}: status={status:12s}  elapsed={elapsed:>5}s")

        if status == "complete":
            
            objects = job_data.get("objects", [])
            
            completed_at = job_data.get("at")
            sync_at = job_data.get("at")
            sync_until = job_data.get("until")
            if completed_at is None:
               completed_at = job_data.get("until")
            print(f" sync_at{sync_at}  , sync_until..{sync_until}")   
            print(f"    ✅ Job complete — {len(objects)} object(s) to download")
            return {"objects": objects, "completed_at": completed_at}

        if status in ("failed", "error"):
            raise RuntimeError(f"CD2 job {job_id} failed: {job_data}")

        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(
        f"CD2 job {job_id} did not complete within {max_wait}s."
    )


def get_object_download_urls(
    base_url: str, token: str, objects: List[Dict]
) -> Dict[str, str]:
    """Resolve CD2 result objects to their pre-signed download URLs.

    Calls POST /dap/object/url with the list of object descriptors returned
    from poll_job_status, and normalises the response into a mapping of
    object_id → presigned_url for downstream download.

    Any non-JSON-native values (e.g. datetime) inside ``objects`` are first
    converted to strings to avoid ``Object of type datetime is not JSON
    serializable`` errors.
    """
    url = f"{base_url}/dap/object/url"

    safe_objects = _json_safe(objects)

    response = requests.post(url, headers=_headers(token), json=safe_objects, timeout=60)
    response.raise_for_status()
    payload  = response.json()

    print("    Raw /dap/object/url response:")
    # print(payload)

    # Normalise the response into a dict of id -> url, regardless of shape.
    result: Dict[str, str] = {}

    # The actual Canvas response shape is:
    #   { "urls": { "<object_id>": { "url": "https://..." }, ... } }
    # Handle that first.
    if isinstance(payload, dict) and "urls" in payload and isinstance(payload["urls"], dict):
        inner = payload["urls"]
        for obj_id, val in inner.items():
            if isinstance(val, dict) and "url" in val:
                result[obj_id] = val["url"]
            elif isinstance(val, str):
                result[obj_id] = val

    # Fallbacks for other possible shapes (kept for robustness / future-proofing)
    elif isinstance(payload, dict):
        for obj_id, url_value in payload.items():
            if isinstance(url_value, dict):
                if "url" in url_value:
                    result[obj_id] = url_value["url"]
                else:
                    for k, v in url_value.items():
                        if isinstance(v, str):
                            result[obj_id] = v
                            break
            else:
                result[obj_id] = url_value

    elif isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            obj_id = item.get("id") or item.get("objectId")
            url_val = item.get("url") or item.get("downloadUrl") or item.get("href")
            if obj_id and url_val:
                result[obj_id] = url_val

    # print(f"    Normalised URL map ({len(result)} entries):")
    # for oid, u in result.items():
    #     print(f"      {oid} -> {u}")

    return result


def download_jsonl_lines(presigned_url: str) -> List[Dict]:
    """Download and parse a gzip-compressed JSONL file from Canvas CD2.

    The CD2 service delivers result files as gzip-compressed newline-delimited
    JSON. Each line is a JSON object with the following top-level keys:
      * key  – primary key fields
      * value – full data payload
      * meta – action / timestamp metadata

    This helper transparently handles decompression and parsing, and skips any
    malformed JSON lines with a warning.
    """
    resp = requests.get(presigned_url, stream=True, timeout=300)
    resp.raise_for_status()
    raw = resp.content

    # Decompress if gzip magic bytes (\x1f\x8b) are detected
    if raw[:2] == b"\x1f\x8b":
        with gzip.GzipFile(fileobj=io.BytesIO(raw)) as gz_fh:
            raw = gz_fh.read()

    records: List[Dict] = []
    for line_no, line in enumerate(raw.decode("utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            print(f"    ⚠️  Skipping malformed JSON at line {line_no}: {exc}")

    return records


print("✅  CD2 API helper functions defined.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🔧 Cell 6 — Delta Lake Landing Table Helpers

# CELL ********************

# ── Target schema ──────────────────────────────────────────────────────────────
LANDING_SCHEMA_DDL = StructType(
    [
        StructField("hashkey",     StringType(),    nullable=False),
        StructField("rowdata",     StringType(),    nullable=True),
        StructField("created_at",  TimestampType(), nullable=False),
        StructField("modified_at", TimestampType(), nullable=False),
    ]
)

# Watermark table schema: one row per (MasterRunId, table_name)
CANVAS_WATERMARK_SCHEMA_DDL = StructType(
    [
        StructField("masterRunId",  StringType(),    nullable=False),
        StructField("pipelineName",  StringType(),    nullable=False),
        StructField("table_name",    StringType(),    nullable=False),
        StructField("last_refresh",  StringType(),    nullable=True),   # "at" from poll_job_status
        StructField("created_at",    TimestampType(), nullable=False),
        StructField("modified_at",   TimestampType(), nullable=False),
    ]
)


def _landing_table_name(table_name: str) -> str:
    """Canonical landing table name: canv_{tablename}."""
    return f"canv_{table_name}"


def _full_table_ref(schema: str, table_name: str) -> str:
    return f"{schema}.{_landing_table_name(table_name)}"


def ensure_landing_table(spark: SparkSession, schema: str, table_name: str) -> None:
    """Compatibility no-op.

    In the original version this created a Delta table in the Lakehouse
    catalog using `CREATE TABLE`. That requires a default Lakehouse context
    and is failing with:

        "No default context found, please attach a lakehouse ..."

    The pipeline now writes Delta data **directly to paths** under
    `RAW_TABLES_ROOT`, so a catalog table is not required. This function is
    kept for API compatibility and just logs what it would have created.
    """
    full_ref = _full_table_ref(schema, table_name)
    print(f"    [info] Skipping CREATE TABLE for {full_ref}; using path-based Delta writes instead.")


def write_canvas_watermark(
    spark: SparkSession,
    master_run_id: str,
    pipeline_name:str,
    table_name: str,
    last_refresh: Optional[str],
    schema: str,
) -> None:
    """Append a watermark row for the given table into LANDING_SCHEMA.canvas_watermark.

    Columns:
      * MasterRunId  – from MASTER_RUN_ID
      * table_name   – from TARGET_TABLE
      * last_refresh – "at" value from poll_job_status (job_completed_at)
      * created_at / modified_at – current UTC timestamp

    Implemented as a simple path-based Delta append under RAW_TABLES_ROOT.
    """
    now_ts_py = datetime.now(tz=timezone.utc)

    row = [(master_run_id,pipeline_name, table_name, last_refresh, now_ts_py, now_ts_py)]
    wm_df = spark.createDataFrame(row, schema=CANVAS_WATERMARK_SCHEMA_DDL)

    target_path = os.path.join(
        RAW_TABLES_ROOT,
        schema,
        "canvas_watermark",
    )

    (
        wm_df.write
        .format("delta")
        .mode("append")
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    print(
        f"    💧 Watermark upserted → path={target_path} "
        f"(MasterRunId={master_run_id}, table_name={table_name}, last_refresh={last_refresh})"
    )


# ----------------------------------------------------------------------------
# Helper that converts raw JSONL records directly to landing DataFrame in Python
# ----------------------------------------------------------------------------


def build_landing_df_from_records(
    spark: SparkSession,
    records: List[Dict],
    table_name: str,
) -> Optional[DataFrame]:
    """Create landing DataFrame (hashkey, rowdata, created_at, modified_at)
    directly from parsed JSONL records using pure Python.

    This avoids Spark's schema inference on heterogeneous JSON (e.g. the same
    logical field sometimes being a struct and sometimes a scalar), which was
    causing errors like:
        [CANNOT_MERGE_TYPE] Can not merge type `StringType` and `StructType`.

    Hashkey strategy: SHA-256 over the JSON-serialised "key" object
    (sorted keys for determinism).

    IMPORTANT: `rowdata` now contains the **full CD2 envelope** so that
    `key`, `value` and `meta` are all preserved in both the Delta table and
    the landing JSON files:

        rowdata = {"key": <key>, "value": <value>, "meta": <meta>}

    IMPORTANT for large tables (e.g. assignments, rubrics): we *partition* the data
    when creating the DataFrame so that no single Spark task has to ship an
    excessively large serialized payload from the driver to executors. This
    prevents errors like:
        Serialized task ... exceeds spark.rpc.message.maxSize
    """
    import builtins  # ensure we use Python's built-in min/max even if shadowed elsewhere

    if not records:
        return None

    now_ts_py = datetime.now(tz=timezone.utc)
    rows: List[Tuple[str, str, datetime, datetime]] = []

    for rec in records:
        key_obj = rec.get("key") or {}
        val_obj = rec.get("value") or {}
        meta_obj = rec.get("meta") or {}

        # Stable string for hashing: sorted keys from the *key* object
        key_json = json.dumps(key_obj, sort_keys=True, default=str)
        hashkey = hashlib.sha256(key_json.encode("utf-8")).hexdigest()

        # Persist the full envelope so key/meta are not lost
        payload = {"key": key_obj, "value": val_obj, "meta": meta_obj}
        rowdata = json.dumps(payload, default=str)

        # Match LANDING_SCHEMA_DDL (4 fields)
        rows.append((hashkey, rowdata, now_ts_py, now_ts_py))

    total_rows = len(rows)

    # Choose a small number of rows per partition so that each partition stays
    # well below spark.rpc.message.maxSize, even when `rowdata` is large.
    # For example, at 10 KB per row, 5,000 rows ≈ 50 MB per partition.
    target_rows_per_part = 5000
    num_slices = builtins.max(8, builtins.min(400, total_rows // target_rows_per_part + 1))

    rdd = spark.sparkContext.parallelize(rows, numSlices=num_slices)
    landing_df = spark.createDataFrame(rdd, schema=LANDING_SCHEMA_DDL)

    print(
        f"    Landing rows prepared (Python path): {total_rows:,} "
        f"across {num_slices} partition(s)"
    )
    return landing_df


# ----------------------------------------------------------------------------
# Legacy path: JSONL -> flattened Spark DF -> landing
# (kept for potential incremental logic that needs meta.action & value.* cols)
# ----------------------------------------------------------------------------


def jsonl_records_to_spark_df(
    spark: SparkSession,
    records: List[Dict],
    table_name: str,
) -> Optional[DataFrame]:
    """Convert a flat list of parsed JSONL records into a Spark DataFrame.

    Canvas CD2 JSONL records have the structure:
        {
            "key":  { ...primary key fields... },
            "value": { ...data payload fields... },
            "meta": { "action": "upsert" | "delete", "ts": "<ISO-8601>" }
        }

    The function flattens key, value, and meta top-level keys into separate
    columns prefixed with 'key.', 'value.', and 'meta.' respectively,
    preserving the same column-naming convention the previous Parquet reader used.
    This ensures build_landing_df works unchanged regardless of output format.

    To avoid Arrow/PySpark type merge issues when the same logical column has
    mixed JSON types across rows (e.g. true vs 1.0), we coerce all values to
    strings on ingestion. Landing logic relies only on hashkey + JSON payload,
    so this is safe and schema-agnostic.
    """
    import pandas as pd

    if not records:
        return None

    flattened_rows = []
    for rec in records:
        flat: Dict = {}
        # Flatten key.* fields
        for k, v in (rec.get("key") or {}).items():
            flat[f"key.{k}"] = None if v is None else str(v)
        # Flatten value.* fields
        for k, v in (rec.get("value") or {}).items():
            flat[f"value.{k}"] = None if v is None else str(v)
        # Flatten meta.* fields
        for k, v in (rec.get("meta") or {}).items():
            flat[f"meta.{k}"] = None if v is None else str(v)
        # Fallback: include any unexpected top-level keys as-is (stringified)
        for k in rec:
            if k not in ("key", "value", "meta"):
                v = rec[k]
                flat[k] = None if v is None else str(v)
        flattened_rows.append(flat)

    pdf = pd.DataFrame(flattened_rows)
    print(f"    Raw rows parsed from JSONL (flattened): {len(pdf):,}")

    # Coerce all columns to string to prevent Boolean/Double merge errors
    pdf = pdf.astype("string")

    # Use Spark's pandas integration but avoid Arrow type conflicts
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    return spark.createDataFrame(pdf)



def build_landing_df(
    spark: SparkSession,
    raw_df: DataFrame,
    table_name: str,
    now_ts,
) -> DataFrame:
    """Transform the raw CD2 JSONL DataFrame (after flattening) into the
    4-column landing schema:  hashkey, rowdata, created_at, modified_at

    Hashkey strategy (in priority order):
      1. Columns prefixed with 'key.'  (CD2 flattened key fields)
      2. Column literally named 'key'
      3. First column of the raw schema (fallback)

    rowdata is the JSON serialisation of all 'value.*' columns.
    If no 'value.*' columns exist, all columns are serialised.
    """
    all_cols = raw_df.columns

    # ── Identify key columns ────────────────────────────────────────────────────
    key_cols    = [c for c in all_cols if c.startswith("key.")]
    value_cols  = [c for c in all_cols if c.startswith("value.")]

    if not key_cols and "key" in all_cols:
        key_cols = ["key"]

    if not key_cols:
        # Fallback: treat first column as identity
        key_cols = all_cols[:1]
        print(f"    ⚠️  No 'key.*' columns found for {table_name}; hashing '{key_cols}' as key.")

    # ── Build rowdata: serialise all columns to JSON ────────────────────────────
    rowdata_source_cols = value_cols if value_cols else all_cols

    df = raw_df.withColumn(
        "hashkey",
        F.sha2(
            F.concat_ws("|", *[F.col(c).cast(StringType()) for c in key_cols]),
            256,
        ),
    ).withColumn(
        "rowdata",
        F.to_json(F.struct(*[F.col(c) for c in rowdata_source_cols])),
    ).withColumn(
        "created_at",  now_ts
    ).withColumn(
        "modified_at", now_ts
    )

    return df.select("hashkey", "rowdata", "created_at", "modified_at")


print("✅  Landing table helpers defined.")


# ── File-save helpers ─────────────────────────────────────────────────────────


def _file_run_dir(
    base: str, zone: str, table_name: str, sync_mode: str, run_ts: str
) -> str:
    """Build and create the per-run directory path.
    Structure: {base}/{zone}/{table_name}/{sync_mode}/run_{run_ts}/
    """
    path = os.path.join(base, zone, table_name, sync_mode, f"run_{run_ts}")
    return path


def save_raw_records_to_file(
    records: List[Dict],
    table_name: str,
    sync_mode: str,
    run_ts: str,
    base: str = RAW_FILES_ROOT,
) -> str:
    """Persist the raw CD2 JSONL records (as downloaded and parsed) to a
    gzip-compressed JSONL file in the Lakehouse Files raw zone.

    Each line in the output file is the original CD2 JSON object:
        {"key": {...}, "value": {...}, "meta": {...}}

    File path:
        raw/canvas/canvas_raw/{table_name}/{sync_mode}/run_{run_ts}/
            canv_{table_name}_raw.jsonl.gz

    Path is in OneLake (`abfss://...`), so use mssparkutils.fs to create
    folders and write the file.
    """
    from notebookutils import mssparkutils as _mu

    run_dir   = _file_run_dir(base, "canvas_raw", table_name, sync_mode, run_ts)
    file_path = os.path.join(run_dir, f"canv_{table_name}_raw.jsonl.gz")

    # Ensure directory exists
    _mu.fs.mkdirs(run_dir)

    # Write gzip JSONL to a temporary local file, then copy to OneLake
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        local_path = os.path.join(tmpdir, f"canv_{table_name}_raw.jsonl.gz")
        with gzip.open(local_path, "wt", encoding="utf-8") as gz_out:
            for rec in records:
                gz_out.write(json.dumps(rec, default=str) + "\n")
        _mu.fs.cp(local_path, file_path)

    print(f"    📄 Raw file saved  → {file_path}  ({len(records):,} records)")
    return file_path


def save_landing_df_to_file(
    spark: SparkSession,
    landing_df: DataFrame,
    table_name: str,
    sync_mode: str,
    run_ts: str,
    base: str = RAW_FILES_ROOT,
) -> str:
    """Write the 4-column landing DataFrame (hashkey, rowdata, created_at,
    modified_at) to JSONL files in the Lakehouse Files landing zone.

    We write directly to OneLake using a path under RAW_FILES_ROOT and let
    Spark manage the part-files. No local `os` calls are used on abfss paths.

    Directory path:
        raw/canvas/canvas_landing/{table_name}/{sync_mode}/run_{run_ts}/
            part-*.json

    Returns the directory path containing the JSONL part-files.
    """
    from notebookutils import mssparkutils as _mu

    run_dir = _file_run_dir(base, "canvas", table_name, sync_mode, run_ts)

    # Ensure parent directory exists
    _mu.fs.mkdirs(run_dir)

    writer = (
        landing_df
        # keep multiple partitions to avoid huge tasks
        .write
        .mode("append")
        .option("compression", "none")
        .option("maxRecordsPerFile", 500000)
    )

    writer.json(run_dir)

    print(f"    📄 Landing files saved under {run_dir}")
    return run_dir


print("✅  File-save helpers defined.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# MARKDOWN ********************

# ## 🔄 Cell 7 — Full Sync (Snapshot) Writer

# CELL ********************

def write_full_sync(
    spark: SparkSession,
    schema: str,
    table_name: str,
    landing_df: DataFrame,
) -> int:
    """FULL SYNC — overwrite the landing *Delta data* completely using a
    **path-based** write.

    NOTE about large tasks and spark.rpc.message.maxSize
    ----------------------------------------------------
    We avoid operations that create a single huge partition or a giant
    serialized task on the driver (e.g. coalesce(1) + count() on a massive
    DataFrame). Instead, we:
      * write the DataFrame as-is (multiple partitions), and
      * if needed, derive counts from the written Delta data.
    """

    target_path = os.path.join(
        RAW_TABLES_ROOT,
        schema,
        _landing_table_name(table_name),
    )

    # Write WITHOUT pre-counting to avoid large serialized tasks
    (
        landing_df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", True)
        .save(target_path)
    )

    # Post-write count from Delta files (can still be expensive but avoids
    # serializing the pre-write plan from the driver). For very large tables
    # you can comment this out or replace with an approximate count.
    written_df = spark.read.format("delta").load(target_path)
    row_count = written_df.count()

    print(
        f"    ✅ FULL  → path={target_path}: {row_count:,} rows written (OVERWRITE)"
    )
    return row_count


print("✅  Full-sync writer defined.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ♻️ Cell 8 — Incremental Sync (MERGE / Upsert) Writer

# CELL ********************

def write_incremental_sync(
    spark: SparkSession,
    schema: str,
    table_name: str,
    landing_df: DataFrame,
    raw_df: DataFrame,
) -> int:
    """
    INCREMENTAL SYNC — MERGE new/changed rows into the existing landing table.

    Handling of CD2 meta.action values:
      - 'upsert': INSERT if hashkey absent, UPDATE if present (preserves created_at)
      - 'delete': physically DELETE the matching row from the landing table

    If meta.action is not present in the schema (older API versions), all rows
    are treated as upserts.

    Returns the number of rows in the incoming batch.
    """
    full_ref  = _full_table_ref(schema, table_name)
    row_count = landing_df.count()

    # ── Split upserts vs deletes ────────────────────────────────────────────────
    has_meta_action = "meta.action" in raw_df.columns

    if has_meta_action:
        upsert_df = landing_df.join(
            raw_df.filter(F.col("`meta.action`") != "delete")
                  .select(F.sha2(F.col("`key`").cast(StringType()), 256).alias("hk")),
            landing_df["hashkey"] == F.col("hk"), "left_semi"
        ).union(
            landing_df.join(
                raw_df.filter(F.col("`meta.action`") != "delete")
                      .select(F.sha2(F.col("`key`").cast(StringType()), 256).alias("hk")),
                landing_df["hashkey"] == F.col("hk"), "left_anti"
            )
        )

        # hashkeys that should be deleted
        delete_hashkeys = (
            landing_df.join(
                raw_df.filter(F.col("`meta.action`") == "delete")
                      .select(F.sha2(F.col("`key`").cast(StringType()), 256).alias("hk")),
                landing_df["hashkey"] == F.col("hk"), "left_semi"
            ).select("hashkey")
        )
    else:
        # No meta action column — treat everything as upsert
        upsert_df       = landing_df
        delete_hashkeys = None

    # ── MERGE upserts ───────────────────────────────────────────────────────────
    upsert_df.createOrReplaceTempView(f"_cd2_updates_{table_name}")

    spark.sql(f"""
        MERGE INTO {full_ref} AS target
        USING _cd2_updates_{table_name} AS source
        ON target.hashkey = source.hashkey
        WHEN MATCHED THEN
            UPDATE SET
                target.rowdata     = source.rowdata,
                target.modified_at = source.modified_at
        WHEN NOT MATCHED THEN
            INSERT (hashkey, rowdata, created_at, modified_at)
            VALUES (source.hashkey, source.rowdata, source.created_at, source.modified_at)
    """)

    # ── DELETE removed records ──────────────────────────────────────────────────
    if delete_hashkeys and delete_hashkeys.count() > 0:
        delete_hashkeys.createOrReplaceTempView(f"_cd2_deletes_{table_name}")
        deleted_count = spark.sql(f"""
            DELETE FROM {full_ref}
            WHERE hashkey IN (
                SELECT hashkey FROM _cd2_deletes_{table_name}
            )
        """).first()[0]
        print(f"    🗑️  Deleted {deleted_count:,} removed records.")

    print(
        f"    ✅ INCR  → {full_ref}: {row_count:,} rows merged (UPSERT + DELETE)"
    )
    return row_count


print("✅  Incremental-sync writer defined.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🚀 Cell 9 — Per-Table Orchestration Function

# CELL ********************

def process_canvas_table(
    spark: SparkSession,
    base_url: str,
    token: str,
    schema: str,
    table_name: str,
    sync_mode: str,
    since: Optional[str] = None,
) -> Dict:
    """
    End-to-end pipeline for a single Canvas table.

    Steps:
        1. Dispatch snapshot or incremental query job (format=jsonl)
        2. Poll job status until 'complete'
        3. Retrieve pre-signed download URLs for result objects
        4. Download gzip-compressed JSONL files & parse into record dicts
        4b. [Optional] Save raw records to Files/canvas_raw/ as .jsonl.gz
        5. Build landing DataFrame directly from records (hashkey/rowdata/timestamps)
        6. Write to Delta Lake via OVERWRITE (FULL) or MERGE (INCREMENTAL)
        6b. [Optional] Save landing DataFrame to Files/canvas_landing/ as .jsonl

    Returns a status dict.
    """
    landing_tbl = _landing_table_name(table_name)
    full_ref    = _full_table_ref(schema, table_name)
    run_ts      = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")

    result = {
        "table": table_name,
        "landing_table": landing_tbl,
        "sync_mode": sync_mode,
        "status": "PENDING",
        "rows": 0,
        "raw_file": None,      # path to raw .jsonl.gz in Files/canvas_raw/
        "landing_file": None,  # path to landing .jsonl in Files/canvas_landing/
        "error": None,
        "started_at": datetime.now(tz=timezone.utc).isoformat(),
        "finished_at": None,
        "job_completed_at": None,
    }

    try:
        print(f"\n{'─'*70}")
        print(f"  TABLE : {table_name}  [{sync_mode}]")
        print(f"  TARGET: {full_ref}")
        print(f"{'─'*70}")

        # ── Step 1: Dispatch job ────────────────────────────────────────────────
        if sync_mode == "FULL":
            job_id = dispatch_snapshot_query(base_url, token, table_name)
        else:
            if not since:
                raise ValueError("'since' timestamp is required for INCREMENTAL sync.")
            job_id = dispatch_incremental_query(base_url, token, table_name, since)

        # ── Step 2: Poll until complete ─────────────────────────────────────────
        poll_result = poll_job_status(base_url, token, job_id)
        objects = poll_result.get("objects", [])
        job_completed_at = poll_result.get("completed_at")
        result["job_completed_at"] = job_completed_at

        if not objects:
            print(f" No new objects returned for {table_name}. Skipping write.")
            result["status"]      = "NO_DATA"
            result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()
            return result

        # ── Step 3: Get download URLs ───────────────────────────────────────────
        url_map = get_object_download_urls(base_url, token, objects)
        
        for url in url_map.values():
            print(url)
        # ── Step 4: Download & parse JSONL files ────────────────────────────────
        all_records: List[Dict] = []
        for obj_id, dl_url in url_map.items():
            # print(f"    Downloading & parsing JSONL object {obj_id[:12]}...")
            chunk = download_jsonl_lines(dl_url)
            # print(f"      → {len(chunk):,} records in this object")
            all_records.extend(chunk)

        # print(f"    Total records across all objects: {len(all_records):,}")

        # ── Step 4b: Save raw records to Lakehouse Files ────────────────────────
        if SAVE_RAW_FILE and all_records:
            result["raw_file"] = save_raw_records_to_file(
                all_records, table_name, sync_mode, run_ts, FILE_OUTPUT_BASE
            )

        if not all_records:
            result["status"]      = "NO_DATA"
            result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()
            return result

        # ── Step 5: Build landing DataFrame directly in Python ─────────────────
        landing_df = build_landing_df_from_records(spark, all_records, table_name)
        if landing_df is None:
            result["status"]      = "NO_DATA"
            result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()
            return result

        # ── Step 6: Ensure target table exists ─────────────────────────────────
        ensure_landing_table(spark, schema, table_name)

        # ── Step 7: Write to Delta Lake ─────────────────────────────────────────
        if sync_mode == "FULL":
            rows = write_full_sync(spark, schema, table_name, landing_df)
        else:
            # For INCREMENTAL we would need meta.action & key in Spark.
            # For now treat as append-only upsert-equivalent by overwriting.
            rows = write_full_sync(spark, schema, table_name, landing_df)

        # ── Step 7b: Save landing data to Lakehouse Files ──────────────────────
        if SAVE_LANDING_FILE:
            result["landing_file"] = save_landing_df_to_file(
                spark, landing_df, table_name, sync_mode, run_ts, FILE_OUTPUT_BASE
            )

        result["status"]      = "SUCCESS"
        result["rows"]        = rows
        result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()

    except Exception as exc:
        result["status"]      = "FAILED"
        result["error"]       = str(exc)
        result["finished_at"] = datetime.now(tz=timezone.utc).isoformat()
        print(f"    ❌ ERROR processing {table_name}: {exc}")

    return result


print("✅  Per-table orchestration function defined.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 🌐 Cell 10 — Create Lakehouse Schema

# CELL ********************

# ============================================================================
# Cell 10 – Optional Lakehouse schema creation
# This cell attempts to create the logical schema (database) used to expose the
# Delta landing tables via the Lakehouse catalog. The ingestion pipeline itself
# relies on path-based Delta writes and does not require the schema to exist;
# therefore any missing Lakehouse context is treated as a warning only.
# ============================================================================
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Try to ensure the landing schema (database) exists *if* a Lakehouse catalog
# context is available. If not, log a warning but do not fail the pipeline —
# the ingestion now uses path-based Delta writes, which do not require a
# catalog schema.
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LANDING_SCHEMA}")
    print(f"✅  Schema '{LANDING_SCHEMA}' is ready (Lakehouse catalog).")

    # Show existing landing tables (if any)
    spark.sql(f"SHOW TABLES IN {LANDING_SCHEMA}").show(truncate=False)
except Exception as e:
    msg = str(e)
    if "No default context found" in msg:
        print(
            f"⚠️  Could not create/show schema '{LANDING_SCHEMA}' in the Lakehouse catalog: {msg}\n"
            "    → This notebook has no default Lakehouse attached. Ingestion will still "
            "work using path-based Delta writes under RAW_TABLES_ROOT."
        )
    else:
        raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ▶️ Cell 11 — Run All Tables

# CELL ********************

# ============================================================================
# Cell 11 – Pipeline driver for Canvas CD2 ingestion
# This is the main orchestration cell. It:
#   * logs high-level pipeline metadata (mode, tables, schema)
#   * iterates over the configured CANVAS_TABLES list
#   * invokes process_canvas_table for each source table
#   * records per-table results for downstream reporting
# ============================================================================
# pipeline_start = datetime.now(tz=timezone.utc)
# print(f"\n{'═'*70}")
# print(f"  CANVAS CD2 INGESTION PIPELINE")
# print(f"  Mode      : {SYNC_MODE}")
# print(f"  Tables    : {len(CANVAS_TABLES)}")
# print(f"  Schema    : {LANDING_SCHEMA}")
# print(f"  Started   : {pipeline_start.isoformat()}")
# if SYNC_MODE == "INCREMENTAL":
#     print(f"  Since     : {INCREMENTAL_SINCE}")
# print(f"{'═'*70}")

# pipeline_results: List[Dict] = []

# for tbl in CANVAS_TABLES:
#     res = process_canvas_table(
#         spark        = spark,
#         base_url     = BASE_URL,
#         token        = AUTH_TOKEN,
#         schema       = LANDING_SCHEMA,
#         table_name   = tbl,
#         sync_mode    = SYNC_MODE,
#         since        = INCREMENTAL_SINCE if SYNC_MODE == "INCREMENTAL" else None,
#     )
#     print(res)
#     pipeline_results.append(res)

# pipeline_end = datetime.now(tz=timezone.utc)
# elapsed_min  = (pipeline_end - pipeline_start).total_seconds() / 60

# print(f"\n{'═'*70}")
# print(f"  PIPELINE COMPLETE   (elapsed: {elapsed_min:.1f} min)")
# print(f"{'═'*70}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

start_time = datetime.now()
rows_read = 0
rows_written = 0
rows_rejected = 0
try:
    # ==========================================
    # BUSINESS LOGIC (replace with real work)
    # ==========================================

    print("Executing business logic...")

    tbl = CANVAS_TABLES[0]
    result = process_canvas_table(
        spark        = spark,
        base_url     = BASE_URL,
        token        = AUTH_TOKEN,
        schema       = LANDING_SCHEMA,
        table_name   = tbl,
        sync_mode    = SYNC_MODE,
        since        = INCREMENTAL_SINCE if SYNC_MODE == "INCREMENTAL" else None,
    )
    print(result)

    # Write/update canvas watermark row using poll_job_status "at" field
    job_completed_at = result.get("job_completed_at")
    write_canvas_watermark(
        spark=spark,
        master_run_id=_safe_str(MASTER_RUN_ID),
        pipeline_name = _safe_str(PIPELINE_NAME),
        table_name=_safe_str(TARGET_TABLE),
        last_refresh=_safe_str(job_completed_at) if job_completed_at is not None else None,
        schema=LANDING_SCHEMA,
    )

    print("business logic completed...")

    rows_read = result.get("rows", 0)
    rows_written = rows_read
    rows_rejected = 0

    # updated audit log to SUCCESS
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "success", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
        start_time, end_time, "", "", _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(TRIGGERED_BY),
        rerun_count=0
    )
    print("SUCCESS logged")

except Exception as e:
    # updated audit log to FAILURE
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    insert_pipeline_monitoring_record( 
        _safe_str(MASTER_RUN_ID), _safe_str(RUN_ID), _safe_str(PIPELINE_NAME), _safe_str(NOTEBOOK_NAME), _safe_str(DOMAIN), _safe_str(SOURCE_NAME), _safe_str(ACTIVITY_NAME),
        "failed", _safe_str(LEVEL), _safe_str(LAYER), _safe_str(SOURCE_TABLE), _safe_str(TARGET_TABLE), rows_read, rows_written, rows_rejected,
        start_time, end_time, str(e), str(e), _safe_str(ENVIRONMENT), _safe_str(WORKSPACE_NAME), _safe_str(WORKSPACE_ID), _safe_str(TRIGGERED_BY),
        rerun_count=0
    )
    print("FAILED logged")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##  Cell 13 — Sample Row Inspection (Optional)

# CELL ********************

# ============================================================================
# Cell 14 – Optional sample data inspection
# Finds the first successfully ingested Canvas table and displays a small
# sample of rows (hashkey and a truncated rowdata preview) from the landing
# Delta *path* (not catalog table). This is useful for ad-hoc verification
# and troubleshooting when no Lakehouse catalog table exists.
# ============================================================================

# Preview a few rows from the first successfully loaded table
# sample_shown = False

# for res in pipeline_results:
#     if res["status"] == "SUCCESS":
#         table_name = res["table"]
#         delta_path = os.path.join(
#             RAW_TABLES_ROOT,
#             LANDING_SCHEMA,
#             _landing_table_name(table_name),
#         )
#         print(f"\nSample rows from Delta path: {delta_path}")

#         df_preview = (
#             spark.read
#             .format("delta")
#             .load(delta_path)
#             .select(
#                 "hashkey",
#                 F.substring("rowdata", 1, 120).alias("rowdata_preview"),
#                 "created_at",
#                 "modified_at",
#             )
#             .limit(3)
#         )

#         df_preview.show(truncate=False)
#         sample_shown = True
#         break

# if not sample_shown:
#     print("No successful tables found in pipeline_results to preview.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
