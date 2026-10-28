# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ab125293-c8a6-4b75-9261-133eef78de17",
# META       "default_lakehouse_name": "lh_fn_bronze",
# META       "default_lakehouse_workspace_id": "bedb4898-64aa-4f9d-b9de-e0c4b634ee27",
# META       "known_lakehouses": [
# META         {
# META           "id": "ab125293-c8a6-4b75-9261-133eef78de17"
# META         },
# META         {
# META           "id": "5cb1dc45-6dbe-470f-8ef9-0291324328c2"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

import base64
import html
import json
import math
import re
import uuid
import requests
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time as dt_time
from decimal import Decimal

from lxml import etree
from requests import Session
from zeep import Client
from zeep.helpers import serialize_object
from zeep.plugins import HistoryPlugin
from zeep.transports import Transport
from typing import List, Optional

# =====================================================
# utils_workday_api notebook (production-ready + dual mode)
# =====================================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled","true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_variable(lib_name: str, var_name: str):
    """
    Retrieve a variable from a Fabric Variable Library.

    Parameters
    ----------
    lib_name : str
        Name of the variable library to load (as defined in Fabric).
    var_name : str
        Name of the variable (attribute) to retrieve from that library.

    Returns
    -------
    Any
        The value of the requested variable.

    Raises
    ------
    AttributeError
        If the variable does not exist in the specified library.
        The exception includes a list of available variables as context.
    """
    # Load the variable library object by its name
    vl = notebookutils.variableLibrary.getLibrary(lib_name)

    try:
        # Try to get the attribute (variable) from the library object
        return getattr(vl, var_name)
    except AttributeError:
        # If the attribute does not exist, build a list of available variables
        # Skip internal attributes (starting with "_") and the method "getVariable"
        vars = [
            name
            for name in dir(vl)
            if not name.startswith("_") and name != "getVariable"
        ]

        # Raise a more informative AttributeError including the list of valid variable names
        raise AttributeError(
            f"{var_name} not available in {lib_name} variable library",
            vars
        )


def get_workspace_id(var_lib_name: str, workspace_name: str, layer_name: str):
    """
    Build the variable name for a workspace ID and fetch it from a Fabric Variable Library.

    Example expected variable name in the library:
        "<workspace_name>_<layer_name>_workspace_id"
        e.g. "sales_bronze_workspace_id"

    Parameters
    ----------
    var_lib_name : str
        Name of the variable library where workspace IDs are stored.
    workspace_name : str
        Logical name of the workspace (e.g. "sales").
    layer_name : str
        Data layer name (e.g. "bronze", "silver", "gold").

    Returns
    -------
    Any
        The value stored under the constructed variable name.
    """
    # Construct the variable name following the naming convention
    var_name = f"{workspace_name}_{layer_name}_workspace_id"
    # Retrieve the value from the variable library using the helper function
    value = get_variable(var_lib_name, var_name)
    return value


def get_lakehouse_id(var_lib_name: str, lakehouse_name: str):
    """
    Build the variable name for a lakehouse ID and fetch it from a Fabric Variable Library.

    Example expected variable name in the library:
        "<lakehouse_name>_id"
        e.g. "lh_platform_monitoring_id"

    Parameters
    ----------
    var_lib_name : str
        Name of the variable library where lakehouse IDs are stored.
    lakehouse_name : str
        Logical name of the lakehouse.

    Returns
    -------
    Any
        The value stored under the constructed variable name.
    """
    # Construct the variable name following the naming convention
    var_name = f"{lakehouse_name}_id"
    # Retrieve the value from the variable library using the helper function
    value = get_variable(var_lib_name, var_name)
    return value


def get_lh_root_path(workspace_id: str, lakehouse_id: str):
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"

def abfss_path_builder(workspace_id: str, lakehouse_id: str, schema_name: str, table_name: str):
    """
    Construct an ABFSS path for a table stored in a Fabric Lakehouse.

    The resulting path follows the pattern:
        abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{schema_name}/{table_name}

    This can be used, for example, with Spark read/write operations.

    Parameters
    ----------
    workspace_id : str
        Workspace GUID where the lakehouse resides.
    lakehouse_id : str
        Lakehouse GUID (artifact ID).
    schema_name : str
        Schema name under Tables (e.g. 'dbo' or 'audit').
    table_name : str
        Table name.

    Returns
    -------
    str
        Fully qualified ABFSS path to the target table.
    """
    # Base ABFSS endpoint for the workspace in OneLake
    root_path = get_lh_root_path(workspace_id, lakehouse_id)
    # Append lakehouse and table-specific path
    return f"{root_path}/Tables/{schema_name}/{table_name}"

    
def add_uuid_column(df: DataFrame, col_name: str = "id") -> DataFrame:
    """
    Return a new DataFrame with a UUID column added.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    col_name : str, default "id"
        Name of the UUID column to add.

    Returns
    -------
    DataFrame
        DataFrame with the new UUID column.
    """
    return df.withColumn(col_name, F.expr("uuid()"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## Fetching Config object values
def get_param_value(df, source_name,key_name):
    row = (df.filter(df["source"] == source_name).select("param_values").first())
    if not row:
        return None
    param_values = row["param_values"]

    if isinstance(param_values,str):
        param_dict = json.loads(param_values)

    else:
        param_dict = param_values
    return param_dict.get(key_name)

##  Fetching Keyvault values
def get_secret(name: str, kv_url: str, use_key_vault: bool = True, fallback: str = "") -> str:
    if use_key_vault:
        return mssparkutils.credentials.getSecret(kv_url, name)
    return os.environ.get(name, fallback)

def fetch_access_token(TOKEN_URL,GRANT_TYPE,WD_REFRESH_TOKEN,WD_CLIENT_ID,WD_CLIENT_SECRET):
    token_url = TOKEN_URL
    payload = {
            "grant_type":GRANT_TYPE,
            "refresh_token": WD_REFRESH_TOKEN,
            "client_id": WD_CLIENT_ID,
            "client_secret": WD_CLIENT_SECRET
            }
    headers = {"Content-Type" : "text/xml"}
    response = requests.post(token_url,data=payload,headers = headers)
    access_token = response.json().get("access_token")
    return access_token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================
# RECURSIVE KEY FINDER
# =====================================================
def find_key(data, target_key):
    if isinstance(data, dict):
        for key, value in data.items():
            if key == target_key:
                return value
            result = find_key(value, target_key)
            if result is not None:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_key(item, target_key)
            if result is not None:
                return result
    return None
 
# =====================================================
# JSON-SAFE COERCION
# =====================================================
 
def _is_typed_id_group(elem):
    """
    Returns True if ALL child elements share the same local tag name AND
    every child has a 'type' attribute — i.e. the Workday typed-ID pattern:
<wd:ID wd:type="WID">...</wd:ID>
<wd:ID wd:type="ISO_3166-1_Alpha-2_Code">...</wd:ID>
 
    This is the pattern that previously collapsed into a value-only list,
    losing the type keys entirely:
        "ID": ["bc33aa...", "US", "USA", ...]   ← WRONG (before fix)
 
    After fix it becomes a keyed dict:
        "WID": "bc33aa...",
        "ISO_3166-1_Alpha-2_Code": "US",        ← CORRECT (after fix)
        ...
    """
    children = list(elem)
    if not children:
        return False
    first_tag = etree.QName(children[0].tag).localname
    return all(
        etree.QName(c.tag).localname == first_tag and
        any(etree.QName(a).localname == "type" for a in c.attrib)
        for c in children
    )
 
 
def _xml_element_to_safe(elem):
    """
    Convert lxml element to JSON-safe dict using localnames.
 
    FIX: When a parent element contains children that ALL share the same
    local tag (e.g. 'ID') and each child carries a 'type' attribute, those
    children are converted to a keyed dict {type_value: text_value} instead
    of a plain list [value, value, ...].  This preserves:
        WID                      → "bc33aa3152ec42d4995f4791a106ed09"
        ISO_3166-1_Alpha-2_Code  → "US"
        ISO_3166-1_Alpha-3_Code  → "USA"
        OPM_Country_Code         → "US"
        ISO_3166-1_Numeric-3_Code → "840"
    rather than collapsing everything into an anonymous list.
 
    This fix is generic — it applies to ANY Workday typed-ID group
    (Worker IDs, Position IDs, Cost Centre IDs, etc.), not just countries.
    """
    children = list(elem)
 
    # Leaf node — no children
    if not children:
        text = elem.text.strip() if elem.text else None
        return text
 
    # ── TYPED-ID FIX ──────────────────────────────────────────────
    # Detect the Workday pattern where sibling <ID type="..."> elements
    # would otherwise be merged into a plain list, losing their type keys.
    if _is_typed_id_group(elem):
        result = {}
        for child in children:
            # Extract the value of the 'type' attribute (strip any namespace)
            type_val = next(
                v for k, v in child.attrib.items()
                if etree.QName(k).localname == "type"
            )
            text = child.text.strip() if child.text else None
            result[type_val] = text
        return result
    # ── END FIX ───────────────────────────────────────────────────
 
    # Default: group children by local tag name
    children_by_tag = {}
    for child in children:
        tag = etree.QName(child.tag).localname
        children_by_tag.setdefault(tag, []).append(_xml_element_to_safe(child))
 
    result = {}
    for tag, items in children_by_tag.items():
        result[tag] = items if len(items) > 1 else items[0]
    return result
 
 
def to_json_safe(value):
    """
    Recursively convert arbitrary Python/Zeep/lxml values into JSON-safe
    primitives: dict, list, str, int, float, bool, None.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        if isinstance(value, str):
            value = html.unescape(value)  # FIX: decode XML/HTML entities e.g. &amp; -> &
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value
    if isinstance(value, (datetime, date, dt_time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, (set, tuple)):
        return [to_json_safe(v) for v in value]
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [to_json_safe(v) for v in value]
    if isinstance(value, Mapping):
        return {str(k): to_json_safe(v) for k, v in value.items()}
    if isinstance(value, etree._Element):
        return _xml_element_to_safe(value)
    try:
        serialized = serialize_object(value)
        return to_json_safe(serialized)
    except Exception:
        pass
    return str(value)
 
# =====================================================
# ZEEP / CARDINALITY NORMALIZATION HELPERS
# =====================================================
def zeep_serialize(value):
    """
    Serialize Zeep objects with best-effort support for consistent typing.
    """
    try:
        return serialize_object(
            value,
            target_cls=dict,
            strict=False,
            xsi_type=True
        )
    except TypeError:
        try:
            return serialize_object(value, target_cls=dict)
        except Exception:
            return value
    except Exception:
        return value
 
def discover_list_paths(obj, current_path=""):
    """
    Discover dotted paths whose values are lists anywhere in the object.
    Example discovered path:
        Envelope.Body.Get_Workers_Response.Response_Data.Worker
    """
    discovered = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            next_path = f"{current_path}.{k}" if current_path else k
            if isinstance(v, list):
                discovered.add(next_path)
            discovered.update(discover_list_paths(v, next_path))
    elif isinstance(obj, list):
        for item in obj:
            discovered.update(discover_list_paths(item, current_path))
    return discovered
 
def ensure_list_paths(obj, list_paths, current_path=""):
    """
    Force exact dotted paths to always be lists.
    """
    if not list_paths:
        return obj
    if isinstance(obj, dict):
        normalized = {}
        for k, v in obj.items():
            next_path = f"{current_path}.{k}" if current_path else k
            v = ensure_list_paths(v, list_paths, next_path)
            if next_path in list_paths:
                if v is None:
                    normalized[k] = []
                elif isinstance(v, list):
                    normalized[k] = v
                else:
                    normalized[k] = [v]
            else:
                normalized[k] = v
        return normalized
    if isinstance(obj, list):
        return [ensure_list_paths(item, list_paths, current_path) for item in obj]
    return obj
 
def normalize_with_known_paths(obj, known_list_paths):
    """
    Normalize object using previously discovered list paths.
    """
    return ensure_list_paths(obj, known_list_paths)
 
def discover_paths_from_payload(obj, known_list_paths):
    """
    Update known repeating paths from a payload and return normalized payload.
    """
    page_list_paths = discover_list_paths(obj)
    known_list_paths.update(page_list_paths)
    normalized = normalize_with_known_paths(obj, known_list_paths)
    return normalized
 
# =====================================================
# RAW SOAP HELPERS
# =====================================================
def _update_soap_body_page_count(soap_body_xml: str, page: int, count: int) -> str:
    """
    Update <*:Page> and <*:Count> values in the provided SOAP body string.
    Inserts them into Response_Filter if they don't exist.
    """
    page_re = re.compile(r"<([a-zA-Z0-9_]+:)?Page>\s*\d+\s*</([a-zA-Z0-9_]+:)?Page>")
    count_re = re.compile(r"<([a-zA-Z0-9_]+:)?Count>\s*\d+\s*</([a-zA-Z0-9_]+:)?Count>")
    has_page = page_re.search(soap_body_xml) is not None
    has_count = count_re.search(soap_body_xml) is not None
    if not has_page or not has_count:
        rf_open = re.search(r"<([a-zA-Z0-9_]+:)?Response_Filter[^>]*>", soap_body_xml)
        rf_close = re.search(r"</([a-zA-Z0-9_]+:)?Response_Filter>", soap_body_xml)
        if rf_open and rf_close:
            prefix = rf_open.group(1) or ""
            insertion = ""
            if not has_page:
                insertion += f"<{prefix}Page>{page}</{prefix}Page>"
            if not has_count:
                insertion += f"<{prefix}Count>{count}</{prefix}Count>"
            soap_body_xml = (
                soap_body_xml[:rf_open.end()]
                + insertion
                + soap_body_xml[rf_open.end():]
            )
    soap_body_xml = page_re.sub(
        lambda m: re.sub(r">.*<", f">{page}<", m.group(0)),
        soap_body_xml
    )
    soap_body_xml = count_re.sub(
        lambda m: re.sub(r">.*<", f">{count}<", m.group(0)),
        soap_body_xml
    )
    return soap_body_xml
 
def _xml_to_obj(root: etree._Element):
    """
    Convert the entire SOAP XML to a nested Python structure using local names.
    """
    return _xml_element_to_safe(root)
 
def _post_raw_soap(session: Session, url: str, soap_body_xml: str, soapaction: str | None = None) -> dict:
    """
    Post a raw SOAP envelope and parse the response into a Python dict.
    Raises for HTTP errors or SOAP Faults.
    """
    headers = {
        "Content-Type": "text/xml; charset=utf-8",
    }
    if soapaction:
        headers["SOAPAction"] = soapaction
    resp = session.post(
        url,
        data=soap_body_xml.encode("utf-8"),
        headers=headers,
        timeout=120
    )
    resp.raise_for_status()
    root = etree.fromstring(resp.content)
    obj = _xml_to_obj(root)
    fault = find_key(obj, "Fault")
    if fault:
        faultcode = find_key(fault, "faultcode") or "SOAP-FAULT"
        faultstring = find_key(fault, "faultstring") or "Unknown SOAP fault"
        detail = find_key(fault, "detail")
        raise RuntimeError(f"{faultcode}: {faultstring}. Detail: {detail}")
    return obj
 
# =====================================================
# RESPONSE FETCH / SERIALIZE HELPERS
# =====================================================
def _fetch_page_response(
    page_num,
    count_per_page,
    use_raw_soap,
    soap_operation,
    session,
    api_url,
    soap_body,
    soapaction,
):
    """
    Fetch a single page and return it as a Python dict.
    """
    if not use_raw_soap:
        response = soap_operation(Response_Filter={"Page": page_num, "Count": count_per_page})
        response_dict = zeep_serialize(response)
    else:
        body = _update_soap_body_page_count(soap_body, page=page_num, count=count_per_page)
        response_dict = _post_raw_soap(session, api_url, body, soapaction=soapaction)
    return response_dict
 
def _write_page_json(
    response_dict,
    output_folder_path,
    operation_name,
    fallback_page_num,
):
    """
    Write one normalized response page to OneLake as JSON.
    """
    current_page = find_key(response_dict, "Page") or fallback_page_num
    run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_obj = to_json_safe(response_dict)
    json_string = json.dumps(safe_obj, ensure_ascii=False, indent=4)
    file_name = f"{operation_name.lower()}_{current_page}_{run_ts}.json"
    full_file_path = f"{output_folder_path}/{file_name}"
    mssparkutils.fs.put(full_file_path, json_string, True)
    print(f"Written : {full_file_path}")
 
# =====================================================
# MAIN GENERIC FUNCTION
# =====================================================
def extract_workday_api_pages(
    api_url,
    operation_name,
    service_name,
    count_per_page,
    access_token,
    workspace_id,
    lakehouse_id,
    soap_body: str | None = None,   # Optional raw SOAP XML body
    soapaction: str | None = None,  # Optional SOAPAction header if required by tenant
):
    # -------------------------------------------------
    # ROOT PATHS
    # -------------------------------------------------
    _ONELAKE_ROOT = (
        f"abfss://{workspace_id}"
        f"@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}"
    )
    RAW_FILES_ROOT = f"{_ONELAKE_ROOT}/Files/workday"
    # -------------------------------------------------
    # DATE PARTITIONS
    # -------------------------------------------------
    utc_now = datetime.utcnow()
    year_part = utc_now.strftime("%Y")
    month_part = utc_now.strftime("%m")
    day_part = utc_now.strftime("%d")
    # -------------------------------------------------
    # OUTPUT PATH
    # -------------------------------------------------
    output_folder_path = (
        f"{RAW_FILES_ROOT}/"
        f"{service_name.lower()}/"
        f"{operation_name.lower()}/"
        f"{year_part}/"
        f"{month_part}/"
        f"{day_part}"
    )
    print("=" * 60)
    print(f"Output Path : {output_folder_path}")
    # -------------------------------------------------
    # SHARED SESSION
    # -------------------------------------------------
    session = Session()
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    # -------------------------------------------------
    # CHOOSE MODE
    # -------------------------------------------------
    use_raw_soap = soap_body is not None and soap_body.strip() != ""
    soap_operation = None
    if not use_raw_soap:
        transport = Transport(session=session)
        history = HistoryPlugin()
        client = Client(
            wsdl=f"{api_url}?wsdl",
            transport=transport,
            plugins=[history]
        )
        soap_operation = getattr(client.service, operation_name)
    # -------------------------------------------------
    # LEARN FROM FIRST PAGE
    # -------------------------------------------------
    known_list_paths = set()
    page_cache = {}
    print("=" * 60)
    print("Fetching initial page : 1")
    page_1 = _fetch_page_response(
        page_num=1,
        count_per_page=count_per_page,
        use_raw_soap=use_raw_soap,
        soap_operation=soap_operation,
        session=session,
        api_url=api_url,
        soap_body=soap_body,
        soapaction=soapaction,
    )
    page_cache[1] = page_1
    total_pages = find_key(page_1, "Total_Pages")
    if total_pages is None:
        total_results = find_key(page_1, "Total_Results")
        if total_results is not None:
            try:
                total_pages = math.ceil(int(total_results) / int(count_per_page))
            except Exception:
                total_pages = None
    if total_pages is None:
        raise Exception("Total_Pages not found and unable to compute from Total_Results.")
    total_pages = int(total_pages)
    print(f"Total Pages : {total_pages}")
    known_list_paths.update(discover_list_paths(page_1))
    # -------------------------------------------------
    # LEARN FROM SECOND PAGE IF AVAILABLE
    # -------------------------------------------------
    if total_pages >= 2:
        print("=" * 60)
        print("Fetching learning page : 2")
        page_2 = _fetch_page_response(
            page_num=2,
            count_per_page=count_per_page,
            use_raw_soap=use_raw_soap,
            soap_operation=soap_operation,
            session=session,
            api_url=api_url,
            soap_body=soap_body,
            soapaction=soapaction,
        )
        page_cache[2] = page_2
        known_list_paths.update(discover_list_paths(page_2))
    # -------------------------------------------------
    # NORMALIZE CACHED PAGES AFTER LEARNING
    # -------------------------------------------------
    for cached_page_num in list(page_cache.keys()):
        page_cache[cached_page_num] = normalize_with_known_paths(
            page_cache[cached_page_num],
            known_list_paths
        )
    # -------------------------------------------------
    # WRITE CACHED PAGES IN ORDER
    # -------------------------------------------------
    write_errors = 0   # FIX: track failed writes to detect total failure
    total_writes = 0
    for page_num in sorted(page_cache.keys()):
        print("=" * 60)
        print(f"Writing Cached Page : {page_num}")
        total_writes += 1
        try:
            _write_page_json(
                response_dict=page_cache[page_num],
                output_folder_path=output_folder_path,
                operation_name=operation_name,
                fallback_page_num=page_num,
            )
        except Exception as e:
            write_errors += 1
            print(f"Error Writing Cached Page : {page_num}")
            print(str(e))
    # -------------------------------------------------
    # LOOP REMAINING PAGES
    # -------------------------------------------------
    start_page = 3 if total_pages >= 2 else 2
    for page_num in range(start_page, total_pages + 1):
        print("=" * 60)
        print(f"Processing Page : {page_num}")
        total_writes += 1
        try:
            response_dict = _fetch_page_response(
                page_num=page_num,
                count_per_page=count_per_page,
                use_raw_soap=use_raw_soap,
                soap_operation=soap_operation,
                session=session,
                api_url=api_url,
                soap_body=soap_body,
                soapaction=soapaction,
            )
            # Learn any new list paths from later pages too
            new_paths = discover_list_paths(response_dict)
            if new_paths - known_list_paths:
                known_list_paths.update(new_paths)
            response_dict = normalize_with_known_paths(response_dict, known_list_paths)
            _write_page_json(
                response_dict=response_dict,
                output_folder_path=output_folder_path,
                operation_name=operation_name,
                fallback_page_num=page_num,
            )
        except Exception as e:
            write_errors += 1
            print(f"Error Processing Page : {page_num}")
            print(str(e))
    print("=" * 60)
    # FIX: raise if every single write failed — means root cause error (e.g. serialisation bug)
    # that silently swallowed all writes; surface it clearly before spark.read fails downstream
    if write_errors == total_writes:
        raise RuntimeError(
            f"All {total_writes} page write(s) failed for operation={operation_name}. "
            f"No files written to {output_folder_path}. Check per-page errors above."
        )
    print(f"Completed Successfully ({total_writes - write_errors}/{total_writes} pages written)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def add_audit_column(df: DataFrame, job_name: str):
    cols = {
        "created_on": F.current_timestamp(),
        "created_by": F.lit(job_name),
        "modified_on": F.current_timestamp(),
        "modified_by": F.lit(job_name)
    }

    return df.withColumns(cols)

def to_lowercase_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[c.lower() for c in df.columns])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_data(target_table_path: str, source_df: DataFrame, keys: list):
    from delta.tables import DeltaTable

    delta_table = DeltaTable.forPath(spark, target_table_path)

    # Null-safe merge condition
    merge_condition = " AND ".join(
        [f"t.`{k}` <=> s.`{k}`" for k in keys]
    )

    # Audit columns
    insert_only_cols = {"created_on", "created_by"}

    audit_columns = {"created_on", "created_by", "modified_on", "modified_by"}

    # All source columns
    source_cols = source_df.columns

    # Columns to compare for change detection
    compare_cols = [
        c for c in source_cols
        if c not in keys and c not in audit_columns
    ]

    # Update only when at least one column value has changed
    update_condition = " OR ".join(
        [f"NOT (t.`{c}` <=> s.`{c}`)" for c in compare_cols]
    )

    # Update mapping
    update_set = {
        col: f"s.`{col}`"
        for col in source_cols
        if col not in insert_only_cols
    }

    # Insert mapping
    insert_set = {
        col: f"s.`{col}`"
        for col in source_cols
    }

    (
        delta_table.alias("t")
        .merge(source_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            condition=update_condition,
            set=update_set
        )
        .whenNotMatchedInsert(values=insert_set)
        .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_delta_tbl(source_df: DataFrame, target_table_path: str, keys: list, partition_by: Optional[List[str]] =None):
    """
    Write a DataFrame to a Delta table. Creates the table if it does not
    exist, otherwise appends data with schema evolution enabled.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Input DataFrame.
    abfss_path : str
        Delta table path.
    partition_by : list[str], optional
        Partition columns.
    """
    partition_by = partition_by or []

    writer = (
        source_df.write
        .format("delta")
        .option("mergeSchema", "true")
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if table_exists(target_table_path):
        upsert_data(target_table_path, source_df, keys)
       
    else:
        (
            writer
            .mode("overwrite")
            .save(target_table_path)
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def table_exists(abfss_path):
    try:
        df = spark.read.format("delta").load(abfss_path)
        return True
    except Exception:
        return False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def explode_ids(df, col_names=[]):
    """
    Explodes nested Workday ID arrays and pivots them into columns while
    preserving the original row count.

    Args:
        df: Input Spark DataFrame.
        col_names: List of dictionaries, e.g.
            [
                {
                    "col_name": "Educational_Taxonomy_Reference",
                    "col_not_req": ["WID"],
                    "col_rename": {
                        "educational_taxonomy_code_id": "Educational_Taxonomy_Code_ID"
                    }
                }
            ]

    Returns:
        Spark DataFrame
    """

    final_df = df

    # Create a unique key to preserve every original row
    final_df = final_df.withColumn(
        "__explode_row_id",
        F.monotonically_increasing_id()
    )

    for item in col_names:

        col_name = item["col_name"]

        if len(final_df.columns) == 1 and col_name in final_df.columns:
            final_df = final_df.withColumn("dummy_key", F.lit(1))

        drop_exploded_cols = [
            f"{x}#drop".lower()
            for x in item.get("col_not_req", [])
        ]

        ids_df = (
            final_df
            .withColumn(
                "tag_id",
                F.explode_outer(F.col(f"{col_name}.ID"))
            )
            .withColumn(
                "id_type",
                F.concat(
                    F.coalesce(
                        F.col("tag_id.type"),
                        F.lit("unknown")
                    ),
                    F.lit("#drop")
                )
            )
            .withColumn(
                "id_value",
                F.col("tag_id._value_1")
            )
            .drop("tag_id", col_name)
        )

        # Preserve every original row by grouping with the generated row id
        group_cols = [
            c for c in ids_df.columns
            if c not in ("id_type", "id_value")
        ]

        pivot_df = (
            ids_df
            .groupBy(*group_cols)
            .pivot("id_type")
            .agg(F.first("id_value"))
            .drop("unknown#drop", "dummy_key")
        )

        select_expr = []

        col_rename = {
            k.lower(): v
            for k, v in item.get("col_rename", {}).items()
        }

        for c in pivot_df.columns:

            if c == "__explode_row_id":
                continue

            if c.endswith("#drop"):

                if c.lower() in drop_exploded_cols:
                    continue

                original_name = c[:-5].lower()

                new_name = col_rename.get(
                    original_name,
                    original_name
                )

                select_expr.append(f"`{c}` AS `{new_name}`")

            else:
                select_expr.append(f"`{c}`")

        final_df = pivot_df.selectExpr(*select_expr)

    # Drop the helper column from the final output if it still exists
    if "__explode_row_id" in final_df.columns:
        final_df = final_df.drop("__explode_row_id")

    return final_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def explode_ids(df, col_names = {}):
#     """Explode nested ID arrays for configured columns and pivot them to wide format.

#     This helper is designed to work with Workday-style ID structures where a
#     nested field contains an array of ID objects of the form:

#         <col_name>.ID = [ { type: '...', _value_1: '...' }, ... ]

#     For each configured column in `col_names`, this function:
#       1. Explodes the ID array so that each ID becomes its own row.
#       2. Builds an `id_type` column (e.g., "Student_ID#drop") and an
#          `id_value` column holding the corresponding ID value.
#       3. Pivots these rows on `id_type` so each ID type becomes a separate
#          column (wide format).
#       4. Optionally renames/drops exploded columns based on the
#          `col_not_req` list for that column.

#     Args:
#         df (DataFrame): Input Spark DataFrame with nested ID structures.
#         col_names (list[dict]): List of configuration dictionaries, each of
#             the form:

#                 {
#                     "col_name": <string>,
#                     "col_not_req": [<string>, ...],
#                     "col_rename": {<string>:<string>, ...}
#                 }:

#             where:
#               - col_name    : the nested column that contains an `ID` array.
#               - col_not_req : (optional) list of ID types that should be
#                               dropped after pivot. These are compared
#                               case-insensitively against the lowercased
#                               `id_type` names.

#     Returns:
#         DataFrame: DataFrame with ID arrays exploded and pivoted into
#         separate columns for each ID type.
#     """

#     # Start from the original DataFrame and progressively transform it for
#     # each configured column in col_names.
#     final_df = df

#     for item in col_names:
#         col_name = item["col_name"]

#         if len(df.columns) == 1 and col_name in df.columns:
#             final_df = final_df.withColumn("dummy_key", F.col(col_name))

#         # Build a list of ID types that we do NOT want to keep after the
#         # pivot. These are stored as "<id_type>#drop" and compared in
#         # lower-case to avoid case-sensitivity issues.
#         drop_exploded_cols = [f"{x}#drop".lower() for x in item.get("col_not_req", [])]

#         # Explode the ID array under <col_name>.ID. Each element in the array
#         # becomes its own row. We then derive:
#         #   - id_type  = tag_id.type + "#drop" (marks this as a candidate for
#         #                renaming/dropping later)
#         #   - id_value = tag_id._value_1
#         ids_df = (
#                 final_df.withColumn("tag_id", F.explode_outer(f"{col_name}.ID"))
#                     .select(
#                         F.col("*"),
#                         F.concat(F.when(F.col("tag_id.type").isNull(), F.lit("unkown#")).otherwise(F.col("tag_id.type")), F.lit(f"#drop")).alias("id_type"),
#                         F.col("tag_id._value_1").alias("id_value")
#                     )
#                     .drop("tag_id", col_name)
#             )

#         # Pivot on id_type to create columns, keeping other columns as
#         # grouping keys. Each distinct id_type becomes a separate column.
#         group_cols = [c for c in ids_df.columns if c not in ["id_type", "id_value"]]

#         pivot_df = (
#             ids_df.groupBy(*group_cols)
#             .pivot("id_type")
#             .agg(F.first("id_value"))
#             .drop("unkown##drop", "dummy_key")
#         )

#         # Build the select list with appropriate renaming/dropping rules:
#         #   - Columns ending with "#drop" and NOT in drop_exploded_cols are
#         #     kept but renamed to strip the "#drop" suffix.
#         #   - Columns whose names appear in drop_exploded_cols are skipped.
#         #   - All other columns are kept as-is.
#         select_cols = []
#         col_rename = item.get("col_rename", {})
#         for x in pivot_df.columns:
#             if x.endswith(f"#drop") and x.lower() not in drop_exploded_cols:
#                 # Keep and rename: "SomeID#drop" -> "SomeID"
#                 removed_drop = x.split('#')[0].lower()
#                 col_nm = col_rename.get(removed_drop, None)
#                 if col_nm:
#                     select_cols.append(f"`{x}` AS {col_nm}")
#                 else:
#                     select_cols.append(f"`{x}` AS {removed_drop}")
#             elif not x.endswith(f"#drop"):
#                 # Non-#drop columns are kept unchanged.
#                 select_cols.append(f"{x} as {x}")

#         # print(select_cols)
#         final_df = pivot_df.selectExpr(*select_cols)

#     return final_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import time
import json
import builtins
import requests
from datetime import datetime


def ingest_api_to_lakehouse(
    url,
    headers,
    service_name,
    operation_name,
    workspace_id,
    lakehouse_id,
    max_retries=3,
    retry_delay=10,
    timeout=600
):
    """
    Stream API response into Microsoft Fabric OneLake.

    Designed for large JSON responses.
    """

    root = (
        f"abfss://{workspace_id}"
        f"@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}"
    )


    for attempt in range(1, max_retries + 1):

        now = datetime.utcnow()

        output_folder = (
            f"{root}/Files/workday/"
            f"{service_name.lower()}/"
            f"{operation_name.lower()}/"
            f"{now:%Y/%m/%d}"
        )

        filename = (
            f"{operation_name.lower()}_"
            f"{now:%Y%m%d_%H%M%S_%f}.json"
        )

        output_path = f"{output_folder}/{filename}"


        try:

            print("==============================")
            print(f"Attempt {attempt}/{max_retries}")
            print("==============================")


            start = time.time()


            # -----------------------------
            # Call API with streaming
            # -----------------------------

            print("Calling API...")

            response = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=timeout
            )

            response.raise_for_status()


            expected_size = response.headers.get(
                "Content-Length"
            )


            if expected_size:
                expected_size = int(expected_size)

                print(
                    f"Expected Size : {expected_size:,} bytes"
                )


            # -----------------------------
            # Write chunks
            # -----------------------------

            temp_file = f"/tmp/{filename}"


            bytes_written = 0


            print("Downloading response...")


            with open(temp_file, "wb") as f:

                for chunk in response.iter_content(
                    chunk_size=1024 * 1024
                ):

                    if chunk:

                        f.write(chunk)

                        bytes_written += len(chunk)



            print(
                f"Downloaded Size: {bytes_written:,} bytes"
            )


            # -----------------------------
            # Validate size
            # -----------------------------

            if expected_size:

                if bytes_written != expected_size:

                    raise Exception(
                        f"Size mismatch. "
                        f"Expected {expected_size}, "
                        f"received {bytes_written}"
                    )


            # -----------------------------
            # Validate JSON
            # -----------------------------

            print("Validating JSON...")


            with open(
                temp_file,
                "r",
                encoding="utf-8"
            ) as f:

                json.load(f)



            # -----------------------------
            # Upload to OneLake
            # -----------------------------

            print("Uploading to OneLake...")


            with open(
                temp_file,
                "r",
                encoding="utf-8"
            ) as f:

                content = f.read()


            mssparkutils.fs.put(
                output_path,
                content,
                False
            )


            elapsed = builtins.round(
                time.time() - start,
                2
            )


            print("==============================")
            print("SUCCESS")
            print("==============================")
            print(f"File : {output_path}")
            print(f"Size : {bytes_written:,}")
            print(f"Time : {elapsed} seconds")


            os.remove(temp_file)


            return output_path



        except Exception as e:


            print("==============================")
            print("FAILED")
            print(e)
            print("==============================")


            if attempt == max_retries:

                raise


            print(
                f"Retrying in {retry_delay} seconds..."
            )

            time.sleep(retry_delay)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# def get_api_response(url, headers, max_retries=3, retry_delay=5, timeout=300):
#     """
#     Makes a GET request with retry logic.

#     Args:
#         url (str): API endpoint.
#         headers (dict): Request headers.
#         max_retries (int): Maximum retry attempts.
#         retry_delay (int): Delay between retries in seconds.
#         timeout (int): Request timeout in seconds.

#     Returns:
#         requests.Response: Successful response object.

#     Raises:
#         Exception: If all retry attempts fail.
#     """
#     for attempt in range(1, max_retries + 1):
#         try:
#             print(f"Attempt {attempt} of {max_retries}...")

#             response = requests.get(
#                 url,
#                 headers=headers,
#                 timeout=timeout
#             )

#             response.raise_for_status()

#             print("Request succeeded.")
#             return response

#         except requests.exceptions.RequestException as e:
#             print(f"Attempt {attempt} failed: {e}")

#             if attempt == max_retries:
#                 raise Exception(
#                     f"API request failed after {max_retries} attempts."
#                 ) from e

#             print(f"Retrying in {retry_delay} seconds...")
#             time.sleep(retry_delay)


# def save_response_to_lakehouse(response, service_name, operation_name, workspace_id, lakehouse_id):
#     """
#     Saves the API response to a Lakehouse file.
#     """

#     # -------------------------------------------------
#     # ROOT PATHS
#     # -------------------------------------------------
#     _ONELAKE_ROOT = (
#         f"abfss://{workspace_id}"
#         f"@onelake.dfs.fabric.microsoft.com/"
#         f"{lakehouse_id}"
#     )
#     RAW_FILES_ROOT = f"{_ONELAKE_ROOT}/Files/workday"
#     # -------------------------------------------------
#     # DATE PARTITIONS
#     # -------------------------------------------------
#     utc_now = datetime.utcnow()
#     year_part = utc_now.strftime("%Y")
#     month_part = utc_now.strftime("%m")
#     day_part = utc_now.strftime("%d")
#     run_ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
#     file_name = f"{operation_name.lower()}_{run_ts}.json"
#     # -------------------------------------------------
#     # OUTPUT PATH
#     # -------------------------------------------------
#     output_folder_path = (
#         f"{RAW_FILES_ROOT}/"
#         f"{service_name.lower()}/"
#         f"{operation_name.lower()}/"
#         f"{year_part}/"
#         f"{month_part}/"
#         f"{day_part}/"
#         f"{file_name}"
#     )
#     print(f"Status Code: {response.status_code}")
#     print(f"Response Size: {len(response.text):,} bytes")

#     mssparkutils.fs.put(
#         output_folder_path,
#         response.text,
#         True  # overwrite
#     )

#     print(f"Response saved to: {output_folder_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
