# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5bd544a5-340c-493c-aa88-61dc1992c614",
# META       "default_lakehouse_name": "lh_cs_landing",
# META       "default_lakehouse_workspace_id": "f2aea860-156d-4e18-b078-1356be7161de",
# META       "known_lakehouses": [
# META         {
# META           "id": "5bd544a5-340c-493c-aa88-61dc1992c614"
# META         },
# META         {
# META           "id": "0277ffad-3784-4551-8d7d-66d1b8ff633a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable
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


def _element_attributes(elem):
    """
    Return XML attributes using local names (strip namespaces).
    Example:
        wd:Public="0"  -> {"Public": "0"}
        wd:Primary="1" -> {"Primary": "1"}
    """
    return {
        etree.QName(k).localname: v
        for k, v in elem.attrib.items()
    }
 
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
    # print("TAG:", etree.QName(elem.tag).localname)
    # print("ATTRS:", {
    #     etree.QName(k).localname: v
    #     for k, v in elem.attrib.items()
    # })
    children = list(elem)

    # Leaf node — no children
    if not children:
        text = elem.text.strip() if elem.text else None
        attrs = _element_attributes(elem)

        if attrs:
            if text is not None:
                attrs["_value"] = text
            return attrs
        return text
 
    # ── TYPED-ID FIX ──────────────────────────────────────────────
    # Detect the Workday pattern where sibling <ID type="..."> elements
    # would otherwise be merged into a plain list, losing their type keys.
    if _is_typed_id_group(elem):
        # Preserve any attributes on the parent element
        result = _element_attributes(elem)

        for child in children:
            type_val = next(
                v
                for k, v in child.attrib.items()
                if etree.QName(k).localname == "type"
            )
            text = child.text.strip() if child.text else None
            result[type_val] = text
        return result
    # ── END FIX ───────────────────────────────────────────────────
 
    # Default: group children by local tag name
    # Preserve attributes on the current element
    result = _element_attributes(elem)

    children_by_tag = {}
    for child in children:
        tag = etree.QName(child.tag).localname
        children_by_tag.setdefault(tag, []).append(_xml_element_to_safe(child))

    for tag, items in children_by_tag.items():
        result[tag] = items if len(items) > 1 else items[0]

    # print("RETURNING:", etree.QName(elem.tag).localname, result)
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
# ATTENTION: AI-generated code can include errors or operations you didn't intend. Review the code in this cell carefully before running it.

def zeep_serialize(value):
    """
    Serialize Zeep objects with best-effort support for consistent typing.

    NOTE: We explicitly set ``xsi_type=True`` and ``false_values=None`` so
    that properties defined as arrays in the WSDL are always returned as
    lists (even when they contain zero or one item). This improves
    cardinality stability before we apply our own list-path normalization.
    """
    try:
        return serialize_object(
            value,
            target_cls=dict,
            # strict=False,
            # xsi_type=True,
            # false_values=None,
        )
    except TypeError:
        try:
            return serialize_object(
                value,
                target_cls=dict,
                false_values=None,
            )
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

    # print(etree.tostring(root, pretty_print=True, encoding="unicode"))

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
    # --- XSD-based cardinality fix ------------------------------------------------
    # Seed known_list_paths from the WSDL/XSD schema (ground truth for maxOccurs)
    # BEFORE any page is sampled, instead of relying solely on what pages 1-2
    # happen to contain. This prevents the same repeatable element (e.g.
    # National_ID, Program_of_Study_Record, phone/email entries) from being
    # written as a struct in some page files and as an array in others later in
    # the run -- which is what caused the intermittent Delta merge/schema errors
    # for Get_Students / Get_Student_Applications. Falls back to an empty set on
    # any failure, so existing sample-based discovery behaviour is unaffected.
    known_list_paths.update(
        _discover_schema_list_paths_safe(api_url, operation_name, access_token)
    )
    # --------------------------------------------------------------------------------
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

from concurrent.futures import ThreadPoolExecutor, as_completed


def extract_workday_api_pages_v2(
    api_url,
    operation_name,
    service_name,
    count_per_page,
    access_token,
    workspace_id,
    lakehouse_id,
    soap_body: str | None = None,   # Optional raw SOAP XML body
    soapaction: str | None = None,  # Optional SOAPAction header if required by tenant
    parallel_requests: int = 1,     # NEW: max concurrent page requests (raw SOAP only)
):
    """Version 2 of extract_workday_api_pages with optional parallel page fetch.

    Behaviour:
      * When parallel_requests <= 1 → delegates to the original
        extract_workday_api_pages (sequential behaviour).
      * When parallel_requests > 1:
          - Only supported in raw SOAP mode (soap_body provided).
          - Fetches pages 1 & 2 sequentially to discover Total_Pages and
            initial list paths.
          - Fetches remaining pages in parallel using ThreadPoolExecutor.
          - Discovers list paths from *all* fetched pages, then normalizes
            every page with the final set of list paths.
          - Writes all pages to OneLake in page-number order.
    """

    # Sanitize parallel_requests
    try:
        parallel_requests = int(parallel_requests)
    except Exception:
        parallel_requests = 1
    if parallel_requests < 1:
        parallel_requests = 1

    # If no parallelism requested, fall back to v1 for identical behaviour
    if parallel_requests == 1:
        print("parallel_requests=1 → using sequential extract_workday_api_pages()")
        return extract_workday_api_pages(
            api_url=api_url,
            operation_name=operation_name,
            service_name=service_name,
            count_per_page=count_per_page,
            access_token=access_token,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            soap_body=soap_body,
            soapaction=soapaction,
        )

    # -------------------------------------------------
    # ROOT / OUTPUT PATHS (same layout as v1)
    # -------------------------------------------------
    _ONELAKE_ROOT = (
        f"abfss://{workspace_id}"
        f"@onelake.dfs.fabric.microsoft.com/"
        f"{lakehouse_id}"
    )
    RAW_FILES_ROOT = f"{_ONELAKE_ROOT}/Files/workday"

    utc_now = datetime.utcnow()
    year_part = utc_now.strftime("%Y")
    month_part = utc_now.strftime("%m")
    day_part = utc_now.strftime("%d")

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
    # MODE SELECTION
    # -------------------------------------------------
    use_raw_soap = soap_body is not None and soap_body.strip() != ""

    if not use_raw_soap:
        # For safety, we *do not* attempt to parallelize Zeep client calls
        print(
            "[WARN] parallel_requests > 1 is only supported in raw SOAP mode. "
            "Falling back to sequential extract_workday_api_pages() using Zeep client."
        )
        return extract_workday_api_pages(
            api_url=api_url,
            operation_name=operation_name,
            service_name=service_name,
            count_per_page=count_per_page,
            access_token=access_token,
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            soap_body=soap_body,
            soapaction=soapaction,
        )

    # -------------------------------------------------
    # RAW SOAP PARALLEL IMPLEMENTATION
    # -------------------------------------------------
    print("Using raw SOAP mode with parallel requests")

    # Cache for all pages we successfully fetch
    page_cache: dict[int, dict] = {}

    # -------------------------------------------------
    # INITIAL PAGE 1 (discover Total_Pages)
    # -------------------------------------------------
    print("=" * 60)
    print("Fetching initial page : 1")
    page_1 = _fetch_page_response(
        page_num=1,
        count_per_page=count_per_page,
        use_raw_soap=True,
        soap_operation=None,
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

    # -------------------------------------------------
    # OPTIONAL PAGE 2 (still sequential for better path discovery)
    # -------------------------------------------------
    if total_pages >= 2:
        print("=" * 60)
        print("Fetching learning page : 2")
        page_2 = _fetch_page_response(
            page_num=2,
            count_per_page=count_per_page,
            use_raw_soap=True,
            soap_operation=None,
            session=session,
            api_url=api_url,
            soap_body=soap_body,
            soapaction=soapaction,
        )
        page_cache[2] = page_2

    # -------------------------------------------------
    # PARALLEL FETCH REMAINING PAGES
    # -------------------------------------------------
    start_page = 3 if total_pages >= 2 else 2
    remaining_pages = [p for p in range(start_page, total_pages + 1)]

    fetch_errors = 0

    if remaining_pages:
        print("=" * 60)
        print(
            f"Fetching remaining pages in parallel: "
            f"{remaining_pages[0]}..{remaining_pages[-1]} "
            f"with max_workers={parallel_requests}"
        )

        with ThreadPoolExecutor(max_workers=parallel_requests) as executor:
            future_to_page = {
                executor.submit(
                    _fetch_page_response,
                    page_num=p,
                    count_per_page=count_per_page,
                    use_raw_soap=True,
                    soap_operation=None,
                    session=session,
                    api_url=api_url,
                    soap_body=soap_body,
                    soapaction=soapaction,
                ): p
                for p in remaining_pages
            }

            for future in as_completed(future_to_page):
                p = future_to_page[future]
                try:
                    resp = future.result()
                    page_cache[p] = resp
                    print(f"Fetched page {p}")
                except Exception as e:
                    fetch_errors += 1
                    print(f"Error fetching page {p}: {e}")

    # -------------------------------------------------
    # DISCOVER LIST PATHS FROM *ALL* SUCCESSFULLY FETCHED PAGES
    # -------------------------------------------------
    known_list_paths = set()
    # --- XSD-based cardinality fix (see extract_workday_api_pages for details) ----
    known_list_paths.update(
        _discover_schema_list_paths_safe(api_url, operation_name, access_token)
    )
    # --------------------------------------------------------------------------------
    for payload in page_cache.values():
        page_paths = discover_list_paths(payload)
        known_list_paths.update(page_paths)

    # -------------------------------------------------
    # NORMALIZE & WRITE ALL PAGES IN ORDER
    # -------------------------------------------------
    write_errors = 0
    total_writes = 0

    for page_num in sorted(page_cache.keys()):
        print("=" * 60)
        print(f"Writing Page : {page_num}")
        total_writes += 1
        try:
            normalized = normalize_with_known_paths(
                page_cache[page_num],
                known_list_paths,
            )
            _write_page_json(
                response_dict=normalized,
                output_folder_path=output_folder_path,
                operation_name=operation_name,
                fallback_page_num=page_num,
            )
        except Exception as e:
            write_errors += 1
            print(f"Error writing page {page_num}: {e}")

    print("=" * 60)
    if total_writes == 0:
        raise RuntimeError(
            "No pages were successfully fetched; nothing written to OneLake."
        )

    if write_errors == total_writes:
        raise RuntimeError(
            f"All {total_writes} page write(s) failed for operation={operation_name}. "
            f"No files written to {output_folder_path}. Check per-page errors above."
        )

    print(
        f"Completed Successfully ("
        f"{total_writes - write_errors}/{total_writes} pages written, "
        f"{fetch_errors} fetch errors)"
    )

    # Return basic summary
    return {
        "total_pages": total_pages,
        "pages_attempted": total_pages,
        "pages_fetched": len(page_cache),
        "pages_written": total_writes - write_errors,
        "fetch_errors": fetch_errors,
        "write_errors": write_errors,
        "output_folder_path": output_folder_path,
    }


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

    # If there are no non-key, non-audit columns to compare, skip whenMatchedUpdate
    has_update_condition = bool(update_condition.strip()) if isinstance(update_condition, str) else False

    if has_update_condition:
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
    else:
        (
            delta_table.alias("t")
            .merge(source_df.alias("s"), merge_condition)
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

def add_scd2_audit_column(df):
    cols = {
        "effective_from": F.current_timestamp(),
        "effective_to": F.to_timestamp(F.lit("9999-12-31 23:59:59")),
        "is_current" : F.lit(1)

    }
    return df.withColumns(cols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def perform_scd_typ2(spark, update_df, target_path, primary_keys):
    """
    Perform an SCD Type 2 merge into a Delta table.
    """

    try:

        # Initial load
        if not DeltaTable.isDeltaTable(spark, target_path):
            (
                update_df.write
                .format("delta")
                .mode("overwrite")
                .save(target_path)
            )
            return True

        target_table = DeltaTable.forPath(spark, target_path)

        remove_cols = ["effective_from", "effective_to", "is_current"] + primary_keys
        non_pk_cols = [c for c in update_df.columns if c not in remove_cols]

        not_conditions = [
            f"(NOT (updates.{c} <=> target.{c}))"
            for c in non_pk_cols
        ]
        not_condition_str = " OR ".join(not_conditions)

        null_merge_keys = [f"NULL AS {pk}_merge_key" for pk in primary_keys]
        merge_keys = [f"{pk} AS {pk}_merge_key" for pk in primary_keys]

        insert_cols = {
            c: f"updates.{c}"
            for c in update_df.columns
        }

        merge_condition = " AND ".join(
            [
                f"target.{pk} = updates.{pk}_merge_key"
                for pk in primary_keys
            ]
        )

        # Identify changed current records
        new_records_to_insert = (
            update_df.alias("updates")
            .join(target_table.toDF().alias("target"), primary_keys)
            .where(f"target.is_current = 1 AND ({not_condition_str})")
            .selectExpr("updates.*")
        )

        # Stage records for update and insert
        staged_updates = (
            new_records_to_insert
            .selectExpr(*null_merge_keys, "*")
            .unionByName(update_df.selectExpr(*merge_keys, "*"))
        )
        
        # Expire existing records and insert new versions
        (
            target_table.alias("target")
            .merge(
                staged_updates.alias("updates"),
                f"{merge_condition}",
            )
            .whenMatchedUpdate(
                condition = f"target.is_current = 1 AND {not_condition_str}",
                set={
                    "is_current": F.lit(0),
                    "effective_to": "updates.effective_from",
                }
            )
            .whenNotMatchedInsert(values=insert_cols)
            .execute()
        )

        return True

    except Exception as e:
        raise Exception(f"Failed to perform SCD Type 2 merge: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calculate_end_dates(
    incremental_df,
    id_columns=[],
    target_df=None,
    history_in_incremental=False,
    effective_date_column="effective_date"
):
    """
    Parameters
    ----------
    incremental_df : DataFrame
        Incoming incremental data.

    target_df : DataFrame
        Existing target table. Required only when history_in_incremental=False.

    history_in_incremental : bool
        True  -> Incremental contains complete history for affected IDs.
        False -> Incremental contains only new records.

    id_columns : list
        Business key columns.

    effective_date_column : str
        Effective date column.
    """

    if history_in_incremental:
        working_df = incremental_df

    else:
        if target_df is None:
            raise ValueError(
                "target_df is required when history_in_incremental=False"
            )

        affected_ids = incremental_df.select(*id_columns).distinct()

        history_df = (
            target_df
            .join(affected_ids, id_columns, "inner")
        )

        working_df = history_df.unionByName(incremental_df)

    # Remove duplicates (important if incoming contains existing rows)
    working_df = working_df.dropDuplicates(id_columns + [effective_date_column])

    window = (
        Window.partitionBy(*id_columns)
              .orderBy(effective_date_column)
    )

    result_df = (
        working_df
        .withColumn(
            "next_effective_date",
            F.lead(effective_date_column).over(window)
        )
        .withColumn(
            "expiration_date",
            F.when(
                F.col("next_effective_date").isNull(),
                F.lit("9999-12-31").cast("date")
            ).otherwise(
                F.date_sub(F.col("next_effective_date"), 1)
            )
        )
        .drop("next_effective_date")
    )

    return result_df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### XSD-based cardinality fix
# 
# `_discover_schema_list_paths_safe` reads the Workday WSDL/XSD for the operation being
# called and returns the dotted element paths that the schema declares as repeatable
# (`maxOccurs` > 1 or `unbounded`). This is used to seed `known_list_paths` in
# `extract_workday_api_pages` / `extract_workday_api_pages_v2` **before** any page is
# sampled, instead of inferring cardinality only from pages 1-2 of live data.
# 
# Root cause this fixes: elements that are singular on most records but occasionally
# repeat (e.g. `National_ID`, `Program_of_Study_Record`, phone/email entries) were
# previously only forced into JSON arrays if that repetition happened to show up in the
# first 1-2 sampled pages. When it first appeared later in a run (or on a later day),
# earlier page files kept the field as a struct while later ones wrote it as an array of
# structs -- a type conflict Spark/Delta cannot auto-merge. This is why `Get_Students`
# and `Get_Student_Applications` (which have many such low-frequency multi-valued
# elements) worked for a while and then started failing, while flatter operations were
# unaffected.
# 
# This is purely additive: no existing function signature, variable name, or execution
# order above is changed. If schema introspection fails for any reason (WSDL
# unreachable, unexpected type name, etc.) it returns an empty set, so behaviour falls
# back exactly to the previous sample-based discovery.


# CELL ********************

# =====================================================
# XSD-BASED CARDINALITY FIX
# =====================================================

# Cache keyed by (api_url, operation_name) so the WSDL is only introspected
# once per operation, not once per page.
_SCHEMA_LIST_PATH_CACHE = {}


def _discover_schema_list_paths_safe(api_url, operation_name, access_token):
    """
    Return the set of dotted element paths that the Workday WSDL/XSD schema
    declares as repeatable (maxOccurs > 1 / unbounded) for the given
    operation's response type -- the schema ground truth, independent of
    what any particular sampled page of data happens to contain.

    Both path styles produced elsewhere in this notebook are included:
      * zeep-serialized calls  -> paths start at "Response_Data...."
      * raw-SOAP calls (_post_raw_soap parses the full envelope) ->
        paths start at "Envelope.Body.<Operation>_Response...."

    Best-effort / non-fatal: any failure (unreachable WSDL, unexpected type
    name, older zeep version, etc.) returns an empty set so the existing
    sample-based discovery (discover_list_paths / ensure_list_paths /
    normalize_with_known_paths) continues to work exactly as before.
    """
    cache_key = (api_url, operation_name)
    if cache_key in _SCHEMA_LIST_PATH_CACHE:
        return _SCHEMA_LIST_PATH_CACHE[cache_key]

    schema_paths = set()
    try:
        schema_session = Session()
        schema_session.headers.update({"Authorization": f"Bearer {access_token}"})
        schema_transport = Transport(session=schema_session)
        schema_client = Client(wsdl=f"{api_url}?wsdl", transport=schema_transport)

        response_type_qname = f"{{urn:com.workday/bsvc}}{operation_name}_ResponseType"
        response_type = schema_client.get_type(response_type_qname)

        visited_types = set()

        def walk(xsd_type, current_path):
            if xsd_type in visited_types:
                return
            visited_types.add(xsd_type)
            elements = getattr(xsd_type, "elements", None)
            if not elements:
                return
            for name, element in elements:
                next_path = f"{current_path}.{name}" if current_path else name
                max_occurs = getattr(element, "max_occurs", 1)
                if (
                    max_occurs is None
                    or max_occurs == "unbounded"
                    or (isinstance(max_occurs, int) and max_occurs > 1)
                ):
                    schema_paths.add(next_path)
                if getattr(element, "type", None) is not None:
                    walk(element.type, next_path)

        walk(response_type, "")

        # Add the raw-SOAP-envelope path style too (see docstring above).
        envelope_prefix = f"Envelope.Body.{operation_name}_Response"
        schema_paths |= {f"{envelope_prefix}.{p}" for p in list(schema_paths)}

    except Exception as e:
        print(
            f"[XSD cardinality fix] Could not derive schema list paths for "
            f"{operation_name} (falling back to sample-based discovery only): {e}"
        )
        schema_paths = set()

    _SCHEMA_LIST_PATH_CACHE[cache_key] = schema_paths
    return schema_paths


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
