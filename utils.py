# utils.py
import requests
import json
import logging
from typing import Optional, Dict, Any, List
import math # Import math for isnan check

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
log = logging.getLogger(__name__)

API_BASE_URL = "https://dashboard.nregsmp.org/api" # Or load from config/env

# fetch_api_data remains the same as before...
def fetch_api_data(endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """
    Fetches data from a specified API endpoint.
    (Implementation from previous response)
    """
    full_url = f"{API_BASE_URL}{endpoint}"
    try:
        log.info(f"Fetching data from: {full_url} with params: {params}")
        response = requests.get(full_url, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        log.info(f"Successfully fetched data from {endpoint}")
        if isinstance(data, dict) and data.get("error"):
             log.error(f"API endpoint {endpoint} returned an error: {data['error']}")
             return None
        if isinstance(data, dict) and data.get("detail"):
             log.error(f"API endpoint {endpoint} returned detail error: {data['detail']}")
             return None
        return data
    except requests.exceptions.Timeout:
        log.error(f"Timeout error fetching data from {full_url}")
        return None
    except requests.exceptions.HTTPError as http_err:
        log.error(f"HTTP error fetching data from {full_url}: {http_err}")
        try:
            error_detail = http_err.response.json()
            log.error(f"API Error Detail: {json.dumps(error_detail)}")
        except json.JSONDecodeError:
            log.error(f"Response Text (non-JSON): {http_err.response.text[:500]}...")
        return None
    except requests.exceptions.RequestException as req_err:
        log.error(f"Request exception fetching data from {full_url}: {req_err}")
        return None
    except json.JSONDecodeError as json_err:
        log.error(f"JSON decode error fetching data from {full_url}: {json_err}")
        log.error(f"Response text: {response.text[:500]}") # Use response from outer scope if available
        return None

# safe_get remains the same...
def safe_get(data: Optional[Dict], keys: List[str], default: Any = None) -> Any:
    """
    Safely retrieves nested dictionary values. Returns default if any key is not found,
    the data is not a dict, or the value is None or NaN.
    (Implementation from previous response)
    """
    if not isinstance(data, dict):
        return default
    temp = data
    try:
        for key in keys:
            if temp is None: return default
            temp = temp.get(key)
        # Check for NaN specifically for floats after retrieving
        if isinstance(temp, float) and math.isnan(temp):
             return default
        return temp if temp is not None else default
    except Exception as e:
        log.warning(f"safe_get encountered unexpected issue accessing {keys}: {e}. Returning default.")
        return default

# find_district_data remains the same...
def find_district_data(data_list: Optional[List[Dict]], district_name_upper: str, name_key: str = "name") -> Optional[Dict]:
    """Finds the dictionary for a specific district (case-insensitive) in a list."""
    if not data_list:
        return None
    for item in data_list:
        item_name = safe_get(item, [name_key])
        if isinstance(item_name, str) and item_name.strip().upper() == district_name_upper:
            return item
    return None

# --- MODIFIED Function ---
def get_top_bottom_by_field(data_list: Optional[List[Dict]],
                            field_key: str,
                            nested_keys: Optional[List[str]] = None,
                            name_key: str = "name",
                            higher_is_better: bool = True) -> Dict[str, Optional[Dict]]:
    """
    Finds top and bottom performers based on a numeric field key (can be nested).
    Returns the full dictionary for the top and bottom items.

    Args:
        data_list: The list of dictionaries to search.
        field_key: The key of the numeric field to sort by.
        nested_keys: Optional list of keys to access the field_key if it's nested.
                     e.g., for field 'score' inside 'details', use ['details', 'score'].
        name_key: The key containing the name of the entity (e.g., 'name', 'district').
        higher_is_better: True if a higher value is better, False otherwise.

    Returns:
        A dictionary with 'top' and 'bottom' keys, each holding the full
        dictionary of the respective performer, or None if not found.
    """
    result = {"top": None, "bottom": None}
    if not data_list:
        return result

    valid_entries = []
    for item in data_list:
        # Construct the full key path
        key_path = (nested_keys or []) + [field_key]
        value = safe_get(item, key_path)
        name = safe_get(item, [name_key])

        # Ensure value is a valid number (not None, not NaN) and name exists
        if isinstance(value, (int, float)) and value == value and name:
             # Store the original item along with the value for sorting
             valid_entries.append({"item": item, "value": value})
        # else:
        #     log.debug(f"Skipping item for top/bottom: Invalid value '{value}' or missing name '{name}' for key path {key_path} in item {item}")


    if not valid_entries:
         return result

    # Sort based on the value and higher_is_better flag
    valid_entries.sort(key=lambda x: x["value"], reverse=higher_is_better)

    result["top"] = valid_entries[0]["item"]    # Return the full original item
    result["bottom"] = valid_entries[-1]["item"] # Return the full original item
    return result

# --- Convenience functions calling the generalized one ---

def get_top_bottom_performers_full(data_list: Optional[List[Dict]], score_key: str, name_key: str = "name") -> Dict[str, Optional[Dict]]:
    """Finds top/bottom based on score (higher is better), returns full data."""
    return get_top_bottom_by_field(data_list, field_key=score_key, name_key=name_key, higher_is_better=True)

def get_top_bottom_by_count_full(data_list: Optional[List[Dict]], count_key: str, name_key: str = "name") -> Dict[str, Optional[Dict]]:
    """Finds top/bottom based on count (higher is better), returns full data."""
    return get_top_bottom_by_field(data_list, field_key=count_key, name_key=name_key, higher_is_better=True)