# analyze_district_kpis.py
import argparse
import json
import logging
import os
import statistics  # Keep for mean/median
import math      # Keep for isnan/inf check
from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime, timedelta

# Use the functions from your utils.py
# Assuming utils.py has fetch_api_data, safe_get, get_top_bottom_by_field
try:
    from utils import fetch_api_data, safe_get, get_top_bottom_by_field
except ImportError:
    print("ERROR: utils.py not found or contains errors. Please ensure it's in the same directory.")
    # Define dummy functions to allow script to load partially for inspection
    def fetch_api_data(endpoint, params=None, base_url=None): return None
    def safe_get(data, keys, default=None): return default
    def get_top_bottom_by_field(data_list, field_key, name_key, higher_is_better): return {"top": None, "bottom": None}


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
log = logging.getLogger(__name__)

# --- Component Definitions ---
COMPONENTS_CONFIG = {
    "performance": {
        "endpoint": "/report_jsm/performance-marks",
        "results_key": "results",
        "name_key": "name",
        "target_marks_key": "target_marks",
        "payment_marks_key": "payment_marks",
        "old_work_completed_key": "_helper_total_old_work_completed", # Internal helper key
        "old_work_categories": [
            "Talab Nirman", "Check_Stop Dam", "Recharge Pit", "Koop Nirman",
            "Percolation Talab", "Khet Talab", "Other NRM Work",
        ],
        "use_date_param": True
    },
    "farm_ponds": {
        "endpoint": "/report_jsm/farm-ponds-marks",
        "results_key": "results",
        "name_key": "name",
        "marks_key": "marks",
        "count_key": "actual_count",
        "use_date_param": True
    },
    "dugwell": {
        "endpoint": "/report_jsm/dugwell-marks",
        "results_key": "results",
        "name_key": "name",
        "marks_key": "marks",
        "count_key": "actual_count",
        "use_date_param": True
    },
     "amrit_sarovar": {
        "endpoint": "/report_jsm/amritsarovar-stats",
        "results_key": "details",
        "name_key": "name",
        "marks_key": "marks",
        "count_key": "actual_count",
        "use_date_param": False # Amrit Sarovar API might not use date
    },
    "mybharat": {
        "endpoint": "/report_jsm/mybharat/gender-stats",
        "results_key": "districts_data",
        "name_key": "district",
        "marks_key": "marks",
        "count_key": "total_count",
        "target_key": "target", # If applicable
        "use_date_param": True
    }
}

# Key to store calculated total marks
TOTAL_MARKS_KEY = "total_marks"

# --- Data Fetching and Processing Helpers ---

def _calculate_old_work_completed(perf_data: Dict[str, Any]) -> int:
    """Helper to sum completed counts from performance categories."""
    total_completed = 0
    categories_data = safe_get(perf_data, ["categories"], {})
    if not isinstance(categories_data, dict):
        log.warning(f"Expected 'categories' to be a dict, but got {type(categories_data)}. Cannot calculate old work completed.")
        return 0
    old_work_cats = COMPONENTS_CONFIG["performance"]["old_work_categories"]
    for cat_name in old_work_cats:
        cat_info = categories_data.get(cat_name, {})
        completed_val = safe_get(cat_info, ["completed"], 0)
        try:
             # Handle potential None or non-numeric string before int conversion
             if completed_val is None: continue
             numeric_val = float(completed_val) # Use float first for broader compatibility
             if not math.isnan(numeric_val) and not math.isinf(numeric_val):
                 total_completed += int(numeric_val)
        except (ValueError, TypeError):
             log.warning(f"Could not convert 'completed' value '{completed_val}' to int for category '{cat_name}'. Skipping.")
    return total_completed

def _safe_convert(value, target_type, default):
    """Safely converts value to target_type, handling None, NaN, Inf."""
    if value is None: return default
    try:
        converted = target_type(value)
        # Check specifically for float NaN and Inf after conversion
        if target_type is float and (math.isnan(converted) or math.isinf(converted)):
            return default
        return converted
    except (ValueError, TypeError):
        log.debug(f"Conversion failed for value '{value}' to type {target_type}. Using default '{default}'.")
        return default

def _fetch_and_process_state_data_for_date(target_date: str) -> Dict[str, Any]:
    """
    Fetches state-level data for ALL components for a single date,
    processes it, calculates total marks, and combines it per district.
    Handles conditional date parameter sending and NaN/Inf values.
    """
    log.info(f"--- Processing ALL state data for date: {target_date} ---")
    processed_data_by_district: Dict[str, Dict[str, Any]] = {}
    fetch_errors: List[str] = []

    for comp_key, config in COMPONENTS_CONFIG.items():
        endpoint = config['endpoint']
        use_date = config.get('use_date_param', True)
        params = {'date': target_date} if use_date else {}
        log.debug(f"Calling fetch_api_data for {endpoint} with params: {params}")
        raw_data = fetch_api_data(endpoint, params=params) # fetch_api_data should handle its own errors/timeouts

        if not raw_data:
            msg = f"Failed to fetch data for component '{comp_key}' on {target_date} from {endpoint}."
            log.error(msg); fetch_errors.append(msg); continue

        results_list = safe_get(raw_data, [config['results_key']], [])
        if not isinstance(results_list, list):
             msg = f"Expected results key '{config['results_key']}' to contain a list for '{comp_key}', but got {type(results_list)}. Skipping component."
             log.error(msg); fetch_errors.append(msg); continue
        if not results_list:
             # This might be normal (e.g., no data for that day yet), log as warning
             msg = f"No results found in '{config['results_key']}' key for component '{comp_key}' on {target_date}."
             log.warning(msg); continue

        log.info(f"Processing {len(results_list)} entries for {comp_key} on {target_date}.")

        for item_data in results_list:
            if not isinstance(item_data, dict):
                 log.warning(f"Skipping non-dictionary item in {comp_key} results: {item_data}"); continue

            dist_name = safe_get(item_data, [config['name_key']])
            if not dist_name or not isinstance(dist_name, str):
                log.warning(f"Skipping item in {comp_key} due to missing/invalid name (key: {config['name_key']}): {item_data}"); continue
            dist_name = dist_name.strip().upper()
            if not dist_name:
                 log.warning(f"Skipping item in {comp_key} due to empty name after stripping: {item_data}"); continue

            if dist_name not in processed_data_by_district:
                processed_data_by_district[dist_name] = {"name": dist_name}

            component_entry = {}
            default_mark = 0.0; default_count = 0; default_target = 0

            # Use the refined _safe_convert helper
            if config.get("marks_key"):
                 marks = safe_get(item_data, [config['marks_key']])
                 component_entry['marks'] = _safe_convert(marks, float, default_mark)
            if config.get("count_key"):
                 count = safe_get(item_data, [config['count_key']])
                 component_entry['count'] = _safe_convert(count, int, default_count)
            if config.get("target_key"):
                 target = safe_get(item_data, [config['target_key']])
                 component_entry['target'] = _safe_convert(target, int, default_target)

            # Specific handling for performance component
            if comp_key == "performance":
                t_marks = safe_get(item_data, [config['target_marks_key']])
                p_marks = safe_get(item_data, [config['payment_marks_key']])
                component_entry['target_marks'] = _safe_convert(t_marks, float, default_mark)
                component_entry['payment_marks'] = _safe_convert(p_marks, float, default_mark)
                # Calculate and store helper key for Old Works completed count
                component_entry[config['old_work_completed_key']] = _calculate_old_work_completed(item_data)

            # Store the processed component data for the district
            processed_data_by_district[dist_name][comp_key] = component_entry

    log.info("Calculating total marks for all processed districts...")
    districts_with_marks_calculated = 0
    for dist_name, dist_data in processed_data_by_district.items():
        total_score = 0.0
        try:
            # Safely get marks, defaulting to 0.0 if component data is missing or marks are invalid
            perf_data = dist_data.get("performance", {})
            total_score += perf_data.get("target_marks", 0.0) + perf_data.get("payment_marks", 0.0)
            total_score += dist_data.get("farm_ponds", {}).get("marks", 0.0)
            total_score += dist_data.get("dugwell", {}).get("marks", 0.0)
            total_score += dist_data.get("amrit_sarovar", {}).get("marks", 0.0)
            total_score += dist_data.get("mybharat", {}).get("marks", 0.0)

            # Final check for NaN/Inf in total score
            if math.isnan(total_score) or math.isinf(total_score):
                 log.warning(f"Total score for {dist_name} resulted in NaN/Inf. Setting to 0.0.")
                 total_score = 0.0

            dist_data[TOTAL_MARKS_KEY] = round(total_score, 2)
            districts_with_marks_calculated += 1
        except Exception as e:
            # Log error but assign None to indicate calculation failure for this district
            log.error(f"Error calculating total marks for district {dist_name}: {e}", exc_info=True)
            dist_data[TOTAL_MARKS_KEY] = None # Indicate failure

    log.info(f"Finished calculating total marks. Successful calculations for {districts_with_marks_calculated} districts.")
    log.info(f"--- Finished processing state data for date: {target_date}. Districts processed: {len(processed_data_by_district)} ---")
    return {
        "date": target_date,
        "all_districts_processed": processed_data_by_district,
        "fetch_error": "; ".join(fetch_errors) if fetch_errors else None
    }

# --- Rank Calculation ---
def calculate_ranks(processed_state_data: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    """Calculates ranks based on total_marks, handling ties."""
    if not processed_state_data: return {}

    # Filter out entries where total_marks is None or NaN/Inf
    valid_entries = []
    for data in processed_state_data.values():
        marks = data.get(TOTAL_MARKS_KEY)
        if isinstance(marks, (int, float)) and not math.isnan(marks) and not math.isinf(marks):
            valid_entries.append(data)
        else:
             log.debug(f"Excluding district {data.get('name', 'Unknown')} from ranking due to invalid total_marks: {marks}")

    if not valid_entries:
        log.warning("No valid entries found for ranking.")
        return {}

    # Sort remaining valid districts by total_marks (descending)
    sorted_districts = sorted(valid_entries, key=lambda x: x.get(TOTAL_MARKS_KEY, -float('inf')), reverse=True)

    ranks = {}
    current_rank = 0
    last_score = -float('inf') # Initialize with a value lower than any possible score
    districts_at_rank = 0

    for i, district_data in enumerate(sorted_districts):
        dist_name = district_data.get("name")
        current_score = district_data.get(TOTAL_MARKS_KEY) # Already validated as numeric

        if current_score != last_score:
            current_rank += districts_at_rank # Advance rank by the number of tied districts
            districts_at_rank = 1 # Reset count for the new rank
            last_score = current_score
        else:
            districts_at_rank += 1 # Increase count for tied rank

        if dist_name:
            ranks[dist_name] = current_rank + 1 # Assign rank (1-based)
        else:
            # Should not happen if name checks in fetch were robust, but log just in case
            log.warning(f"District data found without name during ranking: {district_data}")

    log.info(f"Calculated ranks for {len(ranks)} districts (handling ties).")
    return ranks

# --- Value Extraction Helpers ---
# MODIFIED _get_kpi_value
def _get_kpi_value(district_data: Optional[Dict], component: str, key: Optional[str], default: Any = None) -> Any: # Added Optional[str] for key
    """Safely extracts a specific KPI value, handling None, NaN, Inf."""
    if not isinstance(district_data, dict): return default

    value = None
    if component == TOTAL_MARKS_KEY:
        value = district_data.get(TOTAL_MARKS_KEY)
    else:
        component_data = district_data.get(component)
        if isinstance(component_data, dict):
            # Ensure key is not None before accessing component data with it
            if key is not None:
                 value = component_data.get(key)
            else:
                 # Handle cases where key might be legitimately None if component is not TOTAL_MARKS_KEY
                 # This shouldn't happen with current usage, but safer to handle.
                 log.warning(f"Key is None when trying to get value for component '{component}' (not TOTAL_MARKS_KEY).")
                 return default # Or handle as appropriate

    # --- FIX IS HERE ---
    # Determine target type based on common usage (marks=float, count/target=int)
    # Handle the case where key is None (specifically for TOTAL_MARKS_KEY)
    if key is None and component == TOTAL_MARKS_KEY:
        target_type = float # Total marks are float
    elif key is not None: # Proceed only if key is not None
        # Check if 'marks' is in the key name to decide the type
        target_type = float if 'marks' in key.lower() else int
    else:
        # Fallback or error if key is None and component isn't TOTAL_MARKS_KEY
        log.error(f"Unexpected None key for component '{component}' in _get_kpi_value.")
        return default # Return default if key logic fails
    # --- END FIX ---

    # Use default value specific to the type if needed
    type_default = 0.0 if target_type is float else 0

    # Pass the determined type_default to _safe_convert
    converted_value = _safe_convert(value, target_type, type_default)

    # If the original default was None, return None if conversion resulted in the type_default
    # This preserves the intention of returning None if the value was truly absent/invalid
    if default is None and converted_value == type_default and value is None:
         return None
    elif default is None and converted_value == type_default:
         # If default was None but conversion yielded 0/0.0 due to invalid input,
         # return the 0/0.0 for calculation safety.
         return converted_value
    else:
         # If a specific default was requested, return the converted value or that default
         # If value was None initially, conversion resulted in type_default, return original default
         return converted_value if value is not None else default


def _calculate_change(current_val: Optional[Any], previous_val: Optional[Any]) -> Optional[Union[int, float]]:
    """Calculates numeric change, handling None, NaN, Inf."""
    # Use _safe_convert to ensure we are comparing valid numbers
    num_current = _safe_convert(current_val, float, None) # Convert to float for comparison, default to None if invalid
    num_previous = _safe_convert(previous_val, float, None)

    if num_current is None or num_previous is None:
        return None

    change = num_current - num_previous

    # Check if the change resulted in NaN or Inf (though unlikely with prior checks)
    if math.isnan(change) or math.isinf(change):
        return None

    # Return int if change is whole number, else round float
    if change == int(change):
        return int(change)
    else:
        return round(change, 2)

def _calculate_rank_change(current_rank: Optional[int], previous_rank: Optional[int]) -> Optional[int]:
    """Calculates rank change (lower is better). Handles None."""
    if current_rank is None or previous_rank is None: return None
    # Ranks should already be ints, but cast just in case
    try:
        # Rank change: Positive means improvement (e.g., rank 5 -> 3, change = 5-3 = 2)
        return int(previous_rank) - int(current_rank)
    except (ValueError, TypeError):
        return None

# --- NEW Helper: Calculate Avg/Median ---
def _calculate_stats(data_list: List[Union[int, float]]) -> Dict[str, Optional[float]]:
    """Calculates average and median for a list of valid numbers."""
    # Filter out non-numeric or problematic values first (redundant if input is clean, but safer)
    valid_data = [x for x in data_list if isinstance(x, (int, float)) and not math.isnan(x) and not math.isinf(x)]

    stats = {"average": None, "median": None, "count": len(valid_data)}
    if not valid_data:
        log.debug("No valid data provided for statistics calculation.")
        return stats

    try:
        stats["average"] = round(statistics.mean(valid_data), 2)
    except statistics.StatisticsError:
        log.debug("Not enough data for mean calculation (needs >= 1).")
    except Exception as e:
        log.warning(f"Error calculating mean: {e}")

    try:
        stats["median"] = round(statistics.median(valid_data), 2)
    except statistics.StatisticsError:
        log.debug("Not enough data for median calculation (needs >= 1).")
    except Exception as e:
        log.warning(f"Error calculating median: {e}")

    return stats

# --- NEW Helper: Simplify Performer Data ---
def _extract_performer_summary(district_data: Optional[Dict]) -> Optional[Dict[str, Any]]:
    """Extracts just name and total_marks if valid."""
    if not isinstance(district_data, dict):
        return None
    name = district_data.get("name")
    score = district_data.get(TOTAL_MARKS_KEY)
    # Ensure score is valid before returning
    if name and isinstance(score, (int, float)) and not math.isnan(score) and not math.isinf(score):
        return {"name": name, "score": score} # Return raw valid score
    log.debug(f"Could not extract valid performer summary from: {district_data}")
    return None

# --- Main Analysis Function (MODIFIED with State Context Calculation) ---
def analyze(district_name: str, report_date_str: str) -> Optional[Dict[str, Any]]:
    """
    Analyzes overall District KPIs, compares with previous day, and includes
    calculated state context (top/bottom by total marks, avg/median for total & components).
    """
    if not district_name or not report_date_str:
        log.error("District name and report date are required."); return None
    district_name_upper = district_name.strip().upper()

    try:
        report_date_obj = datetime.strptime(report_date_str, "%Y-%m-%d").date()
        previous_date_obj = report_date_obj - timedelta(days=1)
        previous_date_str = previous_date_obj.strftime("%Y-%m-%d")
    except ValueError:
        log.error(f"Invalid date format: {report_date_str}. Use YYYY-MM-DD."); return None

    log.info(f"Starting analysis for district '{district_name_upper}' on {report_date_str} vs {previous_date_str}")

    # Fetch data for both dates
    current_state_data = _fetch_and_process_state_data_for_date(report_date_str)
    previous_state_data = _fetch_and_process_state_data_for_date(previous_date_str)

    current_processed_map = current_state_data.get("all_districts_processed", {})
    previous_processed_map = previous_state_data.get("all_districts_processed", {})

    # Calculate ranks *after* processing data for both dates
    current_ranks = calculate_ranks(current_processed_map)
    previous_ranks = calculate_ranks(previous_processed_map)

    # Get data for the specific district
    current_district_data = current_processed_map.get(district_name_upper)
    previous_district_data = previous_processed_map.get(district_name_upper)

    # --- Calculate State-Level Context for Current Date ---
    log.info(f"Calculating state context for {report_date_str}...")
    state_context_data = {
        "report_date": report_date_str,
        "total_marks_stats": {
            "top_performer": None,      # Will store simplified {name, score}
            "bottom_performer": None,   # Will store simplified {name, score}
            "average": None,
            "median": None,
            "count_valid_districts": 0,
        },
        "component_stats": { # Structure for avg/median marks per component
            "performance_target": {"average": None, "median": None, "count": 0},
            "performance_payment": {"average": None, "median": None, "count": 0},
            "farm_ponds": {"average": None, "median": None, "count": 0},
            "dugwell": {"average": None, "median": None, "count": 0},
            "amrit_sarovar": {"average": None, "median": None, "count": 0},
            "mybharat": {"average": None, "median": None, "count": 0},
        }
    }

    if current_processed_map:
        # Use list comprehension for cleaner extraction of valid district data
        current_district_list = [
            d for d in current_processed_map.values()
            if isinstance(d.get(TOTAL_MARKS_KEY), (int, float)) and not math.isnan(d.get(TOTAL_MARKS_KEY)) and not math.isinf(d.get(TOTAL_MARKS_KEY))
        ]

        if current_district_list:
            # 1. Find Top/Bottom by Total Marks (Get Full Data first)
            # Ensure utils function handles empty list gracefully
            top_bottom_full = get_top_bottom_by_field(
                data_list=current_district_list, field_key=TOTAL_MARKS_KEY, name_key="name", higher_is_better=True
            )

            # Extract Simplified Summary using helper
            state_context_data["total_marks_stats"]["top_performer"] = _extract_performer_summary(top_bottom_full.get("top"))
            state_context_data["total_marks_stats"]["bottom_performer"] = _extract_performer_summary(top_bottom_full.get("bottom"))
            log.info(f"State Top Performer (Total Marks): {state_context_data['total_marks_stats']['top_performer']}")
            log.info(f"State Bottom Performer (Total Marks): {state_context_data['total_marks_stats']['bottom_performer']}")

            # 2. Calculate State Avg/Median for Total Marks
            valid_total_marks = [d[TOTAL_MARKS_KEY] for d in current_district_list] # Already filtered
            total_marks_stats = _calculate_stats(valid_total_marks)
            state_context_data["total_marks_stats"]["average"] = total_marks_stats["average"]
            state_context_data["total_marks_stats"]["median"] = total_marks_stats["median"]
            state_context_data["total_marks_stats"]["count_valid_districts"] = total_marks_stats["count"]
            log.info(f"State Total Marks - Avg: {total_marks_stats['average']}, Median: {total_marks_stats['median']} (from {total_marks_stats['count']} districts)")

            # 3. Calculate State Avg/Median for Each Component's Marks
            component_marks_lists = {
                "performance_target": [], "performance_payment": [], "farm_ponds": [],
                "dugwell": [], "amrit_sarovar": [], "mybharat": [],
            }
            # Use the safer _get_kpi_value to extract component marks
            for d_data in current_district_list: # Iterate through valid districts
                # Performance component parts
                perf_target = _get_kpi_value(d_data, "performance", "target_marks", default=None)
                if perf_target is not None: component_marks_lists["performance_target"].append(perf_target)
                perf_payment = _get_kpi_value(d_data, "performance", "payment_marks", default=None)
                if perf_payment is not None: component_marks_lists["performance_payment"].append(perf_payment)

                # Other components
                for comp_key, comp_stat_key in [
                    ("farm_ponds", "farm_ponds"), ("dugwell", "dugwell"),
                    ("amrit_sarovar", "amrit_sarovar"), ("mybharat", "mybharat")
                ]:
                    comp_marks = _get_kpi_value(d_data, comp_key, "marks", default=None)
                    if comp_marks is not None: component_marks_lists[comp_stat_key].append(comp_marks)

            # Calculate stats for each component list using helper
            for comp_stat_key, marks_list in component_marks_lists.items():
                stats = _calculate_stats(marks_list)
                state_context_data["component_stats"][comp_stat_key] = stats # Store dict {average, median, count}
                log.info(f"State {comp_stat_key.replace('_', ' ').title()} Marks - Avg: {stats['average']}, Median: {stats['median']} (from {stats['count']} districts)")
        else:
             log.warning("No districts with valid total marks found for state context calculation.")
    else:
        log.warning("No current processed data available to calculate state context.")


    # --- Build Final Analysis Result ---
    analysis_result: Dict[str, Any] = {
        "district_name": district_name_upper,
        "report_date": report_date_str,
        "previous_report_date": previous_date_str,
        "kpis": {}, # To be populated below
        "state_context": state_context_data, # Includes calculated state stats
        "fetch_errors": {
            "current": current_state_data.get("fetch_error"),
            "previous": previous_state_data.get("fetch_error"),
        },
        "notes": []
    }

    kpis = analysis_result["kpis"]
    log.info(f"Populating KPIs for district: {district_name_upper}")

    # Get Rank KPI
    current_rank = current_ranks.get(district_name_upper)
    previous_rank = previous_ranks.get(district_name_upper)
    kpis["rank"] = {
        "current": current_rank,
        "previous": previous_rank,
        "change": _calculate_rank_change(current_rank, previous_rank),
        "total_districts_ranked_today": state_context_data["total_marks_stats"]["count_valid_districts"] # Use count from stats
    }

    # Get Total Marks KPI
    current_total_marks = _get_kpi_value(current_district_data, TOTAL_MARKS_KEY, None, default=None) # Use correct call signature
    previous_total_marks = _get_kpi_value(previous_district_data, TOTAL_MARKS_KEY, None, default=None) # Use correct call signature
    kpis["total_marks"] = {
        "current": current_total_marks,
        "previous": previous_total_marks,
        "change": _calculate_change(current_total_marks, previous_total_marks)
    }

    # Get Individual Component KPIs (Counts)
    kpi_configs = [
        # component_key_in_data, data_field_to_extract, kpi_key_in_output
        ("farm_ponds", "count", "farm_ponds_completed"),
        ("dugwell", "count", "dugwell_recharge_completed"),
        ("amrit_sarovar", "count", "amrit_sarovar_completed"),
        # Special case for old work completed count (using the helper key)
        ("performance", COMPONENTS_CONFIG["performance"]["old_work_completed_key"], "old_work_completed"),
        ("mybharat", "count", "mybharat_completed"),
    ]
    for comp, key, kpi_key in kpi_configs:
        curr_val = _get_kpi_value(current_district_data, comp, key, default=None)
        prev_val = _get_kpi_value(previous_district_data, comp, key, default=None)
        kpis[kpi_key] = {
            "current": curr_val,
            "previous": prev_val,
            "change": _calculate_change(curr_val, prev_val)
        }

    # Check for missing district data notes
    if current_district_data is None:
        analysis_result["notes"].append(f"Data for {district_name_upper} missing for current date {report_date_str}.")
    if previous_district_data is None:
        analysis_result["notes"].append(f"Data for {district_name_upper} missing for previous date {previous_date_str}.")

    # --- Generate Explanation ---
    analysis_result["explanation"] = generate_simplified_explanation(analysis_result)
    log.info(f"Finished populating KPIs and explanation for {district_name_upper}.")

    return analysis_result

# --- Simplified Explanation Function (Updated for simplified top/bottom) ---
def generate_simplified_explanation(result: Dict[str, Any]) -> str:
    """Generates narrative using calculated state context."""
    parts = []
    dist_name = result["district_name"]
    curr_date = result["report_date"]
    prev_date = result["previous_report_date"]
    kpis = result.get("kpis", {})
    # Access the calculated state stats
    state_total_stats = result.get("state_context", {}).get("total_marks_stats", {})
    # state_component_stats = result.get("state_context", {}).get("component_stats", {}) # Available if needed

    rank_info = kpis.get("rank", {})
    marks_info = kpis.get("total_marks", {})

    # --- District Summary ---
    curr_rank = rank_info.get("current")
    total_ranked = rank_info.get("total_districts_ranked_today", 0) # Default to 0
    rank_str = f"for {dist_name} on {curr_date}: "
    if curr_rank is not None and total_ranked > 0:
        rank_str += f"Rank {curr_rank}/{total_ranked}."
        rank_change = rank_info.get("change")
        prev_rank = rank_info.get("previous")
        if rank_change is not None and prev_rank is not None:
            # Use rank_change directly: Positive is improvement, Negative is decline
            change_desc = f"Improved by {rank_change}" if rank_change > 0 else (f"Declined by {abs(rank_change)}" if rank_change < 0 else "No change")
            rank_str += f" ({change_desc} from rank {prev_rank} on {prev_date})."
        elif prev_rank is None:
             rank_str += f" (Previous rank on {prev_date} unavailable)."
    else:
        rank_str += "Rank unavailable."
    parts.append(rank_str)

    curr_marks = marks_info.get("current")
    mark_str = ""
    if curr_marks is not None:
        mark_str = f"Total Marks: {curr_marks:.2f}."
        marks_change = marks_info.get("change")
        if marks_change is not None:
            mark_str += f" Change vs {prev_date}: {marks_change:+.2f}."
        else:
            mark_str += f" Comparison vs {prev_date} unavailable."
    else:
        mark_str = "Total marks unavailable."
    parts.append(mark_str)

    # --- State Context Summary ---
    state_top = state_total_stats.get("top_performer") # {name, score} or None
    state_bottom = state_total_stats.get("bottom_performer") # {name, score} or None
    state_avg = state_total_stats.get("average")
    state_median = state_total_stats.get("median")
    state_count = state_total_stats.get("count_valid_districts", 0)

    if state_count > 0:
        state_parts = [f"State Context ({state_count} districts):"]
        if state_top: state_parts.append(f"Highest: {state_top.get('score'):.2f} ({state_top.get('name')})")
        if state_bottom and state_bottom != state_top:
            state_parts.append(f"Lowest: {state_bottom.get('score'):.2f} ({state_bottom.get('name')})")
        if state_avg is not None: state_parts.append(f"Average: {state_avg:.2f}")
        if state_median is not None: state_parts.append(f"Median: {state_median:.2f}")
        if len(state_parts) > 1: # Only add if we have actual stats
             parts.append(" ".join(state_parts) + ".")
        else:
             parts.append(f"Partial state context available for {curr_date}.") # If only count is there
    else:
        parts.append(f"Could not determine state-wide performance context for {curr_date}.")

    # --- Individual KPI Changes ---
    # (Keep this section as it was - it correctly shows changes)
    parts.append("Progress vs Previous Day:")
    def format_kpi_change(kpi_dict_key, kpi_name):
        info = kpis.get(kpi_dict_key, {})
        curr_val = info.get("current"); change = info.get("change")
        if curr_val is not None:
            try: # Format as int with commas if possible
                val_str = f"{kpi_name}: {int(curr_val):,}"
            except (ValueError, TypeError): # Fallback to string
                val_str = f"{kpi_name}: {curr_val}"

            if change is not None and change != 0:
                try: # Format change as int with sign/commas
                     change_str = f"{int(change):+,}"
                except (ValueError, TypeError): # Fallback to float
                     change_str = f"{change:+.2f}"
                val_str += f" ({change_str})" # Simplified change display
            elif change == 0: val_str += " (No change)"
            # No change info if previous data was missing
            return val_str + "."
        return None # Return None if current value is missing

    kpi_keys_names = [
        ("farm_ponds_completed", "Farm Ponds"),
        ("dugwell_recharge_completed", "Dugwell Recharge"),
        ("amrit_sarovar_completed", "Amrit Sarovar"),
        ("old_work_completed", "Old Work (Completed)"), # Clarify it's completed count
        ("mybharat_completed", "MyBharat (Jaldoot)"),
    ]
    change_notes = []
    for key, name in kpi_keys_names:
        kpi_str = format_kpi_change(key, name)
        if kpi_str: change_notes.append(kpi_str)

    if change_notes: parts.extend(change_notes)
    else: parts.append("No component change data available.")

    # --- Notes & Errors ---
    fetch_err = result.get("fetch_errors", {})
    error_notes = []
    if fetch_err.get("current"): error_notes.append(f"current date ({curr_date})")
    if fetch_err.get("previous"): error_notes.append(f"previous date ({prev_date})")
    if error_notes: parts.append(f"Note: Fetch errors occurred for { ' and '.join(error_notes) } which may affect results.")
    if result.get("notes"): parts.append("Data Notes: " + "; ".join(result["notes"]))

    return " ".join(parts)


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze District JSM KPIs with State Context (Total & Component Stats).")
    parser.add_argument("-d", "--district", required=True, help="District name.")
    parser.add_argument("-dt", "--date", required=True, help="Report date (YYYY-MM-DD).")
    parser.add_argument("-o", "--output", help="Optional: File path for JSON output.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")

    args = parser.parse_args()

    # Set logging level
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.getLogger().setLevel(log_level) # Set root logger level
    log.setLevel(log_level) # Set this script's logger level
    # Attempt to set level for utils logger
    try:
         utils_logger = logging.getLogger('utils') # Assuming logger name in utils.py
         if utils_logger: utils_logger.setLevel(log_level)
         log.debug(f"Set utils logger level to {logging.getLevelName(log_level)}")
    except Exception as e:
         log.warning(f"Could not set logging level for 'utils' module: {e}")

    log.info(f"Starting Overall KPI analysis for District: {args.district}, Date: {args.date}")

    result = analyze(args.district, args.date)
    output_json = {}

    if result:
        output_json = result
        log.info("Analysis complete.")
        # Add warnings based on results
        if result.get("fetch_errors", {}).get("current") or result.get("fetch_errors", {}).get("previous"):
            log.warning("Analysis finished, but encountered data fetch errors. Results might be incomplete.")
        if result.get("notes"):
             log.warning(f"Analysis finished with notes: {'; '.join(result.get('notes', []))}")
    else:
        # Create a more informative error structure if analyze fails
        log.error("Analysis failed to produce a result structure.")
        try: prev_date = (datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")
        except: prev_date = "N/A"
        output_json = {
            "district_name": args.district.strip().upper() if args.district else "Unknown",
            "report_date": args.date,
            "previous_report_date": prev_date,
            "error": "Failed to generate analysis structure. Critical error during analysis. Check logs.",
            "explanation": "Analysis could not be completed due to critical errors.",
            "kpis": {},
            "state_context": {}, # Empty context on failure
            "fetch_errors": {"current": "Analysis failed", "previous": "Analysis failed"},
            "notes": ["Analysis function returned None or raised an exception"]
        }

    # Output the result
    # Use ensure_ascii=False for proper Hindi rendering in JSON output if needed
    output_string = json.dumps(output_json, indent=2, default=str, ensure_ascii=False)

    if args.output:
        try:
            output_dir = os.path.dirname(args.output)
            if output_dir: os.makedirs(output_dir, exist_ok=True) # Ensure directory exists
            # Write with utf-8 encoding
            with open(args.output, 'w', encoding='utf-8') as f:
                 f.write(output_string)
            log.info(f"Output saved to {args.output}")
        except Exception as e:
            log.error(f"Error saving output to file {args.output}: {e}", exc_info=True)
            # Print to console if saving fails
            print("\n--- JSON Output (Error saving file) ---")
            print(output_string)
            print("--- End JSON Output ---")
    else:
        # Print directly to console, ensuring terminal supports UTF-8
        try:
             print(output_string)
        except UnicodeEncodeError:
             log.warning("Terminal might not support UTF-8 fully. JSON printed with default encoding.")
             print(json.dumps(output_json, indent=2, default=str)) # Fallback without ensure_ascii=False