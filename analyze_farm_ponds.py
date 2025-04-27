# analyze_farm_ponds.py
import argparse
import json
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import statistics # Import statistics module
import math # For checking isnan

# Use the modified utils functions
from utils import (fetch_api_data, safe_get, find_district_data,
                   get_top_bottom_performers_full, get_top_bottom_by_count_full)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
log = logging.getLogger(__name__)

COMPONENT_NAME = "Farm Ponds"
API_ENDPOINT = "/report_jsm/farm-ponds-marks" # Updated API Endpoint
SCORE_KEY = "marks"
COUNT_KEY = "actual_count" # Field representing the count for this component
NAME_KEY = "name"
MAX_MARKS = 30.0 # Updated Max Marks

# --- process_component_data (Using robust version from dugwell) ---
def process_component_data(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Processes a single district/block/panchayat entry from the component API response."""
    if not data:
        return None
    # Ensure consistent data types, especially for comparison
    try:
        score = float(safe_get(data, [SCORE_KEY], 0.0))
    except (ValueError, TypeError):
        score = 0.0 # Default score
    try:
        count = int(safe_get(data, [COUNT_KEY], 0))
    except (ValueError, TypeError):
        count = 0 # Default count
    try:
        # Keep target as is if not convertible or missing
        target_raw = safe_get(data, ["target"])
        target = int(target_raw) if target_raw is not None else "N/A"
    except (ValueError, TypeError):
        target = "N/A" # Keep N/A if conversion fails

    # Process achievement percentage carefully (can be 'inf')
    ach_perc_raw = safe_get(data, ["achievement_percentage"])
    ach_perc_processed = "N/A"
    if isinstance(ach_perc_raw, (int, float)):
        # Check for infinity explicitly before rounding
        if math.isinf(ach_perc_raw):
            ach_perc_processed = 'Inf'
        else:
            ach_perc_processed = round(ach_perc_raw, 2)
    elif isinstance(ach_perc_raw, str):
        # Handle potential string representations like 'inf' or numbers as strings
        try:
            num_val = float(ach_perc_raw)
            if math.isinf(num_val):
                 ach_perc_processed = 'Inf'
            else:
                 ach_perc_processed = round(num_val, 2)
        except ValueError:
             # Keep as string if it's not 'inf' or a number
             if ach_perc_raw.strip().lower() == 'inf':
                 ach_perc_processed = 'Inf'
             else:
                # Or keep original string if it's something else? For now default to N/A
                # ach_perc_processed = ach_perc_raw
                ach_perc_processed = "N/A"


    # Minimal set of useful fields, add others if needed by downstream processing
    return {
        "name": safe_get(data, [NAME_KEY]),
        COUNT_KEY: count,
        SCORE_KEY: round(score, 2), # Round score here
        "target": target,
        "achievement_percentage": ach_perc_processed, # Use processed value
    }

# --- _fetch_and_process_data_for_date (Generic, adapted from dugwell) ---
def _fetch_and_process_data_for_date(district_name: str, target_date: str) -> Dict[str, Any]:
    """
    Fetches and processes data (state, district, block, panchayat) for a single date.
    Uses the globally defined API_ENDPOINT and COMPONENT_NAME.
    Returns a dictionary containing processed data. Indicates errors within the dict.
    """
    log.info(f"--- Processing {COMPONENT_NAME} data for date: {target_date} ---")
    district_name_upper = district_name.strip().upper()
    date_analysis: Dict[str, Any] = {
        "date": target_date,
        "state_results_processed": [], # List of processed district data
        "district_data": None,        # Processed data for the selected district
        "block_level_data": [],       # List of processed block data with top 5 panchayats
        "fetch_error": None           # Store fetch/processing errors for this date
    }
    fetch_errors = []

    # 1. Fetch State-Level Data
    log.info(f"Fetching state-level {COMPONENT_NAME} data for {target_date}...")
    state_params = {'date': target_date}
    # *** Uses the global API_ENDPOINT ***
    state_data_raw = fetch_api_data(API_ENDPOINT, params=state_params)
    state_results_raw = safe_get(state_data_raw, ["results"], [])

    if not state_results_raw:
        msg = f"Could not fetch or parse state-level {COMPONENT_NAME} data for {target_date} from {API_ENDPOINT}."
        log.error(msg)
        fetch_errors.append(msg)
    else:
        log.info(f"Fetched {len(state_results_raw)} district results for {COMPONENT_NAME} state-level on {target_date}.")
        # Process all state results
        for district_raw in state_results_raw:
            processed = process_component_data(district_raw)
            if processed:
                date_analysis["state_results_processed"].append(processed)

    # 2. Extract Selected District's Data from Processed State Results
    selected_district_state_data = next((d for d in date_analysis["state_results_processed"] if d.get("name") == district_name_upper), None)

    if not selected_district_state_data:
         raw_dist_found = find_district_data(state_results_raw, district_name_upper, name_key=NAME_KEY)
         if raw_dist_found:
              msg = f"Data for selected district '{district_name}' found raw but failed processing for {target_date}."
              log.warning(msg)
              fetch_errors.append(msg)
         elif state_results_raw:
             msg = f"Data for selected district '{district_name}' not found in state-level {COMPONENT_NAME} results for {target_date}."
             log.warning(msg)
    else:
        log.info(f"Found and processed state-level {COMPONENT_NAME} data for selected district: {district_name} on {target_date}")
        date_analysis["district_data"] = selected_district_state_data

    # 3. Fetch Block-Level Data & Top Panchayats
    log.info(f"Fetching block-level {COMPONENT_NAME} data for district: {district_name} on {target_date}")
    district_params = {'district': district_name, 'date': target_date}
     # *** Uses the global API_ENDPOINT ***
    block_data_raw = fetch_api_data(API_ENDPOINT, params=district_params)
    block_results_raw = safe_get(block_data_raw, ["results"], [])

    if not block_results_raw:
         msg = f"Could not fetch block list/data for {district_name} ({COMPONENT_NAME}) on {target_date} from {API_ENDPOINT}."
         log.warning(msg)
         date_analysis["block_level_data"] = []
    else:
        log.info(f"Found {len(block_results_raw)} blocks in {district_name} for {COMPONENT_NAME} on {target_date}. Processing...")
        processed_blocks = []
        for block_data in block_results_raw:
            block_processed = process_component_data(block_data)
            if not block_processed or not block_processed.get("name"):
                log.warning(f"Skipping block due to missing name or processing error: {block_data}")
                continue
            block_name = block_processed["name"]
            log.debug(f"Processing block: {block_name} for date {target_date}")

            block_info = {
                "name": block_name,
                COUNT_KEY: block_processed[COUNT_KEY],
                SCORE_KEY: block_processed[SCORE_KEY], # Keep block score
                "top_5_panchayats": []
            }

            panchayat_params = {'district': district_name, 'block': block_name, 'date': target_date}
             # *** Uses the global API_ENDPOINT ***
            panchayat_data_raw = fetch_api_data(API_ENDPOINT, params=panchayat_params)
            panchayat_results_raw = safe_get(panchayat_data_raw, ["results"], [])

            panchayat_processed_list = []
            if panchayat_results_raw:
                for panchayat in panchayat_results_raw:
                    panchayat_processed = process_component_data(panchayat)
                    if panchayat_processed and panchayat_processed.get("name"):
                        panchayat_processed_list.append({
                            "name": panchayat_processed["name"],
                            COUNT_KEY: panchayat_processed[COUNT_KEY]
                        })

                panchayat_processed_list.sort(key=lambda x: x[COUNT_KEY], reverse=True)
                block_info["top_5_panchayats"] = panchayat_processed_list[:5]
            else:
                log.warning(f"No panchayat data found for block '{block_name}' on {target_date} using {API_ENDPOINT}.")

            processed_blocks.append(block_info)

        processed_blocks.sort(key=lambda x: x[COUNT_KEY], reverse=True)
        date_analysis["block_level_data"] = processed_blocks

    if fetch_errors:
        date_analysis["fetch_error"] = "; ".join(fetch_errors)

    log.info(f"--- Finished processing {COMPONENT_NAME} data for date: {target_date} ---")
    return date_analysis


# --- generate_simplified_explanation (Generic, adapted from dugwell) ---
def generate_simplified_explanation(result: Dict[str, Any]) -> str:
    """Generates a narrative explanation based on the simplified analysis result including state stats."""
    parts = []
    # Use component name and max marks from the result dict for flexibility
    comp_name = result.get("component", "Component")
    max_marks_local = result.get("max_marks", 0.0) # Get max marks from result if passed
    dist_name = result["selected_district"]
    curr_date = result["report_date"]
    prev_date = result["previous_report_date"]

    dist_comp = result.get("selected_district_comparison", {})
    curr_dist_data = dist_comp.get("current_data")
    prev_dist_data = dist_comp.get("previous_data")
    dist_change = dist_comp.get("change", {})

    state_summary = result.get("state_level_summary_today", {})
    blocks_comp = result.get("block_level_comparison", [])
    state_stats = result.get("state_statistics_today", {})
    dist_pos = result.get("selected_district_position_vs_state", {})


    # Part 1: Selected District Performance and Change
    if curr_dist_data:
        score = format(safe_get(curr_dist_data, [SCORE_KEY], 0.0), '.2f')
        actual = format(safe_get(curr_dist_data, [COUNT_KEY], 0), ',')
        target = format(safe_get(curr_dist_data, ["target"], 0), ',') if safe_get(curr_dist_data, ["target"], "N/A") != "N/A" else "N/A"
        parts.append(f"On {curr_date}, for {comp_name}, {dist_name} reported {actual} units (Target: {target}), scoring {score}/{max_marks_local:.0f}.") # Use local max_marks

        if dist_change and prev_dist_data:
            score_delta = dist_change.get("score_change", 0.0)
            count_delta = dist_change.get("count_change", 0)
            change_desc = []
            if score_delta != 0.0: change_desc.append(f"score changed by {score_delta:+.2f} points")
            else: change_desc.append("score remained the same")

            if count_delta != 0: change_desc.append(f"count changed by {count_delta:+,}")
            else: change_desc.append("count remained the same")

            parts.append(f"Compared to {prev_date}, the { ' and the '.join(change_desc) }.")
        elif prev_dist_data is None and not result.get("previous_analysis_error"):
             parts.append(f"Data for the previous day ({prev_date}) was not available for comparison for {dist_name}.")
        elif result.get("previous_analysis_error"):
             parts.append(f"Could not retrieve comparison data for {dist_name} from {prev_date} due to an error.")

    else:
        parts.append(f"Could not retrieve specific {comp_name} performance data for {dist_name} on {curr_date}.")
        if result.get("current_analysis_error"):
             parts.append(f"(Error fetching current data: {result['current_analysis_error']})")


    # Part 2: Block Level Summary
    if blocks_comp:
         num_blocks = len(blocks_comp)
         block_with_prev_count = sum(1 for b in blocks_comp if b.get("actual_count_daybefore") is not None and b.get("actual_count_daybefore") != "N/A")
         parts.append(f"Block-level data for {num_blocks} blocks within {dist_name} is included.")
         if block_with_prev_count > 0:
              parts.append(f"Counts for today ({curr_date}) and the previous day ({prev_date}) are shown ({block_with_prev_count}/{num_blocks} blocks had previous day data).")
         else:
              parts.append(f"Previous day ({prev_date}) block counts were not available for comparison.")
         parts.append("Top 5 panchayats by count (as of today) are listed for each block.")
    elif not result.get("current_analysis_error"):
         parts.append(f"Block-level breakdown for {comp_name} in {dist_name} could not be retrieved for {curr_date}.")


    # Part 3: State Comparison Summary & Statistics for Current Date
    num_dist_reporting = state_stats.get("districts_reporting", 0)
    if num_dist_reporting > 0:
         parts.append(f"Across the state ({num_dist_reporting} districts reporting on {curr_date}):")
         # Top/Bottom
         top_score = state_summary.get('by_score', {}).get('top_performer')
         bot_score = state_summary.get('by_score', {}).get('bottom_performer')
         if top_score and bot_score:
             parts.append(f"- Top performer by Score: {top_score['name']} ({top_score[SCORE_KEY]:.2f}). Bottom: {bot_score['name']} ({bot_score[SCORE_KEY]:.2f}).")
         else:
             parts.append("- Top/Bottom performers by SCORE could not be fully determined.")

         top_count = state_summary.get('by_count', {}).get('top_performer')
         bot_count = state_summary.get('by_count', {}).get('bottom_performer')
         if top_count and bot_count:
             parts.append(f"- Top performer by Count: {top_count['name']} ({top_count[COUNT_KEY]:,}). Bottom: {bot_count['name']} ({bot_count[COUNT_KEY]:,}).")
         else:
              parts.append("- Top/Bottom districts by COUNT could not be fully determined.")

         # State Statistics
         mean_score = state_stats.get("score", {}).get("mean")
         median_score = state_stats.get("score", {}).get("median")
         mean_count = state_stats.get("count", {}).get("mean")
         median_count = state_stats.get("count", {}).get("median")

         stat_parts = []
         if mean_score is not None: stat_parts.append(f"Mean Score: {mean_score:.2f}")
         if median_score is not None: stat_parts.append(f"Median Score: {median_score:.2f}")
         if mean_count is not None: stat_parts.append(f"Mean Count: {mean_count:.2f}")
         if median_count is not None: stat_parts.append(f"Median Count: {median_count:,.0f}")

         if stat_parts:
              parts.append(f"- State Statistics: {'; '.join(stat_parts)}.")
         else:
              parts.append("- State descriptive statistics could not be calculated.")

         # District position vs State
         score_pos = dist_pos.get("score_comparison")
         count_pos = dist_pos.get("count_comparison")
         if curr_dist_data and score_pos and count_pos and "missing" not in score_pos:
             parts.append(f"- {dist_name}'s position: Score is {score_pos}; Count is {count_pos}.")
         elif curr_dist_data:
             parts.append(f"- Could not determine {dist_name}'s position relative to state averages.")

    elif not result.get("current_analysis_error"):
         parts.append(f"State-level comparison data could not be retrieved for {curr_date}.")

    # Combine explanation parts
    return " ".join(parts)


# --- analyze function (Generic, adapted from dugwell) ---
def analyze(district_name: str, report_date_str: str) -> Optional[Dict[str, Any]]:
    """
    Analyzes Component data for a specific district for the report_date
    and the previous day, providing a simplified comparison and state statistics.
    Uses globally defined COMPONENT_NAME, API_ENDPOINT, MAX_MARKS.
    """
    if not district_name or not report_date_str:
        log.error("District name and report date are required.")
        return None

    try:
        report_date_obj = datetime.strptime(report_date_str, "%Y-%m-%d").date()
        previous_date_obj = report_date_obj - timedelta(days=1)
        previous_date_str = previous_date_obj.strftime("%Y-%m-%d")
    except ValueError:
        log.error(f"Invalid date format: {report_date_str}. Please use YYYY-MM-DD.")
        return None

    # --- Fetch data for both dates using the helper ---
    current_analysis_data = _fetch_and_process_data_for_date(district_name, report_date_str)
    previous_analysis_data = _fetch_and_process_data_for_date(district_name, previous_date_str)

    # --- Initialize the final result structure ---
    analysis_result: Dict[str, Any] = {
        "component": COMPONENT_NAME,
        "max_marks": MAX_MARKS, # Include max_marks for explanation context
        "selected_district": district_name,
        "report_date": report_date_str,
        "previous_report_date": previous_date_str,
        "explanation": "",
        "selected_district_comparison": {
            "current_data": None,
            "previous_data": None,
            "change": None
        },
        "state_level_summary_today": {
            "by_score": {"top_performer": None, "bottom_performer": None},
            "by_count": {"top_performer": None, "bottom_performer": None}
        },
        "block_level_comparison": [],
        "state_statistics_today": {
             "districts_reporting": 0,
             "score": {"mean": None, "median": None, "stdev": None, "min": None, "max": None},
             "count": {"mean": None, "median": None, "stdev": None, "min": None, "max": None},
             "calculation_notes": []
        },
         "selected_district_position_vs_state": {
             "score_comparison": None,
             "count_comparison": None,
         },
        "current_analysis_error": current_analysis_data.get("fetch_error"),
        "previous_analysis_error": previous_analysis_data.get("fetch_error"),
    }

    # --- Populate Selected District Comparison ---
    curr_dist_data = current_analysis_data.get("district_data")
    prev_dist_data = previous_analysis_data.get("district_data")
    analysis_result["selected_district_comparison"]["current_data"] = curr_dist_data
    analysis_result["selected_district_comparison"]["previous_data"] = prev_dist_data

    if curr_dist_data and prev_dist_data:
        try:
            # Ensure scores are treated as floats for subtraction
            curr_score = float(safe_get(curr_dist_data, [SCORE_KEY], 0.0))
            prev_score = float(safe_get(prev_dist_data, [SCORE_KEY], 0.0))
            score_change = curr_score - prev_score

            # Ensure counts are treated as ints for subtraction
            curr_count = int(safe_get(curr_dist_data, [COUNT_KEY], 0))
            prev_count = int(safe_get(prev_dist_data, [COUNT_KEY], 0))
            count_change = curr_count - prev_count

            analysis_result["selected_district_comparison"]["change"] = {
                "score_change": round(score_change, 2),
                "count_change": count_change
            }
        except (TypeError, ValueError) as e:
             log.error(f"Error calculating district change (type error): {e}. Curr: {curr_dist_data}, Prev: {prev_dist_data}")
             analysis_result["selected_district_comparison"]["change"] = {"error": "Could not calculate change due to data type issue."}
        except Exception as e:
             log.error(f"Error calculating district change: {e}")
             analysis_result["selected_district_comparison"]["change"] = {"error": "Could not calculate change."}
    elif curr_dist_data and not prev_dist_data:
         analysis_result["selected_district_comparison"]["change"] = {"status": "Previous data unavailable"}


    # --- Populate State Level Summary & Statistics for Today ---
    state_results_today = current_analysis_data.get("state_results_processed", [])
    num_dist_reporting = len(state_results_today)
    analysis_result["state_statistics_today"]["districts_reporting"] = num_dist_reporting

    if num_dist_reporting > 0:
        # --- State Top/Bottom Performers ---
        comparison_score = get_top_bottom_performers_full(state_results_today, score_key=SCORE_KEY, name_key=NAME_KEY)
        analysis_result["state_level_summary_today"]["by_score"]["top_performer"] = comparison_score.get("top")
        analysis_result["state_level_summary_today"]["by_score"]["bottom_performer"] = comparison_score.get("bottom")

        comparison_count = get_top_bottom_by_count_full(state_results_today, count_key=COUNT_KEY, name_key=NAME_KEY)
        analysis_result["state_level_summary_today"]["by_count"]["top_performer"] = comparison_count.get("top")
        analysis_result["state_level_summary_today"]["by_count"]["bottom_performer"] = comparison_count.get("bottom")

        # --- State Descriptive Statistics ---
        # Ensure data used for stats is numeric
        all_scores = [float(d.get(SCORE_KEY, 0.0)) for d in state_results_today if isinstance(d.get(SCORE_KEY), (int, float))]
        all_counts = [int(d.get(COUNT_KEY, 0)) for d in state_results_today if isinstance(d.get(COUNT_KEY), int)]

        # Check if filtering removed all data
        if not all_scores:
            log.warning("No valid numeric scores found in state data for statistics.")
            analysis_result["state_statistics_today"]["calculation_notes"].append("No valid numeric scores found.")
        if not all_counts:
             log.warning("No valid integer counts found in state data for statistics.")
             analysis_result["state_statistics_today"]["calculation_notes"].append("No valid integer counts found.")


        stats = analysis_result["state_statistics_today"] # Shortcut

        try:
            if all_scores: # Check if list is not empty
                stats["score"]["min"] = min(all_scores)
                stats["score"]["max"] = max(all_scores)
                stats["score"]["mean"] = round(statistics.mean(all_scores), 2)
                stats["score"]["median"] = round(statistics.median(all_scores), 2)
                if len(all_scores) >= 2:
                    stats["score"]["stdev"] = round(statistics.stdev(all_scores), 2)
                else:
                    stats["score"]["stdev"] = 0.0
                    if "Standard deviation requires at least 2 data points for score." not in stats["calculation_notes"]:
                        stats["calculation_notes"].append("Standard deviation requires at least 2 data points for score.")

            if all_counts: # Check if list is not empty
                stats["count"]["min"] = min(all_counts)
                stats["count"]["max"] = max(all_counts)
                stats["count"]["mean"] = round(statistics.mean(all_counts), 2)
                stats["count"]["median"] = round(statistics.median(all_counts), 0)
                if len(all_counts) >= 2:
                    stats["count"]["stdev"] = round(statistics.stdev(all_counts), 2)
                else:
                    stats["count"]["stdev"] = 0.0
                    if "Standard deviation requires at least 2 data points for count." not in stats["calculation_notes"]:
                        stats["calculation_notes"].append("Standard deviation requires at least 2 data points for count.")

        except statistics.StatisticsError as e:
            log.error(f"Error calculating statistics: {e}")
            stats["calculation_notes"].append(f"Statistics calculation error: {e}")
        except Exception as e:
             log.error(f"Unexpected error calculating statistics: {e}")
             stats["calculation_notes"].append(f"Unexpected statistics calculation error: {e}")


        # --- Compare Selected District to State Stats ---
        if curr_dist_data:
            dist_pos = analysis_result["selected_district_position_vs_state"] # Shortcut
            try:
                dist_score = float(safe_get(curr_dist_data, [SCORE_KEY], 0.0))
                dist_count = int(safe_get(curr_dist_data, [COUNT_KEY], 0))

                # Score Comparison
                mean_score = stats["score"].get("mean")
                median_score = stats["score"].get("median")
                score_comp_parts = []
                if mean_score is not None:
                    if dist_score > mean_score: score_comp_parts.append("Above Mean")
                    elif dist_score < mean_score: score_comp_parts.append("Below Mean")
                    else: score_comp_parts.append("Equal to Mean")
                if median_score is not None:
                    if dist_score > median_score: score_comp_parts.append("Above Median")
                    elif dist_score < median_score: score_comp_parts.append("Below Median")
                    else: score_comp_parts.append("Equal to Median")
                dist_pos["score_comparison"] = " / ".join(score_comp_parts) if score_comp_parts else "Comparison N/A"


                # Count Comparison
                mean_count = stats["count"].get("mean")
                median_count = stats["count"].get("median")
                count_comp_parts = []
                if mean_count is not None:
                    if dist_count > mean_count: count_comp_parts.append("Above Mean")
                    elif dist_count < mean_count: count_comp_parts.append("Below Mean")
                    else: count_comp_parts.append("Equal to Mean")
                if median_count is not None:
                    if dist_count > median_count: count_comp_parts.append("Above Median")
                    elif dist_count < median_count: count_comp_parts.append("Below Median")
                    else: count_comp_parts.append("Equal to Median")
                dist_pos["count_comparison"] = " / ".join(count_comp_parts) if count_comp_parts else "Comparison N/A"
            except (TypeError, ValueError) as e:
                log.error(f"Error comparing district to state (type error): {e}. District data: {curr_dist_data}")
                dist_pos["score_comparison"] = "Error during comparison"
                dist_pos["count_comparison"] = "Error during comparison"


        else:
             analysis_result["selected_district_position_vs_state"]["score_comparison"] = "District data missing"
             analysis_result["selected_district_position_vs_state"]["count_comparison"] = "District data missing"

    else:
        log.warning(f"No state results processed for {report_date_str}, cannot generate state summary or statistics.")
        analysis_result["state_statistics_today"]["calculation_notes"].append("No reporting districts found.")


    # --- Populate Block Level Comparison ---
    current_blocks = current_analysis_data.get("block_level_data", [])
    previous_blocks_list = previous_analysis_data.get("block_level_data", [])
    previous_blocks_map = {block["name"]: block.get(COUNT_KEY, 0) for block in previous_blocks_list if block.get("name")}

    block_comparison_list = []
    for current_block in current_blocks:
        block_name = current_block.get("name")
        if not block_name: continue

        previous_count = previous_blocks_map.get(block_name)

        block_comp_entry = {
            "name": block_name,
            "actual_count_today": current_block.get(COUNT_KEY, 0),
            "actual_count_daybefore": previous_count if previous_count is not None else "N/A",
            "top_5_panchayats": current_block.get("top_5_panchayats", [])
        }
        block_comparison_list.append(block_comp_entry)

    block_comparison_list.sort(key=lambda x: x["actual_count_today"], reverse=True)
    analysis_result["block_level_comparison"] = block_comparison_list


    # --- Generate Final Explanation ---
    analysis_result["explanation"] = generate_simplified_explanation(analysis_result)


    # Check if essential data is missing for the current date
    if not curr_dist_data and not current_blocks and not state_results_today:
         log.error(f"Essential data missing for {district_name} ({COMPONENT_NAME}) on {report_date_str}. Analysis incomplete.")
         analysis_result["explanation"] = f"Error: Could not retrieve essential performance data for {district_name} ({COMPONENT_NAME}) on {report_date_str}. Analysis is incomplete. " + analysis_result["explanation"]


    return analysis_result


# --- Main Execution Block (Generic, adapted from dugwell) ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Analyze and Compare JSM {COMPONENT_NAME} Data (Simplified + Stats) for a District.")
    parser.add_argument("-d", "--district", required=True, help="Name of the district to analyze.")
    parser.add_argument("-dt", "--date", required=True, help="Report date (YYYY-MM-DD). Analysis will compare this date with the day before.")
    parser.add_argument("-o", "--output", help="Optional: File path to save the JSON output.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")

    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger('utils').setLevel(logging.DEBUG)

    log.info(f"Starting Simplified + Stats {COMPONENT_NAME} analysis for District: {args.district}, Date: {args.date}")
    result = analyze(args.district, args.date)

    output_json = {}
    if result:
        output_json = result
        log.info("Analysis complete.")
    else:
        log.error("Analysis failed to produce a result structure.")
        try:
            prev_date = (datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")
        except ValueError:
            prev_date = "N/A"
        output_json = {
            "component": COMPONENT_NAME,
            "max_marks": MAX_MARKS,
            "selected_district": args.district,
            "report_date": args.date,
            "previous_report_date": prev_date,
            "error": "Failed to generate analysis structure. Check logs.",
            "explanation": "Analysis could not be completed due to errors.",
            "selected_district_comparison": {},
            "state_level_summary_today": {},
            "block_level_comparison": [],
            "state_statistics_today": {},
            "selected_district_position_vs_state": {}
        }

    # Output the result
    output_string = json.dumps(output_json, indent=2, default=str) # Use default=str for safety


    if args.output:
        try:
            with open(args.output, 'w') as f:
                f.write(output_string)
            log.info(f"Output successfully saved to {args.output}")
        except IOError as e:
            log.error(f"Error saving output to file {args.output}: {e}")
            print("\n--- JSON Output ---")
            print(output_string)
            print("--- End JSON Output ---")
    else:
        print(output_string)