# analyze_old_works.py
import argparse
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import statistics # <--- IMPORT STATISTICS MODULE
import math

# Use the modified utils functions
from utils import (fetch_api_data, safe_get, find_district_data,
                   get_top_bottom_performers_full, get_top_bottom_by_count_full)

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
log = logging.getLogger(__name__)

# --- Constants ---
COMPONENT_NAME = "Old Works (NRM)"
API_ENDPOINT_PERF = "/report_jsm/performance-marks"
SCORE_KEY = "overall_old_work_score"
BLOCK_COMPLETED_TODAY_KEY = "completed_works_by_type_till_today" # Key for block counts today
BLOCK_CHANGE_KEY = "completed_works_change_by_type" # Key for block changes
MAX_MARKS = 20.0
COUNT_KEY = "total_work_count" # Represents relevant works for scoring period
TOTAL_WORK_COMPLETED_KEY = "total_work_completed" # Key for sum of actual completed works
NAME_KEY = "name"

OLD_NRM_TARGET_CATEGORIES = [
    "Talab Nirman", "Check_Stop Dam", "Recharge Pit", "Koop Nirman",
    "Percolation Talab", "Khet Talab", "Other NRM Work"
]
ALL_RELEVANT_CATEGORIES_FOR_BLOCK_BREAKDOWN = OLD_NRM_TARGET_CATEGORIES

# --- Data Processing Functions ---

def process_district_perf_data(raw_district_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Processes raw performance data for a single entity (district or block)."""
    if not raw_district_data: return None
    name = safe_get(raw_district_data, [NAME_KEY]);
    if not name: return None
    # log.debug(f"Processing performance data for entity: {name}") # Reduce verbosity

    target_marks = float(safe_get(raw_district_data, ["target_marks"], 0.0) or 0.0)
    payment_marks = float(safe_get(raw_district_data, ["payment_marks"], 0.0) or 0.0)
    overall_score = round(target_marks + payment_marks, 2)

    payment_details_raw = safe_get(raw_district_data, ["payment_details"], {})
    financial_progress = {
        "baseline_pending_lakhs": round(safe_get(payment_details_raw, ["baseline_pending_for_calc"], 0.0) / 100000, 2),
        "current_pending_lakhs": round(safe_get(payment_details_raw, ["current_pending"], 0.0) / 100000, 2),
        "reduction_percentage": round(safe_get(payment_details_raw, ["reduction_percentage"], 0.0), 2),
        "marks": round(payment_marks, 2)
    }

    work_type_details = {}; total_district_date_specific_work_count = 0
    categories_api_data = safe_get(raw_district_data, ["categories"], {})
    category_counts_api_data = safe_get(raw_district_data, ["category_counts"], {})
    total_actual_completed_works = 0 # Initialize the new counter

    for category in OLD_NRM_TARGET_CATEGORIES:
        cat_perf_data = categories_api_data.get(category, {})
        raw_count_total = category_counts_api_data.get(category, 0)
        total_district_date_specific_work_count += raw_count_total

        target_val = cat_perf_data.get("target")
        processed_target = target_val if isinstance(target_val, (int, float)) else "N/A"
        completed_val = cat_perf_data.get("completed", 0)
        # Safely add to the total actual completed count
        total_actual_completed_works += int(completed_val or 0)

        ach_perc_val = cat_perf_data.get("achievement_percentage")
        ach_perc_processed = "N/A"
        if isinstance(ach_perc_val, (int, float)):
            ach_perc_processed = round(ach_perc_val, 2) if not math.isinf(ach_perc_val) else 'Inf'
        elif isinstance(ach_perc_val, str) and ach_perc_val.strip().lower() == 'inf':
             ach_perc_processed = 'Inf'
        marks_val = cat_perf_data.get("marks", 0.0)
        processed_marks = round(marks_val, 2) if isinstance(marks_val, (int, float)) else 0.0

        work_type_details[category] = {
            "target": processed_target,
            "completed": completed_val,
            "achievement_percentage": ach_perc_processed,
            "marks": processed_marks
        }

    return {
        "name": name,
        SCORE_KEY: overall_score,
        COUNT_KEY: total_district_date_specific_work_count, # Represents relevant works for scoring period
        TOTAL_WORK_COMPLETED_KEY: total_actual_completed_works, # Total actual completed from categories
        "target_achievement_marks": round(target_marks, 2),
        "financial_progress_marks": round(payment_marks, 2),
        "financial_progress_details": financial_progress,
        "individual_work_types": work_type_details
    }


def find_category_leaders(processed_state_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Finds the district with the highest marks for each NRM work category."""
    category_leaders = {}
    if not processed_state_data: return category_leaders
    for category in OLD_NRM_TARGET_CATEGORIES:
        leader_district = None; max_marks = -1.0
        for district_data in processed_state_data:
            cat_details = safe_get(district_data, ["individual_work_types", category])
            if cat_details:
                current_marks = safe_get(cat_details, ["marks"], 0.0)
                if isinstance(current_marks, (int, float)):
                    if current_marks > max_marks:
                        max_marks = current_marks
                        leader_district = {"name": district_data["name"], "category_details": cat_details}
        category_leaders[category] = leader_district if leader_district else {"name": "N/A", "category_details": None}
    return category_leaders


def simplify_performer_data(performer_data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Removes the 'individual_work_types' key from a performer's data."""
    if not performer_data: return None
    simplified_data = performer_data.copy()
    simplified_data.pop("individual_work_types", None)
    return simplified_data


def _fetch_and_process_data_for_date(district_name: str, target_date: str, block_list: List[str]) -> Dict[str, Any]:
    """Fetches and processes Old Works performance data for a single date."""
    log.info(f"--- Processing {COMPONENT_NAME} data for date: {target_date} ---")
    district_name_upper = district_name.strip().upper()
    date_analysis: Dict[str, Any] = {
        "date": target_date, "state_results_processed": [], "district_data": None,
        "block_level_data_completed_counts": {}, "fetch_error": None
    }
    fetch_errors = []

    # 1. Fetch State-Level Performance Data
    state_perf_params = {'date': target_date}
    state_data_perf_raw = fetch_api_data(API_ENDPOINT_PERF, params=state_perf_params)
    state_results_perf_raw = safe_get(state_data_perf_raw, ["results"], [])
    if not state_results_perf_raw:
        msg = f"Could not fetch state-level {COMPONENT_NAME} performance data for {target_date}."
        log.error(msg); fetch_errors.append(msg)
    else:
        # log.info(f"Fetched {len(state_results_perf_raw)} raw state entries for {target_date}.")
        for district_raw_data in state_results_perf_raw:
            processed = process_district_perf_data(district_raw_data) # This now includes total_work_completed
            if processed: date_analysis["state_results_processed"].append(processed)
            else: log.warning(f"Skipped state processing for raw entry on {target_date}: {safe_get(district_raw_data, [NAME_KEY], 'Unknown')}")
        log.info(f"Processed {len(date_analysis['state_results_processed'])} state entries for {target_date}.")

    # 2. Extract Selected District's Processed Data
    selected_district_processed_data = next((d for d in date_analysis["state_results_processed"] if d.get(NAME_KEY) == district_name_upper), None)
    if selected_district_processed_data: date_analysis["district_data"] = selected_district_processed_data
    elif state_results_perf_raw:
        msg = f"Processed performance data for selected district '{district_name}' not found in state results for {target_date}."
        log.warning(msg)

    # 3. Fetch Block-Level Performance Data for completed counts
    if not block_list:
        msg = f"Block list empty for {district_name}, cannot fetch block breakdown for {target_date}."
        log.warning(msg); fetch_errors.append(msg)
    else:
        block_perf_params = {'district': district_name, 'date': target_date}
        block_data_perf_raw = fetch_api_data(API_ENDPOINT_PERF, params=block_perf_params)
        block_results_raw = safe_get(block_data_perf_raw, ["results"], [])
        if not block_results_raw:
             msg = f"Could not fetch block-level performance data using endpoint {API_ENDPOINT_PERF} for {district_name} on {target_date}."
             log.warning(msg); fetch_errors.append(msg)
        else:
            block_data_map = {safe_get(b, [NAME_KEY]): b for b in block_results_raw if safe_get(b, [NAME_KEY])}
            # log.info(f"Processing completed counts for {len(block_data_map)} blocks found in performance data for {target_date}.")
            for block_name in block_list:
                block_raw_data = block_data_map.get(block_name)
                block_completed_counts = {}
                for category in ALL_RELEVANT_CATEGORIES_FOR_BLOCK_BREAKDOWN:
                    completed_count = 0
                    if block_raw_data:
                         block_categories_api_data = safe_get(block_raw_data, ["categories"], {})
                         cat_perf_data = block_categories_api_data.get(category, {})
                         completed_count = int(safe_get(cat_perf_data, ["completed"], 0) or 0)
                    block_completed_counts[category] = completed_count
                date_analysis["block_level_data_completed_counts"][block_name] = block_completed_counts

    if fetch_errors: date_analysis["fetch_error"] = "; ".join(fetch_errors)
    log.info(f"--- Finished processing {COMPONENT_NAME} data for date: {target_date} ---")
    return date_analysis

# --- Explanation Generation ---
def generate_old_works_explanation_with_delta_changes(result: Dict[str, Any]) -> str:
    """Generates narrative explanation referencing calculated changes including block changes and today's block counts."""
    parts = []
    comp_name = result.get("component", "Old Works (NRM)")
    max_marks_local = result.get("max_marks", MAX_MARKS)
    dist_name = result["selected_district"]
    curr_date = result["report_date"]
    prev_date = result["previous_report_date"]

    dist_comp = result.get("selected_district_comparison", {})
    curr_dist_data = dist_comp.get("current_data")
    dist_change = dist_comp.get("change", {})

    state_summary = result.get("state_level_summary_today", {})
    cat_leaders = result.get("state_category_leaders_today", {})
    blocks_comp = result.get("block_level_comparison", []) # Now list of {name, completed_today_dict, change_dict}

    # Part 1: Selected District Performance & Change
    if curr_dist_data:
        score = format(safe_get(curr_dist_data, [SCORE_KEY], 0.0), '.2f');
        nrm_count_relevant = format(safe_get(curr_dist_data, [COUNT_KEY], 0), ',') # Relevant count
        nrm_count_completed = format(safe_get(curr_dist_data, [TOTAL_WORK_COMPLETED_KEY], 0), ',') # Actual completed count
        fin_marks = format(safe_get(curr_dist_data, ["financial_progress_marks"], 0.0), '.2f');
        target_marks = format(safe_get(curr_dist_data, ["target_achievement_marks"], 0.0), '.2f')

        parts.append(f"On {curr_date}, for {comp_name}, {dist_name}'s overall performance score was {score}/{max_marks_local:.0f} (Target Marks: {target_marks}, Payment Marks: {fin_marks}).")
        parts.append(f"This score considers {nrm_count_relevant} NRM works relevant to the performance calculation period. A total of {nrm_count_completed} NRM works were completed across the tracked categories.") # Mention both counts

        if isinstance(dist_change, dict) and "score_change" in dist_change:
            score_delta = dist_change.get("score_change", 0.0);
            relevant_count_delta = dist_change.get("count_change", 0) # Change in relevant count
            completed_count_delta = dist_change.get("total_work_completed_change", 0) # Change in actual completed
            fin_marks_delta = dist_change.get("financial_marks_change", 0.0)

            change_desc = []
            if score_delta != 0.0: change_desc.append(f"overall score changed by {score_delta:+.2f}")
            else: change_desc.append("overall score remained the same")

            # Describe changes in both counts
            if relevant_count_delta != 0: change_desc.append(f"relevant NRM work count changed by {relevant_count_delta:+,}")
            else: change_desc.append("relevant NRM work count remained the same")
            if completed_count_delta != 0: change_desc.append(f"total completed works changed by {completed_count_delta:+,}")
            else: change_desc.append("total completed works remained the same")

            if fin_marks_delta != 0.0: change_desc.append(f"payment marks changed by {fin_marks_delta:+.2f}")
            parts.append(f"Compared to {prev_date}, the {', the '.join(change_desc)}.")

            individual_changes = dist_change.get("individual_work_type_changes", {})
            if individual_changes: parts.append(f"Changes in completed works/marks were observed in {len(individual_changes)} specific NRM categories (details in 'change' data).")

        elif dist_change and dist_change.get("status") == "Previous day data unavailable for district": parts.append(f"Data for {prev_date} was not available for district comparison.")
        elif result.get("previous_analysis_error"): parts.append(f"Could not retrieve comparison data for {dist_name} from {prev_date} due to error.")
        else: parts.append(f"Data for {prev_date} was not available for district comparison.")
        parts.append("Detailed current district data includes financial progress metrics and NRM work type breakdown.")
    else:
        parts.append(f"Could not retrieve specific {comp_name} performance data for {dist_name} on {curr_date}.")
        if result.get("current_analysis_error"): parts.append(f"(Error: {result['current_analysis_error']})")

    # Part 2: Block Level Summary & Change Indication
    if blocks_comp:
         num_blocks = len(blocks_comp)
         any_block_changed = any(b.get(BLOCK_CHANGE_KEY) for b in blocks_comp) # Check if the change dict is non-empty

         parts.append(f"Block-level data for {num_blocks} blocks within {dist_name}:")
         parts.append(f"- Shows the total number of completed NRM works by category as of today ({curr_date}).")
         if any_block_changed:
              parts.append(f"- Also shows the CHANGE in completed works for each category compared to the previous day ({prev_date}) (only categories with changes listed).")
         else:
              parts.append(f"- No changes in completed works per category were observed compared to the previous day (or previous data was unavailable).")

         parts.append("Note: Panchayat-level data is not available from this performance endpoint.")
    elif result.get("current_analysis_error") and "block list" in result["current_analysis_error"]: parts.append(f"Block list for {dist_name} could not be retrieved, block comparison unavailable.")
    elif result.get("current_analysis_error") and "block-level performance data" in result["current_analysis_error"]: parts.append(f"Block-level performance data for {dist_name} was unavailable for {curr_date}, block comparison incomplete.")
    elif curr_dist_data: parts.append("Block-level comparison data could not be generated.")


    # Part 3 & 4: State Summary & Category Leaders (Uses TOTAL_WORK_COMPLETED_KEY for count)
    if state_summary.get("by_score",{}).get("top_performer"):
         parts.append(f"State-wide summary for {curr_date}:")
         top_score_dist = state_summary['by_score']['top_performer']; bot_score_dist = state_summary['by_score']['bottom_performer']
         parts.append(f"- Top performer by Overall Score: {top_score_dist['name']} ({top_score_dist[SCORE_KEY]:.2f}). Bottom: {bot_score_dist['name']} ({bot_score_dist[SCORE_KEY]:.2f}).")

         # Use TOTAL_WORK_COMPLETED_KEY for text and value
         top_count_dist = state_summary.get('by_count', {}).get('top_performer'); bot_count_dist = state_summary.get('by_count', {}).get('bottom_performer')
         if top_count_dist and bot_count_dist:
             top_val = format(safe_get(top_count_dist, [TOTAL_WORK_COMPLETED_KEY], 0), ',')
             bot_val = format(safe_get(bot_count_dist, [TOTAL_WORK_COMPLETED_KEY], 0), ',')
             parts.append(f"- Highest Total Completed NRM Work Count: {top_count_dist['name']} ({top_val}). Lowest: {bot_count_dist['name']} ({bot_val}).") # Changed text and value key
         else:
             parts.append("- Top/Bottom districts by total completed NRM work count could not be determined.")
    elif not result.get("current_analysis_error"): parts.append(f"State-level comparison data could not be generated for {curr_date}.")

    # Category Leaders
    if cat_leaders:
        leader_parts = []
        for category, leader_info in cat_leaders.items():
             leader_name = leader_info.get("name", "N/A")
             if leader_name != "N/A" and leader_info.get("category_details"):
                 cat_marks = leader_info["category_details"].get("marks", 0.0); leader_parts.append(f"{category}: {leader_name} (Marks: {cat_marks:.2f})")
        if leader_parts: parts.append(f"State Leaders by Marks (as of {curr_date}) within specific NRM categories:"); parts.append("- " + "; ".join(leader_parts) + ".")

    final_explanation = " ".join(p for p in parts if p)
    return final_explanation

# --- Main Analysis Function ---
def analyze(district_name: str, report_date_str: str) -> Optional[Dict[str, Any]]:
    """
    Analyzes Old Works (NRM) Performance Marks data for a specific district,
    comparing changes from the previous day. Shows block changes and today's counts.
    Includes total completed works count for the district. State summary uses total completed count.
    Calculates and includes state-level financial reduction statistics.
    """
    # --- Date and Initialization ---
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
    district_name_upper = district_name.strip().upper()

    # --- Pre-fetch Block List ---
    log.info(f"Fetching block list for district: {district_name}")
    blocks_raw = fetch_api_data("/report_jsm/blocks", params={'district': district_name})
    block_list = safe_get(blocks_raw, ['blocks'], [])
    if not block_list:
       log.error(f"CRITICAL: Could not fetch block list for {district_name}. Analysis cannot proceed.")
       return {
            "component": COMPONENT_NAME, "max_marks": MAX_MARKS, "selected_district": district_name,
            "report_date": report_date_str, "previous_report_date": previous_date_str,
            "error": f"Could not fetch block list for {district_name}. Analysis aborted.",
            "explanation": "Analysis failed: Essential block list could not be retrieved.",
            "selected_district_comparison": {}, "state_level_summary_today": {},
            "block_level_comparison": [], "state_category_leaders_today": {},
            # Initialize state_context even in error case for consistency
            "state_context": {
                 "financial_stats": {
                     "median_reduction": 0.0, "mean_reduction": 0.0, "count_districts_calculated": 0
                 }
            }
       }

    # --- Fetch data for both dates ---
    current_analysis_data = _fetch_and_process_data_for_date(district_name, report_date_str, block_list)
    previous_analysis_data = _fetch_and_process_data_for_date(district_name, previous_date_str, block_list)


    # --- Initialize the final result structure (ADDED state_context) ---
    analysis_result: Dict[str, Any] = {
        "component": COMPONENT_NAME,
        "max_marks": MAX_MARKS,
        "selected_district": district_name,
        "report_date": report_date_str,
        "previous_report_date": previous_date_str,
        "explanation": "",
        "selected_district_comparison": {
            "current_data": None,
            "change": None
        },
        "state_level_summary_today": {
            "by_score": {"top_performer": None, "bottom_performer": None},
            "by_count": {"top_performer": None, "bottom_performer": None}
        },
        "block_level_comparison": [],
        "state_category_leaders_today": {},
        # ADD state_context STRUCTURE
        "state_context": {
             "financial_stats": {
                 "median_reduction": 0.0,
                 "mean_reduction": 0.0,
                 "count_districts_calculated": 0
             }
             # Can add other state stats here later if needed (e.g., overall scores)
         },
        "current_analysis_error": current_analysis_data.get("fetch_error"),
        "previous_analysis_error": previous_analysis_data.get("fetch_error"),
    }

    # --- Populate Selected District Comparison ---
    curr_dist_data = current_analysis_data.get("district_data")
    prev_dist_data = previous_analysis_data.get("district_data")
    analysis_result["selected_district_comparison"]["current_data"] = curr_dist_data
    if curr_dist_data and prev_dist_data:
        try:
            curr_score = float(safe_get(curr_dist_data, [SCORE_KEY], 0.0)); prev_score = float(safe_get(prev_dist_data, [SCORE_KEY], 0.0))
            score_change = round(curr_score - prev_score, 2)
            curr_relevant_count = int(safe_get(curr_dist_data, [COUNT_KEY], 0)); prev_relevant_count = int(safe_get(prev_dist_data, [COUNT_KEY], 0))
            relevant_count_change = curr_relevant_count - prev_relevant_count
            curr_completed_total = int(safe_get(curr_dist_data, [TOTAL_WORK_COMPLETED_KEY], 0)); prev_completed_total = int(safe_get(prev_dist_data, [TOTAL_WORK_COMPLETED_KEY], 0))
            completed_total_change = curr_completed_total - prev_completed_total
            curr_fin_marks = float(safe_get(curr_dist_data, ["financial_progress_marks"], 0.0)); prev_fin_marks = float(safe_get(prev_dist_data, ["financial_progress_marks"], 0.0))
            fin_marks_change = round(curr_fin_marks - prev_fin_marks, 2)

            individual_changes = {}
            curr_types = safe_get(curr_dist_data, ["individual_work_types"], {}); prev_types = safe_get(prev_dist_data, ["individual_work_types"], {})
            for category in OLD_NRM_TARGET_CATEGORIES:
                curr_cat_data = curr_types.get(category, {}); prev_cat_data = prev_types.get(category, {})
                curr_completed = int(safe_get(curr_cat_data, ["completed"], 0)); prev_completed = int(safe_get(prev_cat_data, ["completed"], 0))
                completed_change = curr_completed - prev_completed
                curr_marks = float(safe_get(curr_cat_data, ["marks"], 0.0)); prev_marks = float(safe_get(prev_cat_data, ["marks"], 0.0))
                marks_change = round(curr_marks - prev_marks, 2)
                if completed_change != 0 or marks_change != 0.0: individual_changes[category] = { "completed_change": completed_change, "marks_change": marks_change }

            analysis_result["selected_district_comparison"]["change"] = {
                "score_change": score_change,
                "count_change": relevant_count_change,
                "total_work_completed_change": completed_total_change,
                "financial_marks_change": fin_marks_change,
                "individual_work_type_changes": individual_changes
            }
        except (TypeError, ValueError) as e: log.error(f"Error calculating district change (type error): {e}."); analysis_result["selected_district_comparison"]["change"] = {"error": "Could not calculate change due to data type issue."}
        except Exception as e: log.error(f"Error calculating district change: {e}"); analysis_result["selected_district_comparison"]["change"] = {"error": "Could not calculate change."}
    elif curr_dist_data and not prev_dist_data: analysis_result["selected_district_comparison"]["change"] = {"status": "Previous day data unavailable for district"}

    # --- Populate State Level Summary & Category Leaders for Today ---
    processed_state_perf_results_today = current_analysis_data.get("state_results_processed", [])
    if processed_state_perf_results_today:
        # Top/Bottom by Score
        comparison_score_full = get_top_bottom_performers_full(processed_state_perf_results_today, score_key=SCORE_KEY, name_key=NAME_KEY)
        analysis_result["state_level_summary_today"]["by_score"]["top_performer"] = simplify_performer_data(comparison_score_full.get("top"))
        analysis_result["state_level_summary_today"]["by_score"]["bottom_performer"] = simplify_performer_data(comparison_score_full.get("bottom"))

        # Top/Bottom by Count (using TOTAL_WORK_COMPLETED_KEY)
        comparison_count_full = get_top_bottom_by_count_full(
            processed_state_perf_results_today,
            count_key=TOTAL_WORK_COMPLETED_KEY,
            name_key=NAME_KEY
        )
        top_count_perf = comparison_count_full.get("top"); bot_count_perf = comparison_count_full.get("bottom")
        analysis_result["state_level_summary_today"]["by_count"]["top_performer"] = top_count_perf
        analysis_result["state_level_summary_today"]["by_count"]["bottom_performer"] = bot_count_perf

        # Category Leaders
        analysis_result["state_category_leaders_today"] = find_category_leaders(processed_state_perf_results_today)

        # *** NEW: Calculate State Financial Statistics ***
        financial_reductions = []
        for district_data in processed_state_perf_results_today:
            reduction_pct = safe_get(district_data, ["financial_progress_details", "reduction_percentage"])
            if isinstance(reduction_pct, (int, float)) and not math.isnan(reduction_pct):
                financial_reductions.append(reduction_pct)
            # else: log.debug(f"Skipping invalid reduction_pct '{reduction_pct}' for district {safe_get(district_data, ['name'])} in financial stats calc.")

        median_reduction = 0.0
        mean_reduction = 0.0
        num_calculated = len(financial_reductions)

        if num_calculated > 0:
            try:
                median_reduction = round(statistics.median(financial_reductions), 2)
                mean_reduction = round(statistics.mean(financial_reductions), 2)
                log.info(f"Calculated state financial stats from {num_calculated} districts: Median={median_reduction}%, Mean={mean_reduction}%")
            except statistics.StatisticsError as e:
                log.error(f"Statistics error calculating financial stats: {e}")
            except Exception as e:
                 log.error(f"Unexpected error calculating financial stats: {e}")
        else:
            log.warning(f"No valid financial reduction percentages found for {report_date_str}, median/mean will be 0.")

        # Store the calculated values in the state_context
        analysis_result["state_context"]["financial_stats"]["median_reduction"] = median_reduction
        analysis_result["state_context"]["financial_stats"]["mean_reduction"] = mean_reduction
        analysis_result["state_context"]["financial_stats"]["count_districts_calculated"] = num_calculated
        # *** END NEW SECTION ***

    else:
        log.warning(f"No processed state data for {report_date_str}, cannot generate state summary, category leaders, or financial stats.")


    # --- Populate Block Level Comparison ---
    current_blocks_completed = current_analysis_data.get("block_level_data_completed_counts", {})
    previous_blocks_completed = previous_analysis_data.get("block_level_data_completed_counts", {})
    block_comparison_list = []
    for block_name in block_list:
        today_counts = current_blocks_completed.get(block_name, {})
        yesterday_counts = previous_blocks_completed.get(block_name, {})
        block_changes = {}
        all_block_categories = set(today_counts.keys()) | set(yesterday_counts.keys()) & set(ALL_RELEVANT_CATEGORIES_FOR_BLOCK_BREAKDOWN)
        for category in all_block_categories:
            today_count = int(today_counts.get(category, 0))
            yesterday_count = int(yesterday_counts.get(category, 0))
            change = today_count - yesterday_count
            if change != 0: block_changes[category] = change
        block_comp_entry = {
            "name": block_name,
            BLOCK_COMPLETED_TODAY_KEY: today_counts,
            BLOCK_CHANGE_KEY: block_changes if block_changes else {}
        }
        block_comparison_list.append(block_comp_entry)
    block_comparison_list.sort(key=lambda x: x["name"])
    analysis_result["block_level_comparison"] = block_comparison_list


    # --- Generate Final Explanation ---
    analysis_result["explanation"] = generate_old_works_explanation_with_delta_changes(analysis_result)


    # --- Final Cleanup and Checks ---
    if analysis_result["current_analysis_error"] and analysis_result["current_analysis_error"].startswith("Warning:"): analysis_result.pop("current_analysis_error", None)
    if analysis_result["previous_analysis_error"] and analysis_result["previous_analysis_error"].startswith("Warning:"): analysis_result.pop("previous_analysis_error", None)
    if not curr_dist_data and not processed_state_perf_results_today:
         log.error(f"Essential district/state data missing for {district_name} on {report_date_str}. Analysis incomplete.")
         analysis_result["explanation"] = f"Error: Could not retrieve essential performance data for {district_name} or state on {report_date_str}. Analysis is incomplete. " + analysis_result["explanation"]

    return analysis_result

# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Analyze and Compare JSM {COMPONENT_NAME} Performance Data for a District.")
    parser.add_argument("-d", "--district", required=True, help="Name of the district to analyze.")
    parser.add_argument("-dt", "--date", required=True, help="Report date (YYYY-MM-DD). Analysis will compare this date with the day before.")
    parser.add_argument("-o", "--output", help="Optional: File path to save the JSON output.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    args = parser.parse_args()

    # Configure logging level based on debug flag
    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger('utils').setLevel(logging.DEBUG) # Also set utils logger level if desired
    else:
        log.setLevel(logging.INFO)
        logging.getLogger('utils').setLevel(logging.INFO)

    log.info(f"Starting {COMPONENT_NAME} analysis for District: {args.district}, Date: {args.date}")
    result = analyze(args.district, args.date)
    output_json = {}

    if result:
        output_json = result
        log.info("Analysis complete.")
    else:
        log.error("Analysis failed to produce a result structure. Critical data likely missing.")
        try:
            prev_date = (datetime.strptime(args.date, "%Y-%m-%d").date() - timedelta(days=1)).strftime("%Y-%m-%d")
        except ValueError:
            prev_date = "N/A"
        # Provide a consistent error structure including state_context
        output_json = {
            "component": COMPONENT_NAME, "max_marks": MAX_MARKS, "selected_district": args.district,
            "report_date": args.date, "previous_report_date": prev_date,
            "error": "Failed to generate analysis structure. Critical data likely missing. Check logs.",
            "explanation": "Analysis could not be completed due to critical errors.",
            "selected_district_comparison": {}, "state_level_summary_today": {},
            "block_level_comparison": [], "state_category_leaders_today": {},
            "state_context": { # Initialize state_context even in error case
                 "financial_stats": {
                     "median_reduction": 0.0, "mean_reduction": 0.0, "count_districts_calculated": 0
                 }
             }
        }

    # Convert result to JSON string
    try:
        output_string = json.dumps(output_json, indent=2, default=str, ensure_ascii=False) # Added ensure_ascii=False for Hindi characters
    except TypeError as e:
        log.error(f"Error serializing result to JSON: {e}")
        output_string = json.dumps({"error": f"JSON Serialization Error: {e}"}, indent=2)

    # Handle output
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f: # Ensure UTF-8 encoding
                f.write(output_string)
            log.info(f"Output successfully saved to {args.output}")
        except IOError as e:
            log.error(f"Error saving output to file {args.output}: {e}")
            print("\n--- JSON Output (Error saving to file) ---")
            print(output_string)
            print("--- End JSON Output ---")
    else:
        # Print directly to console with UTF-8 handling might be needed depending on terminal
        print(output_string)

    log.info(f"Finished {COMPONENT_NAME} analysis for District: {args.district}, Date: {args.date}")