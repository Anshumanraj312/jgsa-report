# analyze_mybharat.py
import argparse
import json
import logging
from typing import Dict, Any, List, Optional
# Use the modified utils functions
from utils import (fetch_api_data, safe_get, find_district_data,
                   get_top_bottom_performers_full, get_top_bottom_by_count_full)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
log = logging.getLogger(__name__)

COMPONENT_NAME = "MyBharat (Jaldoot Volunteer Stats)" # Adjusted name slightly for clarity
API_ENDPOINT = "/report_jsm/mybharat/gender-stats"
SCORE_KEY = "marks"
COUNT_KEY = "total_count" # Field representing the count for this component
NAME_KEY = "district" # The key for the district name in the API response
MAX_MARKS = 10.0

def analyze(district_name: str, report_date: str) -> Optional[Dict[str, Any]]:
    """
    Analyzes MyBharat (Jaldoot Volunteer Stats) data for a specific district.
    Includes full data for top/bottom state performers by score and count.
    Note: Block-level breakdown, detailed gender stats, and top panchayats
          are not available from this specific API endpoint.
    """
    if not district_name or not report_date:
        log.error("District name and report date are required.")
        return None

    district_name_upper = district_name.strip().upper()
    analysis_result: Dict[str, Any] = {
        "component": COMPONENT_NAME,
        "selected_district": district_name,
        "report_date": report_date,
        "explanation": "",
        "district_data": None,
        # "block_level_data": [], # Removed this key
        "state_level_comparison": {
            "by_score": {"top_performer": None, "bottom_performer": None},
            "by_count": {"top_performer": None, "bottom_performer": None}
        }
    }

    # 1. Fetch State-Level Data (contains all districts)
    log.info(f"Fetching state-level {COMPONENT_NAME} data...")
    state_params = {'date': report_date} # API requires date
    state_data_raw = fetch_api_data(API_ENDPOINT, params=state_params)
    # Assuming the relevant district data is under 'districts_data' key in the response
    state_results = safe_get(state_data_raw, ["districts_data"], [])

    if not state_results:
        log.error(f"Could not fetch or parse state-level {COMPONENT_NAME} data.")
        analysis_result["explanation"] = f"Error: Could not retrieve state-level {COMPONENT_NAME} data for comparison."
        return analysis_result
    else:
        log.info(f"Fetched {len(state_results)} district results for {COMPONENT_NAME} state-level comparison.")

    # 2. Extract Selected District's Data from State Results
    selected_district_state_data = find_district_data(state_results, district_name_upper, name_key=NAME_KEY)

    if not selected_district_state_data:
        log.warning(f"Data for selected district '{district_name}' not found in state-level {COMPONENT_NAME} results.")
        # Add warning to explanation, but continue to allow state comparison
        # This part is handled later during explanation generation
    else:
        log.info(f"Found state-level {COMPONENT_NAME} data for selected district: {district_name}")
        analysis_result["district_data"] = process_mybharat_data(selected_district_state_data)

    # 3. Block/Panchayat Breakdown - NOT POSSIBLE
    # No code needed here anymore as the key is removed from analysis_result
    log.warning(f"Block-level breakdown and top panchayats are not available for {COMPONENT_NAME} via the current API endpoint.")

    # ---- STEP 4: Calculate State-Level Comparison (Modified) ----
    if state_results:
        # Comparison by Score
        comparison_score = get_top_bottom_performers_full(state_results, score_key=SCORE_KEY, name_key=NAME_KEY)
        top_score_processed = process_mybharat_data(comparison_score.get("top")) if comparison_score.get("top") else None
        bottom_score_processed = process_mybharat_data(comparison_score.get("bottom")) if comparison_score.get("bottom") else None
        analysis_result["state_level_comparison"]["by_score"]["top_performer"] = top_score_processed
        analysis_result["state_level_comparison"]["by_score"]["bottom_performer"] = bottom_score_processed
        log.info(f"{COMPONENT_NAME} State comparison by SCORE - Top: {safe_get(top_score_processed, ['name'])}, Bottom: {safe_get(bottom_score_processed, ['name'])}")

        # Comparison by Count
        comparison_count = get_top_bottom_by_count_full(state_results, count_key=COUNT_KEY, name_key=NAME_KEY)
        top_count_processed = process_mybharat_data(comparison_count.get("top")) if comparison_count.get("top") else None
        bottom_count_processed = process_mybharat_data(comparison_count.get("bottom")) if comparison_count.get("bottom") else None
        analysis_result["state_level_comparison"]["by_count"]["top_performer"] = top_count_processed
        analysis_result["state_level_comparison"]["by_count"]["bottom_performer"] = bottom_count_processed
        log.info(f"{COMPONENT_NAME} State comparison by COUNT - Top: {safe_get(top_count_processed, ['name'])}, Bottom: {safe_get(bottom_count_processed, ['name'])}")


    # ---- STEP 5: Add Explanations (Modified) ----
    explanation_parts = []
    district_data = analysis_result["district_data"]
    data_source_limitations = f"Note: Block-level breakdown, detailed gender statistics, and top 5 panchayats are not available for {COMPONENT_NAME} from this specific data source."

    if district_data:
        score = format(safe_get(district_data, [SCORE_KEY], 0.0), '.2f')
        actual = format(safe_get(district_data, [COUNT_KEY], 0), ',')
        target_val = safe_get(district_data, ["target"]) # Keep original type for check
        target_str = format(target_val, ',') if isinstance(target_val, (int, float)) else "N/A"

        explanation_parts.append(f"For {COMPONENT_NAME} on {report_date}, {district_name} reported a total of {actual} volunteers against a target of {target_str}, achieving a score of {score} out of {MAX_MARKS:.0f}.")
    else:
        # Include the warning if district data wasn't found but state data was
        if state_results:
             explanation_parts.append(f"Warning: Data for '{district_name}' not found in the state-level {COMPONENT_NAME} results for {report_date}.")
        else:
            # This case is already handled by returning early if state_results is empty
            # But as a fallback:
            explanation_parts.append(f"Could not retrieve specific {COMPONENT_NAME} performance data for {district_name} on {report_date}.")


    explanation_parts.append(data_source_limitations) # Add limitations note

    # Explanation for Score Comparison
    comp_score = analysis_result["state_level_comparison"]["by_score"]
    if comp_score.get("top_performer") and comp_score.get("bottom_performer"):
        top_name = safe_get(comp_score['top_performer'], ['name'], 'N/A')
        top_score_val = format(safe_get(comp_score['top_performer'], [SCORE_KEY], 0.0), '.2f')
        bot_name = safe_get(comp_score['bottom_performer'], ['name'], 'N/A')
        bot_score_val = format(safe_get(comp_score['bottom_performer'], [SCORE_KEY], 0.0), '.2f')
        explanation_parts.append(f"State-wide (by SCORE), the top performing district for {COMPONENT_NAME} was {top_name} (Score: {top_score_val}) and the bottom performer was {bot_name} (Score: {bot_score_val}).")
    elif state_results: # Only add this if state data was fetched but comparison failed
         explanation_parts.append(f"State-wide top/bottom performers by SCORE for {COMPONENT_NAME} could not be fully determined.")

    # Explanation for Count Comparison
    comp_count = analysis_result["state_level_comparison"]["by_count"]
    if comp_count.get("top_performer") and comp_count.get("bottom_performer"):
        top_name = safe_get(comp_count['top_performer'], ['name'], 'N/A')
        top_count_val = format(safe_get(comp_count['top_performer'], [COUNT_KEY], 0), ',')
        bot_name = safe_get(comp_count['bottom_performer'], ['name'], 'N/A')
        bot_count_val = format(safe_get(comp_count['bottom_performer'], [COUNT_KEY], 0), ',')
        explanation_parts.append(f"State-wide (by total volunteer COUNT), the district with the most {COMPONENT_NAME} volunteers was {top_name} ({top_count_val}) and the district with the fewest was {bot_name} ({bot_count_val}).")
    elif state_results: # Only add this if state data was fetched but comparison failed
         explanation_parts.append(f"State-wide top/bottom districts by COUNT for {COMPONENT_NAME} could not be fully determined.")


    analysis_result["explanation"] = " ".join(filter(None, explanation_parts)) # Filter out potential None/empty strings

    return analysis_result


def process_mybharat_data(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Processes a single district entry from the MyBharat API response.
    Assumes gender breakdown is not reliably available from this endpoint.
    """
    if not data:
        return None
    # Provides details for the district level comparison
    processed = {
        "name": safe_get(data, [NAME_KEY]), # Use NAME_KEY to get name from raw data
        COUNT_KEY: safe_get(data, [COUNT_KEY], 0),
        "target": safe_get(data, ["target"], "N/A"), # Keep target
        "achievement_percentage": safe_get(data, ["achievement_percent"], "N/A"), # Key from API? Check actual response
        SCORE_KEY: safe_get(data, [SCORE_KEY], 0.0)
    }
    # Optional: Clean up N/A target if needed, e.g., replace with None or 0 if preferred
    if processed["target"] == "N/A":
        processed["target"] = None # Or keep "N/A" string

    # Ensure achievement_percentage is numeric or None/N/A
    ach_perc = processed["achievement_percentage"]
    if isinstance(ach_perc, str) and ach_perc != "N/A":
        try:
            processed["achievement_percentage"] = float(ach_perc)
        except (ValueError, TypeError):
             processed["achievement_percentage"] = "N/A" # Keep N/A if conversion fails
    elif not isinstance(ach_perc, (int, float)) and ach_perc != "N/A":
         processed["achievement_percentage"] = "N/A" # Default to N/A if not number

    return processed


# __main__ block remains the same...
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Analyze JSM {COMPONENT_NAME} Data for a District.")
    parser.add_argument("-d", "--district", required=True, help="Name of the district to analyze.")
    parser.add_argument("-dt", "--date", required=True, help="Report date (YYYY-MM-DD).")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")

    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger('utils').setLevel(logging.DEBUG) # Assuming utils logger name

    log.info(f"Starting {COMPONENT_NAME} analysis for District: {args.district}, Date: {args.date}")
    result = analyze(args.district, args.date)

    if result:
        print(json.dumps(result, indent=2))
        log.info("Analysis complete. JSON output generated.")
    else:
        log.error("Analysis failed.")
        # Ensure the error JSON structure matches the success structure for consistency
        # Removed block_level_data from error output as well
        print(json.dumps({
            "component": COMPONENT_NAME,
            "selected_district": args.district,
            "report_date": args.date,
            "explanation": "Error: Failed to generate analysis. Check logs for details.",
            "district_data": None,
            # "block_level_data": [], # Removed
            "state_level_comparison": {
                "by_score": {"top_performer": None, "bottom_performer": None},
                "by_count": {"top_performer": None, "bottom_performer": None}
            }
        }, indent=2))