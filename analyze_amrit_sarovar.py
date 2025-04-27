# analyze_amrit_sarovar.py
import argparse
import json
import logging
import os # Needed for path manipulation if saving to specific dirs
from typing import Dict, Any, List, Optional
# Use the modified utils functions
from utils import (fetch_api_data, safe_get, find_district_data,
                   get_top_bottom_performers_full, get_top_bottom_by_count_full)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s:%(lineno)d] - %(message)s')
log = logging.getLogger(__name__)

COMPONENT_NAME = "Amrit Sarovar"
API_ENDPOINT = "/report_jsm/amritsarovar-stats"
SCORE_KEY = "marks" # Marks are calculated at state level (district rows)
COUNT_KEY = "actual_count"
NAME_KEY = "name"
MAX_MARKS = 20.0

# --- Simplified Data Processing Function ---
def process_amrit_sarovar_district_data(data: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Processes a single district entry from the Amrit Sarovar API response."""
    if not data:
        return None
    processed = {
        "name": safe_get(data, [NAME_KEY]),
        COUNT_KEY: safe_get(data, [COUNT_KEY], 0),
        "target": safe_get(data, ["target"], "N/A")
    }
    # Ensure marks are numeric or "N/A" before adding
    marks_val = safe_get(data, [SCORE_KEY])
    processed[SCORE_KEY] = marks_val if isinstance(marks_val, (int, float)) else "N/A"
    return processed
# --- End Simplified Function ---


def analyze(district_name: str, report_date: str) -> Optional[Dict[str, Any]]:
    """
    Analyzes Amrit Sarovar data for a specific district, including
    full data for top/bottom state performers by score and count.
    Block-level data is excluded.
    Note: Date parameter is ignored by the backend endpoint for this component.
    """
    if not district_name:
        log.error("District name is required.")
        return None

    district_name_upper = district_name.strip().upper()
    analysis_result: Dict[str, Any] = {
        "component": COMPONENT_NAME,
        "selected_district": district_name,
        "report_date": report_date, # Keep date for consistency, though unused by API
        "explanation": "",
        "district_data": None,
        # "block_level_data": [], # <<< REMOVED THIS KEY
        "state_level_comparison": {
            "by_score": {"top_performer": None, "bottom_performer": None},
            "by_count": {"top_performer": None, "bottom_performer": None}
        }
    }

    # 1. Fetch State-Level Data (for comparison and district data)
    log.info(f"Fetching state-level {COMPONENT_NAME} data (date is ignored)...")
    state_params = {} # No date or district filter for state view
    state_data_raw = fetch_api_data(API_ENDPOINT, params=state_params)
    state_results = safe_get(state_data_raw, ["details"], []) # district list

    if not state_results:
        log.error(f"Could not fetch or parse state-level {COMPONENT_NAME} data.")
        analysis_result["explanation"] = f"Error: Could not retrieve state-level {COMPONENT_NAME} data for comparison."
        # Return early if state data fails, as district data comes from it
        return analysis_result
    else:
        log.info(f"Fetched {len(state_results)} district results for {COMPONENT_NAME} state-level comparison.")

    # 2. Extract Selected District's Data from State Results
    selected_district_state_data = find_district_data(state_results, district_name_upper, name_key=NAME_KEY)

    if not selected_district_state_data:
        log.warning(f"Data for selected district '{district_name}' not found in state-level {COMPONENT_NAME} results.")
        analysis_result["explanation"] += f" Warning: Data for '{district_name}' not found in the state-level {COMPONENT_NAME} results."
        # District data is None, but state comparison can still proceed
        analysis_result["district_data"] = None
    else:
        log.info(f"Found state-level {COMPONENT_NAME} data for selected district: {district_name}")
        # Process using the simplified function
        analysis_result["district_data"] = process_amrit_sarovar_district_data(selected_district_state_data)


    # ---- STEP 3: BLOCK LEVEL DATA FETCHING REMOVED ----
    # log.info(f"Fetching block list for district: {district_name} (for {COMPONENT_NAME})")
    # blocks_raw = fetch_api_data("/report_jsm/blocks", params={'district': district_name})
    # block_list = safe_get(blocks_raw, ['blocks'], [])
    # ... entire block/panchayat processing loop is removed ...
    # analysis_result["block_level_data"] = processed_blocks # <<< THIS LINE REMOVED


    # ---- STEP 4: Calculate State-Level Comparison (Using Simplified Processor) ----
    # Filter state_results to ensure marks are valid numbers before comparison
    valid_score_districts = [d for d in state_results if isinstance(safe_get(d, [SCORE_KEY]), (int, float)) and safe_get(d, [SCORE_KEY]) == safe_get(d, [SCORE_KEY])] # Check for NaN
    comparison_score = get_top_bottom_performers_full(valid_score_districts, score_key=SCORE_KEY, name_key=NAME_KEY)
    # Process using simplified function
    top_score_processed = process_amrit_sarovar_district_data(comparison_score.get("top")) if comparison_score.get("top") else None
    bottom_score_processed = process_amrit_sarovar_district_data(comparison_score.get("bottom")) if comparison_score.get("bottom") else None
    analysis_result["state_level_comparison"]["by_score"]["top_performer"] = top_score_processed
    analysis_result["state_level_comparison"]["by_score"]["bottom_performer"] = bottom_score_processed
    log.info(f"{COMPONENT_NAME} State comparison by SCORE - Top: {safe_get(top_score_processed, ['name'])}, Bottom: {safe_get(bottom_score_processed, ['name'])}")

    # Comparison by Count
    comparison_count = get_top_bottom_by_count_full(state_results, count_key=COUNT_KEY, name_key=NAME_KEY)
    # Process using simplified function
    top_count_processed = process_amrit_sarovar_district_data(comparison_count.get("top")) if comparison_count.get("top") else None
    bottom_count_processed = process_amrit_sarovar_district_data(comparison_count.get("bottom")) if comparison_count.get("bottom") else None
    analysis_result["state_level_comparison"]["by_count"]["top_performer"] = top_count_processed
    analysis_result["state_level_comparison"]["by_count"]["bottom_performer"] = bottom_count_processed
    log.info(f"{COMPONENT_NAME} State comparison by COUNT - Top: {safe_get(top_count_processed, ['name'])}, Bottom: {safe_get(bottom_count_processed, ['name'])}")


    # ---- STEP 5: Add Explanations (Block explanation removed) ----
    explanation_parts = []
    district_data = analysis_result["district_data"]

    if district_data:
        score = format(safe_get(district_data, [SCORE_KEY], 0.0), '.2f') if safe_get(district_data, [SCORE_KEY], "N/A") != "N/A" else "N/A"
        actual = format(safe_get(district_data, [COUNT_KEY], 0), ',')
        target = format(safe_get(district_data, ["target"], 0), ',') if safe_get(district_data, ["target"], "N/A") != "N/A" else "N/A"
        explanation_parts.append(f"For {COMPONENT_NAME}, {district_name} reported {actual} sites completed against a target of {target}. The district-level score is {score} out of {MAX_MARKS:.0f} (calculated based on state-level performance).")
    else:
        # Append the warning added earlier if district data wasn't found
        if f"Warning: Data for '{district_name}'" in analysis_result["explanation"]:
             explanation_parts.append(analysis_result["explanation"]) # Use the existing warning
        else:
             explanation_parts.append(f"Could not retrieve specific {COMPONENT_NAME} performance data for {district_name}.")


    # --- Block Level Explanation REMOVED ---
    # if analysis_result["block_level_data"]: # This key no longer exists
    #     explanation_parts.append(f"The analysis includes a breakdown of {COMPONENT_NAME} counts for {len(analysis_result['block_level_data'])} blocks within {district_name}. For each block, the top 5 panchayats by count are listed. Note: Marks are only calculated at the district level.")
    # else:
    #     explanation_parts.append(f"Block-level breakdown and top panchayats for {COMPONENT_NAME} in {district_name} could not be retrieved.")

    # Explanation for Score Comparison
    comp_score = analysis_result["state_level_comparison"]["by_score"]
    if comp_score.get("top_performer") and comp_score.get("bottom_performer"):
        top_name = safe_get(comp_score['top_performer'], ['name'], 'N/A')
        top_score_val = format(safe_get(comp_score['top_performer'], [SCORE_KEY], 0.0), '.2f') if safe_get(comp_score['top_performer'], [SCORE_KEY], "N/A") != "N/A" else "N/A"
        bot_name = safe_get(comp_score['bottom_performer'], ['name'], 'N/A')
        bot_score_val = format(safe_get(comp_score['bottom_performer'], [SCORE_KEY], 0.0), '.2f') if safe_get(comp_score['bottom_performer'], [SCORE_KEY], "N/A") != "N/A" else "N/A"
        explanation_parts.append(f"State-wide (by SCORE), the top performing district for {COMPONENT_NAME} is {top_name} (Score: {top_score_val}) and the bottom performer is {bot_name} (Score: {bot_score_val}).")
    else:
         explanation_parts.append(f"State-wide top/bottom performers by SCORE for {COMPONENT_NAME} could not be determined.")

    # Explanation for Count Comparison
    comp_count = analysis_result["state_level_comparison"]["by_count"]
    if comp_count.get("top_performer") and comp_count.get("bottom_performer"):
        top_name = safe_get(comp_count['top_performer'], ['name'], 'N/A')
        top_count_val = format(safe_get(comp_count['top_performer'], [COUNT_KEY], 0), ',')
        bot_name = safe_get(comp_count['bottom_performer'], ['name'], 'N/A')
        bot_count_val = format(safe_get(comp_count['bottom_performer'], [COUNT_KEY], 0), ',')
        explanation_parts.append(f"State-wide (by COUNT), the district with the most {COMPONENT_NAME} sites is {top_name} (Count: {top_count_val}) and the district with the fewest is {bot_name} (Count: {bot_count_val}).")
    else:
         explanation_parts.append(f"State-wide top/bottom districts by COUNT for {COMPONENT_NAME} could not be determined.")


    # Update the main explanation field, handling potential leading warning space
    full_explanation = " ".join(explanation_parts)
    if analysis_result["explanation"] and analysis_result["explanation"].startswith(" Warning:"):
         # If we already had a warning, use it as the start and append others
         analysis_result["explanation"] += " " + " ".join(explanation_parts[1:]) # Skip the duplicate warning
    else:
         analysis_result["explanation"] = full_explanation


    return analysis_result


# __main__ block modified to save output
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=f"Analyze JSM {COMPONENT_NAME} Data for a District.")
    parser.add_argument("-d", "--district", required=True, help="Name of the district to analyze.")
    parser.add_argument("-dt", "--date", required=True, help="Report date (YYYY-MM-DD). NOTE: This date is IGNORED by the Amrit Sarovar API endpoint.")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging.")
    parser.add_argument("-o", "--output_dir", default=".", help="Directory to save the output JSON file (default: current directory).")

    args = parser.parse_args()

    if args.debug:
        log.setLevel(logging.DEBUG)
        logging.getLogger('utils').setLevel(logging.DEBUG) # Also set utils log level if needed

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)

    log.info(f"Starting {COMPONENT_NAME} analysis for District: {args.district}, Date: {args.date} (Date is ignored by API)")
    result = analyze(args.district, args.date)

    # --- Save to File ---
    if result:
        # Create a sanitized filename
        district_slug = args.district.lower().replace(" ", "_").replace("/", "_")
        component_slug = COMPONENT_NAME.lower().replace(" ", "_")
        # Use the report date in the filename for uniqueness, even if API ignores it
        filename_date = args.date
        output_filename = f"analysis_{component_slug}_{district_slug}_{filename_date}.json"
        output_path = os.path.join(args.output_dir, output_filename)

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                # Use json.dump for writing directly to file handle
                json.dump(result, f, indent=2, ensure_ascii=False)
            log.info(f"Analysis complete. Results saved to: {output_path}")
            # Optionally print a success message to console as well
            print(f"Successfully generated analysis and saved to {output_path}")
        except IOError as e:
            log.error(f"Failed to write analysis results to {output_path}: {e}")
            # Print the JSON to console as a fallback if saving fails
            print("Error saving file. Printing JSON to console instead:")
            print(json.dumps(result, indent=2, ensure_ascii=False)) # Ensure proper display of non-ASCII chars
        except Exception as e:
            log.error(f"An unexpected error occurred during file writing: {e}")
            print("Error saving file. Printing JSON to console instead:")
            print(json.dumps(result, indent=2, ensure_ascii=False))

    else:
        log.error("Analysis failed. No output file generated.")
        # Create the error JSON structure without block data
        error_output = {
            "component": COMPONENT_NAME,
            "selected_district": args.district,
            "report_date": args.date,
            "error": "Failed to generate analysis. Check logs.",
            "district_data": None,
            # "block_level_data": [], # <<< REMOVED
            "state_level_comparison": {"by_score":{}, "by_count":{}}
        }
        # Print error json to console
        print(json.dumps(error_output, indent=2))