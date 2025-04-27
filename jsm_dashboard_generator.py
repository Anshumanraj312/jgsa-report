#!/usr/bin/env python3
# jsm_dashboard_generator.py

import os
import sys
import json
import logging
import argparse
import datetime
import subprocess
from typing import Dict, Any, List, Optional, Tuple
import traceback
import asyncio
from datetime import datetime, timedelta
import math
# Third-party imports
import requests
import anthropic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('jsm_dashboard.log')
    ]
)
logger = logging.getLogger('jsm_dashboard')

# Constants
OUTPUT_DIR = "output"
PDF_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "pdf")
JSON_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "json")
HTML_OUTPUT_DIR = os.path.join(OUTPUT_DIR, "html")
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "template.html")

# Create output directories
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(PDF_OUTPUT_DIR, exist_ok=True)
os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
os.makedirs(HTML_OUTPUT_DIR, exist_ok=True)

# =============== DATA COLLECTION FUNCTIONS ===============

def fetch_api_data(endpoint: str, params: Optional[Dict[str, Any]] = None, base_url: str = "https://dashboard.nregsmp.org/api") -> Optional[Dict[str, Any]]:
    """
    Fetches data from a specified API endpoint.
    
    Args:
        endpoint: API endpoint path
        params: Query parameters
        base_url: Base API URL
    
    Returns:
        JSON response as dictionary or None if error
    """
    full_url = f"{base_url}{endpoint}"
    try:
        logger.info(f"Fetching data from: {full_url} with params: {params}")
        response = requests.get(full_url, params=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        
        # Check for API error responses
        if isinstance(data, dict) and (data.get("error") or data.get("detail")):
            error_msg = data.get("error") or data.get("detail")
            logger.error(f"API error: {error_msg}")
            return None
            
        logger.info(f"Successfully fetched data from {endpoint}")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from {full_url}: {e}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Error decoding JSON from {full_url}")
        return None

def get_district_kpis(district_name: str, report_date: str) -> Dict[str, Any]:
    """
    Fetch district KPI data including rankings, scores, etc.
    
    Args:
        district_name: Name of the district
        report_date: Report date (YYYY-MM-DD)
    
    Returns:
        Dictionary containing KPI data
    """
    logger.info(f"Fetching KPI data for {district_name} on {report_date}")
    
    # Execute the analyze_district_kpis.py script
    try:
        # Create output filename
        output_file = os.path.join(JSON_OUTPUT_DIR, f"kpi_{district_name.lower().replace(' ', '_')}_{report_date.replace('-', '')}.json")
        
        # Check if the file already exists
        if os.path.exists(output_file) and os.path.isfile(output_file):
            logger.info(f"Using existing KPI data from {output_file}")
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        # Run the script to generate KPI data
        cmd = ["python", "analyze_district_kpis.py", "-d", district_name, "-dt", report_date, "-o", output_file]
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error executing KPI script: {result.stderr}")
            raise Exception(f"KPI script failed: {result.stderr}")
        
        # Load the generated JSON file
        with open(output_file, 'r', encoding='utf-8') as f:
            kpi_data = json.load(f)
        
        logger.info(f"Successfully fetched KPI data for {district_name}")
        return kpi_data
    except Exception as e:
        logger.error(f"Error getting KPI data: {e}")
        logger.error(traceback.format_exc())
        raise  # Propagate the error so we know something went wrong


def get_amrit_sarovar_data(district_name: str, report_date: str) -> Dict[str, Any]:
    """
    Fetch Amrit Sarovar data for the district by running the analysis script
    and reading its output file.

    Args:
        district_name: Name of the district
        report_date: Report date (YYYY-MM-DD)

    Returns:
        Dictionary containing Amrit Sarovar analysis
    """
    logger.info(f"Fetching Amrit Sarovar data for {district_name} on {report_date}")

    try:
        # Define the expected output filename based on the pattern in analyze_amrit_sarovar.py
        district_slug = district_name.lower().replace(" ", "_").replace("/", "_")
        component_slug = "amrit_sarovar" # Matches the component name used in the script
        filename_date = report_date # Use report date for filename consistency
        output_filename = f"analysis_{component_slug}_{district_slug}_{filename_date}.json"
        output_file = os.path.join(JSON_OUTPUT_DIR, output_filename) # Save in the standard JSON output dir

        # Check if the file already exists
        if os.path.exists(output_file) and os.path.isfile(output_file):
            logger.info(f"Using existing Amrit Sarovar data from {output_file}")
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)

        # Run the script, telling it WHERE to save the output file
        # Pass JSON_OUTPUT_DIR as the output directory
        cmd = ["python", "analyze_amrit_sarovar.py", "-d", district_name, "-dt", report_date, "-o", JSON_OUTPUT_DIR]
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=False) # Use check=False to handle errors manually

        # Check if the script executed successfully
        if result.returncode != 0:
            logger.error(f"Error executing Amrit Sarovar script (Return Code: {result.returncode}):")
            logger.error(f"Stderr: {result.stderr}")
            logger.error(f"Stdout: {result.stdout}") # Log stdout too, it might contain useful info
            raise Exception(f"Amrit Sarovar script failed: {result.stderr or result.stdout}")

        # If script succeeded, check if the output file was created
        if os.path.exists(output_file) and os.path.isfile(output_file):
            logger.info(f"Amrit Sarovar script succeeded. Loading data from {output_file}")
            # Load the generated JSON file
            with open(output_file, 'r', encoding='utf-8') as f:
                as_data = json.load(f)
            logger.info(f"Successfully loaded Amrit Sarovar data from {output_file}")
            return as_data
        else:
            # This case indicates the script reported success (return code 0) but didn't create the file
            logger.error(f"Amrit Sarovar script ran successfully, but output file not found at {output_file}")
            logger.error(f"Script stdout: {result.stdout}") # Log stdout which might explain why
            raise Exception(f"Amrit Sarovar script finished but output file '{output_file}' is missing.")

    except Exception as e:
        logger.error(f"Error getting Amrit Sarovar data: {e}")
        logger.error(traceback.format_exc())
        # Return a minimal error structure or raise, depending on desired handling
        # For now, re-raise to make it clear the process failed
        raise RuntimeError(f"Failed to get Amrit Sarovar data: {e}")

def get_dugwell_data(district_name: str, report_date: str) -> Dict[str, Any]:
    """
    Fetch Dugwell Recharge data for the district.
    
    Args:
        district_name: Name of the district
        report_date: Report date (YYYY-MM-DD)
    
    Returns:
        Dictionary containing Dugwell analysis
    """
    logger.info(f"Fetching Dugwell data for {district_name} on {report_date}")
    
    try:
        # Create output filename
        output_file = os.path.join(JSON_OUTPUT_DIR, f"dugwell_{district_name.lower().replace(' ', '_')}_{report_date.replace('-', '')}.json")
        
        # Check if the file already exists
        if os.path.exists(output_file):
            logger.info(f"Using existing Dugwell data from {output_file}")
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        # Run the script
        cmd = ["python", "analyze_dugwell.py", "-d", district_name, "-dt", report_date, "-o", output_file]
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error executing Dugwell script: {result.stderr}")
            raise Exception(f"Dugwell script failed: {result.stderr}")
        
        # Load the generated JSON file
        with open(output_file, 'r', encoding='utf-8') as f:
            dugwell_data = json.load(f)
        
        logger.info(f"Successfully fetched Dugwell data for {district_name}")
        return dugwell_data
    except Exception as e:
        logger.error(f"Error getting Dugwell data: {e}")
        logger.error(traceback.format_exc())
        # Return a minimal structure
        return {
            "component": "Dugwell Recharge",
            "selected_district": district_name,
            "report_date": report_date,
            "previous_report_date": (datetime.strptime(report_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"),
            "explanation": f"Error fetching Dugwell data: {str(e)}",
            "district_data": None,
            "state_level_summary_today": {"by_score": {}, "by_count": {}},
            "block_level_comparison": []
        }

def get_farm_ponds_data(district_name: str, report_date: str) -> Dict[str, Any]:
    """
    Fetch Farm Ponds data for the district.
    
    Args:
        district_name: Name of the district
        report_date: Report date (YYYY-MM-DD)
    
    Returns:
        Dictionary containing Farm Ponds analysis
    """
    logger.info(f"Fetching Farm Ponds data for {district_name} on {report_date}")
    
    try:
        # Create output filename
        output_file = os.path.join(JSON_OUTPUT_DIR, f"farm_ponds_{district_name.lower().replace(' ', '_')}_{report_date.replace('-', '')}.json")
        
        # Check if the file already exists
        if os.path.exists(output_file):
            logger.info(f"Using existing Farm Ponds data from {output_file}")
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        # Run the script
        cmd = ["python", "analyze_farm_ponds.py", "-d", district_name, "-dt", report_date, "-o", output_file]
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error executing Farm Ponds script: {result.stderr}")
            raise Exception(f"Farm Ponds script failed: {result.stderr}")
        
        # Load the generated JSON file
        with open(output_file, 'r', encoding='utf-8') as f:
            fp_data = json.load(f)
        
        logger.info(f"Successfully fetched Farm Ponds data for {district_name}")
        return fp_data
    except Exception as e:
        logger.error(f"Error getting Farm Ponds data: {e}")
        logger.error(traceback.format_exc())
        # Return a minimal structure
        return {
            "component": "Farm Ponds",
            "max_marks": 30.0,
            "selected_district": district_name,
            "report_date": report_date,
            "previous_report_date": (datetime.strptime(report_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"),
            "explanation": f"Error fetching Farm Ponds data: {str(e)}",
            "district_data": None,
            "state_level_summary_today": {"by_score": {}, "by_count": {}},
            "block_level_comparison": []
        }

def get_old_works_data(district_name: str, report_date: str) -> Dict[str, Any]:
    """
    Fetch Old Works (NRM) data for the district.
    
    Args:
        district_name: Name of the district
        report_date: Report date (YYYY-MM-DD)
    
    Returns:
        Dictionary containing Old Works analysis
    """
    logger.info(f"Fetching Old Works data for {district_name} on {report_date}")
    
    try:
        # Create output filename
        output_file = os.path.join(JSON_OUTPUT_DIR, f"old_works_{district_name.lower().replace(' ', '_')}_{report_date.replace('-', '')}.json")
        
        # Check if the file already exists
        if os.path.exists(output_file):
            logger.info(f"Using existing Old Works data from {output_file}")
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        # Run the script
        cmd = ["python", "analyze_old_works.py", "-d", district_name, "-dt", report_date, "-o", output_file]
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error executing Old Works script: {result.stderr}")
            raise Exception(f"Old Works script failed: {result.stderr}")
        
        # Load the generated JSON file
        with open(output_file, 'r', encoding='utf-8') as f:
            ow_data = json.load(f)
        
        logger.info(f"Successfully fetched Old Works data for {district_name}")
        return ow_data
    except Exception as e:
        logger.error(f"Error getting Old Works data: {e}")
        logger.error(traceback.format_exc())
        # Return a minimal structure
        return {
            "component": "Old Works (NRM)",
            "max_marks": 20.0,
            "selected_district": district_name,
            "report_date": report_date,
            "previous_report_date": (datetime.strptime(report_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d"),
            "explanation": f"Error fetching Old Works data: {str(e)}",
            "district_data": None,
            "state_level_summary_today": {"by_score": {}, "by_count": {}},
            "block_level_comparison": []
        }

def get_mybharat_data(district_name: str, report_date: str) -> Dict[str, Any]:
    """
    Fetch MyBharat data for the district.
    
    Args:
        district_name: Name of the district
        report_date: Report date (YYYY-MM-DD)
    
    Returns:
        Dictionary containing MyBharat analysis
    """
    logger.info(f"Fetching MyBharat data for {district_name} on {report_date}")
    
    try:
        # Create output filename
        output_file = os.path.join(JSON_OUTPUT_DIR, f"mybharat_{district_name.lower().replace(' ', '_')}_{report_date.replace('-', '')}.json")
        
        # Check if the file already exists
        if os.path.exists(output_file) and os.path.isfile(output_file):
            logger.info(f"Using existing MyBharat data from {output_file}")
            with open(output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        
        # Run the script (without -o parameter since it's not supported)
        cmd = ["python", "analyze_mybharat.py", "-d", district_name, "-dt", report_date]
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            logger.error(f"Error executing MyBharat script: {result.stderr}")
            raise Exception(f"MyBharat script failed: {result.stderr}")
        
        # Try to parse the output JSON
        try:
            mb_data = json.loads(result.stdout)
            logger.info("Successfully parsed JSON output from MyBharat script")
            
            # Save the data for future use
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(mb_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved MyBharat data to {output_file}")
            
            return mb_data
        except json.JSONDecodeError:
            logger.error(f"Error parsing JSON from MyBharat script output: {result.stdout}")
            raise Exception("Failed to parse MyBharat script output")
        
    except Exception as e:
        logger.error(f"Error getting MyBharat data: {e}")
        logger.error(traceback.format_exc())
        raise  # Propagate the error so we know something went wrong

# =============== DATA PROCESSING FUNCTIONS ===============

def get_grade_label(value: float, max_value: float, state_stats: Dict = None) -> str:
    """
    Returns a grade label based on statistical position relative to state-wide performance.
    
    Args:
        value: The value to grade
        max_value: The maximum possible value
        state_stats: Optional dict with average, median statistics
    
    Returns:
        Grade label string
    """
    if not isinstance(value, (int, float)) or not isinstance(max_value, (int, float)) or max_value == 0:
        return "N/A"
    
    # If state statistics are provided, use relative grading
    if state_stats and isinstance(state_stats, dict):
        avg = state_stats.get('average', 0)
        median = state_stats.get('median', 0)
        
        # If both exist, use them for statistical grading
        if avg and median:
            # Grade based on relation to state statistics
            if value >= (avg * 1.25):  # 25% or more above average
                return "उत्कृष्ट"
            elif value >= avg:  # Above average
                return "अच्छा"
            elif value >= median:  # Above median
                return "औसत"
            elif value >= (median * 0.7):  # At least 70% of median
                return "निम्न"
            else:  # Below 70% of median
                return "अति निम्न"
    
    # Fallback to percentage-based grading if no state stats
    percentage = (value / max_value) * 100
    
    if percentage >= 90:
        return "उत्कृष्ट"
    elif percentage >= 70:
        return "अच्छा"
    elif percentage >= 50:
        return "औसत"
    elif percentage >= 30:
        return "निम्न"
    else:
        return "अति निम्न"

def get_grade_class(value: float, max_value: float, state_stats: Dict = None) -> str:
    """
    Returns a CSS class for the grade badge based on statistics.
    
    Args:
        value: The value to grade
        max_value: The maximum possible value
        state_stats: Optional dict with average, median statistics
    
    Returns:
        CSS class string
    """
    if not isinstance(value, (int, float)) or not isinstance(max_value, (int, float)) or max_value == 0:
        return "grade-badge"
    
    # If state statistics are provided, use relative grading
    if state_stats and isinstance(state_stats, dict):
        avg = state_stats.get('average', 0)
        median = state_stats.get('median', 0)
        
        # If both exist, use them for statistical grading
        if avg and median:
            # Grade based on relation to state statistics
            if value >= (avg * 1.25):  # 25% or more above average
                return "grade-badge excellent"
            elif value >= avg:  # Above average
                return "grade-badge good"
            elif value >= median:  # Above median
                return "grade-badge average"
            elif value >= (median * 0.7):  # At least 70% of median
                return "grade-badge poor"
            else:  # Below 70% of median
                return "grade-badge very-poor"
    
    # Fallback to percentage-based grading if no state stats
    percentage = (value / max_value) * 100
    
    if percentage >= 90:
        return "grade-badge excellent"
    elif percentage >= 70:
        return "grade-badge good"
    elif percentage >= 50:
        return "grade-badge average"
    elif percentage >= 30:
        return "grade-badge poor"
    else:
        return "grade-badge very-poor"

def get_completion_status_text(comparison_text: str) -> str:
    """Convert the position comparison text to a more readable status."""
    if "Above Mean" in comparison_text and "Above Median" in comparison_text:
        return "उत्कृष्ट (राज्य औसत से बेहतर)"
    elif "Above Mean" in comparison_text or "Above Median" in comparison_text:
        return "अच्छा (औसत के करीब)"
    elif "Below Mean" in comparison_text and "Below Median" in comparison_text:
        return "निम्न (राज्य औसत से कम)"
    else:
        return "औसत"

def process_kpi_data(kpi_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process KPI data to format required by the template.
    
    Args:
        kpi_data: Raw KPI data
    
    Returns:
        Processed KPI data for template
    """
    logger.info("Processing KPI data for template")
    
    result = {}
    
    # Basic information
    result["district_name"] = kpi_data.get("district_name", "")
    result["report_date"] = kpi_data.get("report_date", "")
    result["previous_report_date"] = kpi_data.get("previous_report_date", "")
    result["current_datetime"] = datetime.now().strftime("%d-%m-%Y %H:%M")
    
    # Extract state context for statistical grading
    state_context = kpi_data.get("state_context", {})
    total_marks_stats = state_context.get("total_marks_stats", {})
    component_stats = state_context.get("component_stats", {})
    
    # Process KPIs
    kpis = kpi_data.get("kpis", {})
    processed_kpis = {}
    
    # Total Marks
    total_marks = kpis.get("total_marks", {})
    current_marks = total_marks.get("current")
    change = total_marks.get("change")
    
    processed_kpis["total_marks"] = {
        "current": round(current_marks, 2) if isinstance(current_marks, (int, float)) else "N/A",
        "previous": round(total_marks.get("previous", 0), 2) if isinstance(total_marks.get("previous"), (int, float)) else "N/A",
        "change": round(change, 2) if isinstance(change, (int, float)) else 0,
        "trend_icon": "▲" if change and change > 0 else ("▼" if change and change < 0 else "◆"),
        "trend_class": "trend-up" if change and change > 0 else ("trend-down" if change and change < 0 else "trend-neutral"),
        "grade": get_grade_label(current_marks if isinstance(current_marks, (int, float)) else 0, 100, total_marks_stats),
        "grade_class": get_grade_class(current_marks if isinstance(current_marks, (int, float)) else 0, 100, total_marks_stats)
    }
    
    # Rank
    rank = kpis.get("rank", {})
    processed_kpis["rank"] = {
        "current": rank.get("current", "N/A"),
        "previous": rank.get("previous", "N/A"),
        "change": rank.get("change", 0),
        "total_districts": rank.get("total_districts_ranked_today", 52)
    }
    
    # Farm Ponds
    farm_ponds = kpis.get("farm_ponds_completed", {})
    farm_ponds_current = farm_ponds.get("current", 0)
    farm_ponds_change = farm_ponds.get("change", 0)
    farm_ponds_stats = component_stats.get("farm_ponds", {})
    
    processed_kpis["farm_ponds"] = {
        "completed": farm_ponds_current if isinstance(farm_ponds_current, (int, float)) else 0,
        "previous": farm_ponds.get("previous", 0),
        "change": farm_ponds_change,
        "trend_icon": "▲" if farm_ponds_change and farm_ponds_change > 0 else ("▼" if farm_ponds_change and farm_ponds_change < 0 else "◆"),
        "trend_class": "trend-up" if farm_ponds_change and farm_ponds_change > 0 else ("trend-down" if farm_ponds_change and farm_ponds_change < 0 else "trend-neutral"),
        "marks": 0,  # Will be updated with component data
        "grade": "N/A",  # Will be updated with component data
        "grade_class": "grade-badge"  # Will be updated with component data
    }
    
    # Dugwell
    dugwell = kpis.get("dugwell_recharge_completed", {})
    dugwell_current = dugwell.get("current", 0)
    dugwell_change = dugwell.get("change", 0)
    dugwell_stats = component_stats.get("dugwell", {})
    
    processed_kpis["dugwell"] = {
        "completed": dugwell_current if isinstance(dugwell_current, (int, float)) else 0,
        "previous": dugwell.get("previous", 0),
        "change": dugwell_change,
        "trend_icon": "▲" if dugwell_change and dugwell_change > 0 else ("▼" if dugwell_change and dugwell_change < 0 else "◆"),
        "trend_class": "trend-up" if dugwell_change and dugwell_change > 0 else ("trend-down" if dugwell_change and dugwell_change < 0 else "trend-neutral"),
        "marks": 0,  # Will be updated with component data
        "grade": "N/A",  # Will be updated with component data
        "grade_class": "grade-badge"  # Will be updated with component data
    }
    
    # Amrit Sarovar
    amrit_sarovar = kpis.get("amrit_sarovar_completed", {})
    amrit_sarovar_current = amrit_sarovar.get("current", 0)
    amrit_sarovar_change = amrit_sarovar.get("change", 0)
    amrit_sarovar_stats = component_stats.get("amrit_sarovar", {})
    
    processed_kpis["amrit_sarovar"] = {
        "completed": amrit_sarovar_current if isinstance(amrit_sarovar_current, (int, float)) else 0,
        "previous": amrit_sarovar.get("previous", 0),
        "change": amrit_sarovar_change,
        "trend_icon": "▲" if amrit_sarovar_change and amrit_sarovar_change > 0 else ("▼" if amrit_sarovar_change and amrit_sarovar_change < 0 else "◆"),
        "trend_class": "trend-up" if amrit_sarovar_change and amrit_sarovar_change > 0 else ("trend-down" if amrit_sarovar_change and amrit_sarovar_change < 0 else "trend-neutral"),
        "marks": 0,  # Will be updated with component data
        "grade": "N/A",  # Will be updated with component data
        "grade_class": "grade-badge"  # Will be updated with component data
    }
    
    # Old Work
    old_work = kpis.get("old_work_completed", {})
    old_work_current = old_work.get("current", 0)
    old_work_change = old_work.get("change", 0)
    old_work_stats = {
        "average": component_stats.get("performance_target", {}).get("average", 0) + component_stats.get("performance_payment", {}).get("average", 0),
        "median": component_stats.get("performance_target", {}).get("median", 0) + component_stats.get("performance_payment", {}).get("median", 0)
    }
    
    processed_kpis["old_work"] = {
        "completed": old_work_current if isinstance(old_work_current, (int, float)) else 0,
        "previous": old_work.get("previous", 0),
        "change": old_work_change,
        "trend_icon": "▲" if old_work_change and old_work_change > 0 else ("▼" if old_work_change and old_work_change < 0 else "◆"),
        "trend_class": "trend-up" if old_work_change and old_work_change > 0 else ("trend-down" if old_work_change and old_work_change < 0 else "trend-neutral"),
        "marks": 0,  # Will be updated with component data
        "grade": "N/A",  # Will be updated with component data
        "grade_class": "grade-badge"  # Will be updated with component data
    }
    
    # MyBharat
    mybharat = kpis.get("mybharat_completed", {})
    mybharat_current = mybharat.get("current", 0)
    mybharat_change = mybharat.get("change", 0)
    mybharat_stats = component_stats.get("mybharat", {})
    
    processed_kpis["mybharat"] = {
        "completed": mybharat_current if isinstance(mybharat_current, (int, float)) else 0,
        "previous": mybharat.get("previous", 0),
        "change": mybharat_change,
        "trend_icon": "▲" if mybharat_change and mybharat_change > 0 else ("▼" if mybharat_change and mybharat_change < 0 else "◆"),
        "trend_class": "trend-up" if mybharat_change and mybharat_change > 0 else ("trend-down" if mybharat_change and mybharat_change < 0 else "trend-neutral"),
        "marks": 0,  # Will be updated with component data
        "grade": "N/A",  # Will be updated with component data
        "grade_class": "grade-badge"  # Will be updated with component data
    }
    
    result["kpi"] = processed_kpis
    
    # Store state context for later use
    result["state_context"] = state_context
    
    return result

def process_farm_ponds_data(fp_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process Farm Ponds data for template.
    
    Args:
        fp_data: Raw Farm Ponds data
    
    Returns:
        Processed Farm Ponds data
    """
    logger.info("Processing Farm Ponds data for template")
    
    result = {}
    
    # Extract state statistics if available
    state_context = fp_data.get("state_context", {})
    farm_ponds_stats = state_context.get("farm_ponds_stats", {})
    
    # Process district data
    district_data = fp_data.get("selected_district_comparison", {}).get("current_data", {})
    marks = district_data.get("marks", 0)
    actual_count = district_data.get("actual_count", 0)
    target = district_data.get("target", 0)
    
    result["district_data"] = {
        "name": district_data.get("name", ""),
        "marks": marks,
        "actual_count": actual_count,
        "target": target,
        "completion_percent": round((actual_count / target) * 100, 2) if target and target > 0 else 0,
        "grade": get_grade_label(marks, 30, farm_ponds_stats),
        "grade_class": get_grade_class(marks, 30, farm_ponds_stats)
    }
    
    # Process top performers by score
    top_score = fp_data.get("state_level_summary_today", {}).get("by_score", {}).get("top_performer", {})
    result["top_score"] = {
        "name": top_score.get("name", ""),
        "marks": top_score.get("marks", 0),
        "actual_count": top_score.get("actual_count", 0),
        "target": top_score.get("target", 0),
        "completion_percent": round((top_score.get("actual_count", 0) / top_score.get("target", 1)) * 100, 2) if top_score.get("target", 0) > 0 else 0
    }
    
    # Process top performers by count
    top_count = fp_data.get("state_level_summary_today", {}).get("by_count", {}).get("top_performer", {})
    result["top_count"] = {
        "name": top_count.get("name", ""),
        "marks": top_count.get("marks", 0),
        "actual_count": top_count.get("actual_count", 0),
        "target": top_count.get("target", 0),
        "completion_percent": round((top_count.get("actual_count", 0) / top_count.get("target", 1)) * 100, 2) if top_count.get("target", 0) > 0 else 0
    }
    
    # Process district position
    district_position = fp_data.get("selected_district_position_vs_state", {})
    result["district_position_vs_state"] = {
        "score_comparison": district_position.get("score_comparison", "N/A"),
        "count_comparison": district_position.get("count_comparison", "N/A"),
        "completion_status": get_completion_status_text(district_position.get("score_comparison", ""))
    }
    
    # Process block data
    blocks = fp_data.get("block_level_comparison", [])
    processed_blocks = []
    
    for block in blocks:
        top_panchayat = "N/A"
        if block.get("top_5_panchayats") and len(block.get("top_5_panchayats")) > 0:
            top_panchayat_data = block["top_5_panchayats"][0]
            top_panchayat = f"{top_panchayat_data.get('name', '')} ({top_panchayat_data.get('actual_count', 0)})"
        
        change = block.get("actual_count_today", 0) - block.get("actual_count_daybefore", 0)
        change_text = f"+{change}" if change > 0 else str(change) if change < 0 else "0"
        
        processed_blocks.append({
            "name": block.get("name", ""),
            "actual_count_today": block.get("actual_count_today", 0),
            "actual_count_daybefore": block.get("actual_count_daybefore", 0),
            "change": change_text,
            "top_panchayat": top_panchayat
        })
    
    result["block_data"] = processed_blocks
    
    # Prepare chart data
    block_labels = [block.get("name", "") for block in processed_blocks]
    block_values = [block.get("actual_count_today", 0) for block in processed_blocks]
    
    result["chart_data"] = {
        "labels": json.dumps(block_labels),
        "values": json.dumps(block_values)
    }
    
    # Include state context if available
    if state_context:
        result["state_context"] = state_context
    
    return result

def process_amrit_sarovar_data(as_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process Amrit Sarovar data for template.
    
    Args:
        as_data: Raw Amrit Sarovar data
    
    Returns:
        Processed Amrit Sarovar data
    """
    logger.info("Processing Amrit Sarovar data for template")
    
    result = {}
    
    # Handle None data
    if not as_data:
        logger.error("Amrit Sarovar data is None")
        raise ValueError("Amrit Sarovar data is None")
    
    # Extract state statistics if available
    state_context = as_data.get("state_context", {})
    amrit_sarovar_stats = state_context.get("amrit_sarovar_stats", {})
    
    # Process district data
    district_data = as_data.get("district_data", {})
    if not district_data:
        logger.error("District data is missing in Amrit Sarovar data")
        raise ValueError("District data is missing in Amrit Sarovar data")
        
    marks = district_data.get("marks", 0)
    actual_count = district_data.get("actual_count", 0)
    target = district_data.get("target", 1)  # Avoid division by zero
    
    result["district_data"] = {
        "name": district_data.get("name", "Unknown"),
        "marks": marks,
        "actual_count": actual_count,
        "target": target,
        "completion_percent": round((actual_count / target) * 100, 2) if target and target > 0 else 0,
        "grade": get_grade_label(marks, 20, amrit_sarovar_stats),
        "grade_class": get_grade_class(marks, 20, amrit_sarovar_stats)
    }
    
    # Process top performers by score
    top_score = as_data.get("state_level_comparison", {}).get("by_score", {}).get("top_performer", {})
    if not top_score:
        logger.warning("Top score performer data is missing in Amrit Sarovar data")
        top_score = {}
        
    result["top_score"] = {
        "name": top_score.get("name", "N/A"),
        "marks": top_score.get("marks", 0),
        "actual_count": top_score.get("actual_count", 0),
        "target": top_score.get("target", 1),
        "completion_percent": round((top_score.get("actual_count", 0) / top_score.get("target", 1)) * 100, 2) if top_score.get("target", 0) > 0 else 0
    }
    
    # Process top performers by count
    top_count = as_data.get("state_level_comparison", {}).get("by_count", {}).get("top_performer", {})
    if not top_count:
        logger.warning("Top count performer data is missing in Amrit Sarovar data")
        top_count = {}
        
    result["top_count"] = {
        "name": top_count.get("name", "N/A"),
        "marks": top_count.get("marks", 0),
        "actual_count": top_count.get("actual_count", 0),
        "target": top_count.get("target", 1),
        "completion_percent": round((top_count.get("actual_count", 0) / top_count.get("target", 1)) * 100, 2) if top_count.get("target", 0) > 0 else 0
    }
    
    # Include state context if available
    if state_context:
        result["state_context"] = state_context
    
    return result

def process_dugwell_data(dw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process Dugwell data for template.
    
    Args:
        dw_data: Raw Dugwell data
    
    Returns:
        Processed Dugwell data
    """
    logger.info("Processing Dugwell data for template")
    
    result = {}
    
    # Extract state statistics if available
    state_context = dw_data.get("state_context", {})
    dugwell_stats = state_context.get("dugwell_stats", {})
    
    # Process district data
    district_data = dw_data.get("selected_district_comparison", {}).get("current_data", {})
    marks = district_data.get("marks", 0)
    actual_count = district_data.get("actual_count", 0)
    target = district_data.get("target", 0)
    
    result["district_data"] = {
        "name": district_data.get("name", ""),
        "marks": marks,
        "actual_count": actual_count,
        "target": target,
        "completion_percent": round((actual_count / target) * 100, 2) if target and target > 0 else 0,
        "grade": get_grade_label(marks, 20, dugwell_stats),
        "grade_class": get_grade_class(marks, 20, dugwell_stats)
    }
    
    # Process top performers by score
    top_score = dw_data.get("state_level_summary_today", {}).get("by_score", {}).get("top_performer", {})
    result["top_score"] = {
        "name": top_score.get("name", ""),
        "marks": top_score.get("marks", 0),
        "actual_count": top_score.get("actual_count", 0),
        "target": top_score.get("target", 0),
        "completion_percent": round((top_score.get("actual_count", 0) / top_score.get("target", 1)) * 100, 2) if top_score.get("target", 0) > 0 else 0
    }
    
    # Process top performers by count
    top_count = dw_data.get("state_level_summary_today", {}).get("by_count", {}).get("top_performer", {})
    result["top_count"] = {
        "name": top_count.get("name", ""),
        "marks": top_count.get("marks", 0),
        "actual_count": top_count.get("actual_count", 0),
        "target": top_count.get("target", 0),
        "completion_percent": round((top_count.get("actual_count", 0) / top_count.get("target", 1)) * 100, 2) if top_count.get("target", 0) > 0 else 0
    }
    
    # Process district position
    district_position = dw_data.get("selected_district_position_vs_state", {})
    result["district_position_vs_state"] = {
        "score_comparison": district_position.get("score_comparison", "N/A"),
        "count_comparison": district_position.get("count_comparison", "N/A"),
        "status": get_completion_status_text(district_position.get("score_comparison", ""))
    }
    
    # Process block data
    blocks = dw_data.get("block_level_comparison", [])
    processed_blocks = []
    
    for block in blocks:
        top_panchayat = "N/A"
        if block.get("top_5_panchayats") and len(block.get("top_5_panchayats")) > 0:
            top_panchayat_data = block["top_5_panchayats"][0]
            top_panchayat = f"{top_panchayat_data.get('name', '')} ({top_panchayat_data.get('actual_count', 0)})"
        
        change = 0
        if block.get("actual_count_today") is not None and block.get("actual_count_daybefore") is not None:
            change = block.get("actual_count_today", 0) - block.get("actual_count_daybefore", 0)
        change_text = f"+{change}" if change > 0 else str(change) if change < 0 else "0"
        
        processed_blocks.append({
            "name": block.get("name", ""),
            "actual_count_today": block.get("actual_count_today", 0),
            "actual_count_daybefore": block.get("actual_count_daybefore", 0),
            "change": change_text,
            "top_panchayat": top_panchayat
        })
    
    result["block_data"] = processed_blocks
    
    # Prepare chart data
    block_labels = [block.get("name", "") for block in processed_blocks]
    block_values = [block.get("actual_count_today", 0) for block in processed_blocks]
    
    result["chart_data"] = {
        "labels": json.dumps(block_labels),
        "values": json.dumps(block_values)
    }
    
    # Include state context if available
    if state_context:
        result["state_context"] = state_context
    
    return result

# def process_old_works_data(ow_data: Dict[str, Any]) -> Dict[str, Any]:
#     """
#     Process Old Works data for template, using median for financial progress.
    
#     Args:
#         ow_data: Raw Old Works data
    
#     Returns:
#         Processed Old Works data
#     """
#     logger.info("Processing Old Works data for template")
    
#     result = {}
    
#     # Handle None data gracefully
#     if not ow_data:
#         logger.warning("Old Works data is None")
#         return {
#             "district_data": {
#                 "name": "Unknown",
#                 "overall_old_work_score": 0,
#                 "target_achievement_marks": 0,
#                 "financial_progress_marks": 0,
#                 "total_work_count": 0,
#                 "financial_progress_details": {
#                     "baseline_pending_lakhs": 0,
#                     "current_pending_lakhs": 0,
#                     "reduction_percentage": 0,
#                     "marks": 0
#                 },
#                 "individual_work_types": {},
#                 "grade": "N/A",
#                 "grade_class": "grade-badge"
#             },
#             "top_score": {
#                 "name": "N/A",
#                 "overall_old_work_score": 0,
#                 "target_achievement_marks": 0,
#                 "financial_progress_marks": 0,
#                 "total_work_count": 0
#             },
#             "top_count": {
#                 "name": "N/A",
#                 "total_work_count": 0
#             },
#             "block_data": [],
#             "chart_data": {"labels": "[]", "values": "[]"},
#             "financial_reduction_status": "N/A",
#             "financial_reduction_class": "average",
#             "financial_top_performer": {"name": "N/A", "reduction_percentage": 0, "marks": 0},
#             "state_avg_reduction": 0,
#             "state_median_reduction": 0
#         }
    
#     # Extract state statistics if available
#     state_context = ow_data.get("state_context", {})
#     old_works_stats = {
#         "average": state_context.get("performance_target", {}).get("average", 0) + state_context.get("performance_payment", {}).get("average", 0),
#         "median": state_context.get("performance_target", {}).get("median", 0) + state_context.get("performance_payment", {}).get("median", 0)
#     }
#     financial_stats = state_context.get("financial_stats", {})
    
#     # Process district data
#     district_data = ow_data.get("selected_district_comparison", {}).get("current_data", {})
#     if not district_data:
#         logger.warning("District data is missing in Old Works data")
#         return {
#             "district_data": {
#                 "name": "Unknown",
#                 "overall_old_work_score": 0,
#                 "target_achievement_marks": 0,
#                 "financial_progress_marks": 0,
#                 "total_work_count": 0,
#                 "financial_progress_details": {
#                     "baseline_pending_lakhs": 0,
#                     "current_pending_lakhs": 0,
#                     "reduction_percentage": 0,
#                     "marks": 0
#                 },
#                 "individual_work_types": {},
#                 "grade": "N/A",
#                 "grade_class": "grade-badge"
#             },
#             "top_score": {
#                 "name": "N/A",
#                 "overall_old_work_score": 0,
#                 "target_achievement_marks": 0,
#                 "financial_progress_marks": 0,
#                 "total_work_count": 0
#             },
#             "top_count": {
#                 "name": "N/A",
#                 "total_work_count": 0
#             },
#             "block_data": [],
#             "chart_data": {"labels": "[]", "values": "[]"},
#             "financial_reduction_status": "N/A",
#             "financial_reduction_class": "average",
#             "financial_top_performer": {"name": "N/A", "reduction_percentage": 0, "marks": 0},
#             "state_avg_reduction": 0,
#             "state_median_reduction": 0
#         }
    
#     # Extract state statistics if not already available
#     state_median_reduction = financial_stats.get("median_reduction", 0)
#     state_avg_reduction = financial_stats.get("mean_reduction", 0)
    
#     # If state stats not available, extract from available district data
#     if not state_median_reduction or not state_avg_reduction:
#         # Extract all available reductions from districts
#         reductions = []
#         all_districts = []
        
#         # Get districts from state_level_summary_today
#         state_summary = ow_data.get("state_level_summary_today", {})
#         if "by_score" in state_summary:
#             if "top_performer" in state_summary["by_score"]:
#                 all_districts.append(state_summary["by_score"]["top_performer"])
#             if "bottom_performer" in state_summary["by_score"]:
#                 all_districts.append(state_summary["by_score"]["bottom_performer"])
        
#         if "by_count" in state_summary:
#             if "top_performer" in state_summary["by_count"]:
#                 all_districts.append(state_summary["by_count"]["top_performer"])
#             if "bottom_performer" in state_summary["by_count"]:
#                 all_districts.append(state_summary["by_count"]["bottom_performer"])
        
#         # Extract financial reduction percentages
#         for district in all_districts:
#             if isinstance(district, dict) and "financial_progress_details" in district:
#                 fin_data = district.get("financial_progress_details", {})
#                 red_pct = fin_data.get("reduction_percentage", 0)
#                 if red_pct not in reductions:  # Avoid duplicates
#                     reductions.append(red_pct)
        
#         # Include current district's reduction
#         fin_details = district_data.get("financial_progress_details", {})
#         current_red = fin_details.get("reduction_percentage", 0)
#         if current_red not in reductions:
#             reductions.append(current_red)
        
#         # Calculate statistics if we have data
#         if reductions:
#             # Sort for median calculation
#             reductions.sort()
            
#             # Calculate median
#             mid = len(reductions) // 2
#             if len(reductions) % 2 == 0:
#                 state_median_reduction = (reductions[mid-1] + reductions[mid]) / 2
#             else:
#                 state_median_reduction = reductions[mid]
            
#             # Calculate mean (average)
#             state_avg_reduction = sum(reductions) / len(reductions)
    
#     # Process district data
#     score = district_data.get("overall_old_work_score", 0)
#     target_marks = district_data.get("target_achievement_marks", 0)
#     payment_marks = district_data.get("financial_progress_marks", 0)
#     total_work_count = district_data.get("total_work_count", 0)
#     financial_progress_details = district_data.get("financial_progress_details", {})
#     individual_work_types = district_data.get("individual_work_types", {})
    
#     result["district_data"] = {
#         "name": district_data.get("name", "Unknown"),
#         "overall_old_work_score": score,
#         "target_achievement_marks": target_marks,
#         "financial_progress_marks": payment_marks,
#         "total_work_count": total_work_count,
#         "financial_progress_details": financial_progress_details,
#         "individual_work_types": individual_work_types,
#         "grade": get_grade_label(score, 20, old_works_stats),
#         "grade_class": get_grade_class(score, 20, old_works_stats)
#     }
    
#     # Process top performers by score
#     state_summary = ow_data.get("state_level_summary_today", {})
#     top_score = state_summary.get("by_score", {}).get("top_performer", {})
#     result["top_score"] = {
#         "name": top_score.get("name", "N/A"),
#         "overall_old_work_score": top_score.get("overall_old_work_score", 0),
#         "target_achievement_marks": top_score.get("target_achievement_marks", 0),
#         "financial_progress_marks": top_score.get("financial_progress_marks", 0),
#         "total_work_count": top_score.get("total_work_count", 0)
#     }
    
#     # Process top performers by count
#     top_count = state_summary.get("by_count", {}).get("top_performer", {})
#     result["top_count"] = {
#         "name": top_count.get("name", "N/A"),
#         "total_work_count": top_count.get("total_work_count", 0)
#     }
    
#     # Ensure all_districts is defined for further processing
#     all_districts = []
#     if "by_score" in state_summary:
#         if "top_performer" in state_summary["by_score"]:
#             all_districts.append(state_summary["by_score"]["top_performer"])
#         if "bottom_performer" in state_summary["by_score"]:
#             all_districts.append(state_summary["by_score"]["bottom_performer"])
    
#     if "by_count" in state_summary:
#         if "top_performer" in state_summary["by_count"]:
#             all_districts.append(state_summary["by_count"]["top_performer"])
#         if "bottom_performer" in state_summary["by_count"]:
#             all_districts.append(state_summary["by_count"]["bottom_performer"])
    
#     # Process state category leaders
#     result["category_leaders"] = ow_data.get("state_category_leaders_today", {})
    
#     # Process block data
#     blocks = ow_data.get("block_level_comparison", [])
#     processed_blocks = []
    
#     for block in blocks:
#         # Process current work counts
#         work_counts = block.get("completed_works_by_type_till_today", {})
#         changes = block.get("completed_works_change_by_type", {})
        
#         processed_blocks.append({
#             "name": block.get("name", ""),
#             "Talab_Nirman": work_counts.get("Talab Nirman", 0),
#             "Check_Stop_Dam": work_counts.get("Check_Stop Dam", 0),
#             "Recharge_Pit": work_counts.get("Recharge Pit", 0),
#             "Koop_Nirman": work_counts.get("Koop Nirman", 0),
#             "Percolation_Talab": work_counts.get("Percolation Talab", 0),
#             "Khet_Talab": work_counts.get("Khet Talab", 0),
#             "Other_NRM_Work": work_counts.get("Other NRM Work", 0),
#             "changes": changes
#         })
    
#     result["block_data"] = processed_blocks
    
#     # Prepare chart data
#     block_labels = [block.get("name", "") for block in processed_blocks]
#     block_values = []
    
#     for block in processed_blocks:
#         total = (
#             block.get("Talab_Nirman", 0) +
#             block.get("Check_Stop_Dam", 0) +
#             block.get("Recharge_Pit", 0) +
#             block.get("Koop_Nirman", 0) +
#             block.get("Percolation_Talab", 0) +
#             block.get("Khet_Talab", 0) +
#             block.get("Other_NRM_Work", 0)
#         )
#         block_values.append(total)
    
#     result["chart_data"] = {
#         "labels": json.dumps(block_labels),
#         "values": json.dumps(block_values)
#     }
    
#     # Get current district's financial reduction percentage
#     fin_details = result["district_data"].get("financial_progress_details", {})
#     reduction_pct = fin_details.get("reduction_percentage", 0)
    
#     # Find top financial performer
#     top_financial = {"name": "N/A", "reduction_percentage": 0, "marks": 0}
#     max_reduction = -1
    
#     # Try to get from provided data
#     for district in all_districts:
#         if isinstance(district, dict) and "financial_progress_details" in district:
#             fin_data = district.get("financial_progress_details", {})
#             red_pct = fin_data.get("reduction_percentage", 0)
#             fin_marks = fin_data.get("marks", 0)
            
#             if red_pct > max_reduction:
#                 max_reduction = red_pct
#                 top_financial = {
#                     "name": district.get("name", "N/A"),
#                     "reduction_percentage": red_pct,
#                     "marks": fin_marks
#                 }
    
#     # UPDATED: Financial reduction status based on MEDIAN instead of average
#     if reduction_pct >= (state_median_reduction * 1.5):  # 50% above median
#         result["financial_reduction_status"] = "उत्कृष्ट"
#         result["financial_reduction_class"] = "high"
#     elif reduction_pct >= (state_median_reduction * 1.2):  # 20% above median
#         result["financial_reduction_status"] = "अच्छा"
#         result["financial_reduction_class"] = "above"
#     elif reduction_pct >= (state_median_reduction * 0.8):  # Within 20% of median
#         result["financial_reduction_status"] = "औसत"
#         result["financial_reduction_class"] = "average"
#     elif reduction_pct >= (state_median_reduction * 0.5):  # At least 50% of median
#         result["financial_reduction_status"] = "निम्न"
#         result["financial_reduction_class"] = "below"
#     else:  # Less than 50% of median
#         result["financial_reduction_status"] = "अति निम्न"
#         result["financial_reduction_class"] = "critical"
    
#     result["financial_top_performer"] = top_financial
#     result["state_avg_reduction"] = round(state_avg_reduction, 1)
#     result["state_median_reduction"] = round(state_median_reduction, 1)
    
#     # Bottom district information
#     bottom_score = state_summary.get("by_score", {}).get("bottom_performer", {})
#     result["bottom_score"] = {
#         "name": bottom_score.get("name", "N/A"),
#         "overall_old_work_score": bottom_score.get("overall_old_work_score", 0)
#     }
    
#     # Include state context if available
#     if state_context:
#         result["state_context"] = state_context
    
#     return result


def process_old_works_data(ow_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process Old Works data for template, using median for financial progress.
    
    Args:
        ow_data: Raw Old Works data
    
    Returns:
        Processed Old Works data
    """
    logger.info("Processing Old Works data for template")
    
    result = {}
    
    # Handle None data gracefully
    if not ow_data:
        logger.warning("Old Works data is None")
        return {
            "district_data": {
                "name": "Unknown",
                "overall_old_work_score": 0,
                "target_achievement_marks": 0,
                "financial_progress_marks": 0,
                "total_work_count": 0,
                "total_work_completed": 0,
                "financial_progress_details": {
                    "baseline_pending_lakhs": 0,
                    "current_pending_lakhs": 0,
                    "reduction_percentage": 0,
                    "marks": 0
                },
                "individual_work_types": {},
                "grade": "N/A",
                "grade_class": "grade-badge"
            },
            "top_score": {
                "name": "N/A",
                "overall_old_work_score": 0,
                "target_achievement_marks": 0,
                "financial_progress_marks": 0,
                "total_work_completed": 0
            },
            "top_count": {
                "name": "N/A",
                "total_work_completed": 0
            },
            "block_data": [],
            "chart_data": {"labels": "[]", "values": "[]"},
            "financial_reduction_status": "N/A",
            "financial_reduction_class": "average",
            "financial_top_performer": {"name": "N/A", "reduction_percentage": 0, "marks": 0},
            "state_avg_reduction": 0,
            "state_median_reduction": 0,
            "state_category_leaders_today": {}
        }
    
    # Extract state statistics if available
    state_context = ow_data.get("state_context", {})
    old_works_stats = {
        "average": state_context.get("performance_target", {}).get("average", 0) + state_context.get("performance_payment", {}).get("average", 0),
        "median": state_context.get("performance_target", {}).get("median", 0) + state_context.get("performance_payment", {}).get("median", 0)
    }
    financial_stats = state_context.get("financial_stats", {})
    
    # Process district data
    district_data = ow_data.get("selected_district_comparison", {}).get("current_data", {})
    if not district_data:
        logger.warning("District data is missing in Old Works data")
        district_data = {
            "name": "Unknown",
            "overall_old_work_score": 0,
            "total_work_count": 0,
            "total_work_completed": 0, 
            "target_achievement_marks": 0,
            "financial_progress_marks": 0,
            "financial_progress_details": {
                "baseline_pending_lakhs": 0,
                "current_pending_lakhs": 0,
                "reduction_percentage": 0,
                "marks": 0
            },
            "individual_work_types": {}
        }
    
    # Extract state statistics if not already available
    state_median_reduction = financial_stats.get("median_reduction", 0)
    state_avg_reduction = financial_stats.get("mean_reduction", 0)
    
    # Process district data
    result["district_data"] = district_data
    
    # Process top performers by score
    state_summary = ow_data.get("state_level_summary_today", {})
    top_score = state_summary.get("by_score", {}).get("top_performer", {})
    result["top_score"] = top_score
    
    # Process top performers by count (of completed works)
    top_count = state_summary.get("by_count", {}).get("top_performer", {})
    result["top_count"] = top_count
    
    # Process state category leaders
    result["state_category_leaders_today"] = ow_data.get("state_category_leaders_today", {})
    
    # Process block data
    blocks = ow_data.get("block_level_comparison", [])
    result["block_data"] = blocks
    
    # Prepare chart data for blocks based on completed works
    block_labels = [block.get("name", "") for block in blocks]
    block_values = []
    
    for block in blocks:
        # Calculate total completed works for each block
        completed_works = block.get("completed_works_by_type_till_today", {})
        total = sum(completed_works.values()) if isinstance(completed_works, dict) else 0
        block_values.append(total)
    
    result["chart_data"] = {
        "labels": json.dumps(block_labels),
        "values": json.dumps(block_values)
    }
    
    # Get current district's financial reduction percentage
    fin_details = district_data.get("financial_progress_details", {})
    reduction_pct = fin_details.get("reduction_percentage", 0)
    
    # Find top financial performer
    top_financial = {"name": "N/A", "reduction_percentage": 0, "marks": 0}
    max_reduction = -1
    
    # Try to get from provided data
    top_score_district = state_summary.get("by_score", {}).get("top_performer", {})
    if isinstance(top_score_district, dict) and "financial_progress_details" in top_score_district:
        fin_data = top_score_district.get("financial_progress_details", {})
        red_pct = fin_data.get("reduction_percentage", 0)
        fin_marks = fin_data.get("marks", 0)
        
        if red_pct > max_reduction:
            max_reduction = red_pct
            top_financial = {
                "name": top_score_district.get("name", "N/A"),
                "reduction_percentage": red_pct,
                "marks": fin_marks
            }
    
    # UPDATED: Financial reduction status based on MEDIAN instead of average
    if reduction_pct >= (state_median_reduction * 1.5):  # 50% above median
        result["financial_reduction_status"] = "उत्कृष्ट"
        result["financial_reduction_class"] = "high"
    elif reduction_pct >= (state_median_reduction * 1.2):  # 20% above median
        result["financial_reduction_status"] = "अच्छा"
        result["financial_reduction_class"] = "above"
    elif reduction_pct >= (state_median_reduction * 0.8):  # Within 20% of median
        result["financial_reduction_status"] = "औसत"
        result["financial_reduction_class"] = "average"
    elif reduction_pct >= (state_median_reduction * 0.5):  # At least 50% of median
        result["financial_reduction_status"] = "निम्न"
        result["financial_reduction_class"] = "below"
    else:  # Less than 50% of median
        result["financial_reduction_status"] = "अति निम्न"
        result["financial_reduction_class"] = "critical"
    
    result["financial_top_performer"] = top_financial
    result["state_avg_reduction"] = round(state_avg_reduction, 1)
    result["state_median_reduction"] = round(state_median_reduction, 1)
    
    # Bottom district information
    bottom_score = state_summary.get("by_score", {}).get("bottom_performer", {})
    result["bottom_score"] = {
        "name": bottom_score.get("name", "N/A"),
        "overall_old_work_score": bottom_score.get("overall_old_work_score", 0)
    }
    
    # Include state context if available
    if state_context:
        result["state_context"] = state_context
    
    return result

def process_mybharat_data(mb_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process MyBharat data for template.
    
    Args:
        mb_data: Raw MyBharat data
    
    Returns:
        Processed MyBharat data
    """
    logger.info("Processing MyBharat data for template")
    
    result = {}
    
    # Extract state statistics if available
    state_context = mb_data.get("state_context", {})
    mybharat_stats = state_context.get("mybharat_stats", {})
    
    # Process district data
    district_data = mb_data.get("district_data", {})
    marks = district_data.get("marks", 0)
    count = district_data.get("total_count", 0)
    target = district_data.get("target", 0)
    
    result["district_data"] = {
        "name": district_data.get("name", ""),
        "marks": marks,
        "count": count,
        "target": target,
        "completion_percent": round((count / target) * 100, 2) if target and target > 0 else 0,
        "grade": get_grade_label(marks, 10, mybharat_stats),
        "grade_class": get_grade_class(marks, 10, mybharat_stats)
    }
    
    # Process top performers by score
    top_score = mb_data.get("state_level_comparison", {}).get("by_score", {}).get("top_performer", {})
    result["top_score"] = {
        "name": top_score.get("name", ""),
        "marks": top_score.get("marks", 0),
        "count": top_score.get("total_count", 0),
        "target": top_score.get("target", 0),
        "completion_percent": round((top_score.get("total_count", 0) / top_score.get("target", 1)) * 100, 2) if top_score.get("target", 0) > 0 else 0
    }
    
    # Process top performers by count
    top_count = mb_data.get("state_level_comparison", {}).get("by_count", {}).get("top_performer", {})
    result["top_count"] = {
        "name": top_count.get("name", ""),
        "marks": top_count.get("marks", 0),
        "count": top_count.get("total_count", 0),
        "target": top_count.get("target", 0),
        "completion_percent": round((top_count.get("total_count", 0) / top_count.get("target", 1)) * 100, 2) if top_count.get("target", 0) > 0 else 0
    }
    
    # Include state context if available
    if state_context:
        result["state_context"] = state_context
    
    return result

def generate_recommendations(all_data: Dict[str, Any]) -> List[str]:
    """
    Generate actionable recommendations based on all component data.
    
    Args:
        all_data: Combined data from all components
    
    Returns:
        List of recommendation strings
    """
    logger.info("Generating recommendations based on data analysis")
    recommendations = []
    
    # Extract relevant data
    kpi = all_data.get("kpi", {})
    farm_ponds = all_data.get("farm_ponds", {}).get("district_data", {})
    amrit_sarovar = all_data.get("amrit_sarovar", {}).get("district_data", {})
    dugwell = all_data.get("dugwell", {}).get("district_data", {})
    old_works = all_data.get("old_works", {}).get("district_data", {})
    
    # Calculate completion percentages
    fp_completion = farm_ponds.get("completion_percent", 0)
    as_completion = amrit_sarovar.get("completion_percent", 0)
    dw_completion = dugwell.get("completion_percent", 0)
    
    # Find the component with lowest completion percentage
    components = [
        ("फार्म पोंड", fp_completion, farm_ponds.get("actual_count", 0), farm_ponds.get("target", 0)),
        ("अमृत सरोवर", as_completion, amrit_sarovar.get("actual_count", 0), amrit_sarovar.get("target", 0)),
        ("डगवेल रिचार्ज", dw_completion, dugwell.get("actual_count", 0), dugwell.get("target", 0))
    ]
    
    # Sort by completion percentage (ascending)
    components.sort(key=lambda x: x[1])
    
    # Recommendation 1: Focus on lowest performing component
    if components[0][1] < 50 and components[0][3] > 0:
        recommendations.append(
            f"{components[0][0]} कार्यों पर विशेष ध्यान दें। वर्तमान में केवल {components[0][1]}% कार्य पूरे हुए हैं "
            f"({components[0][2]} कार्य {components[0][3]} टारगेट के विरुद्ध)। इसे प्राथमिकता दें।"
        )
    
    # Recommendation 2: Block level recommendation
    blocks_data = []
    
    # Collect block data from farm ponds and dugwell
    fp_blocks = all_data.get("farm_ponds", {}).get("block_data", [])
    dw_blocks = all_data.get("dugwell", {}).get("block_data", [])
    
    if fp_blocks:
        # Find lowest performing block for farm ponds
        fp_blocks_sorted = sorted(fp_blocks, key=lambda x: x.get("actual_count_today", 0))
        if fp_blocks_sorted and len(fp_blocks_sorted) > 0:
            lowest_block = fp_blocks_sorted[0]
            if lowest_block.get("actual_count_today", 0) < 20:
                recommendations.append(
                    f"{lowest_block.get('name', '')} ब्लॉक में फार्म पोंड कार्यों को बढ़ावा दें, जहां केवल "
                    f"{lowest_block.get('actual_count_today', 0)} कार्य चालू हैं। ब्लॉक स्तरीय अधिकारियों के साथ "
                    f"साप्ताहिक समीक्षा बैठक आयोजित करें।"
                )
    
    # Recommendation 3: Financial progress recommendation
    financial_data = old_works.get("financial_progress_details", {})
    baseline_pending = financial_data.get("baseline_pending_lakhs", 0)
    current_pending = financial_data.get("current_pending_lakhs", 0)
    reduction_percentage = financial_data.get("reduction_percentage", 0)
    
    if reduction_percentage < 10 and baseline_pending > 0:
        recommendations.append(
            f"वित्तीय प्रगति में सुधार पर ध्यान केंद्रित करें। वर्तमान में लंबित भुगतान में केवल {reduction_percentage}% "
            f"की कमी आई है। प्राथमिकता के आधार पर पुराने लंबित भुगतानों को निपटाएं।"
        )
    
    # Recommendation 4: Overall score improvement
    overall_score = kpi.get("total_marks", {}).get("current", 0)
    if overall_score < 30:
        recommendations.append(
            f"समग्र स्कोर में सुधार के लिए सभी घटकों पर समान ध्यान दें। वर्तमान स्कोर {overall_score} "
            f"अपेक्षानुसार कम है। प्रत्येक कार्य घटक के लिए साप्ताहिक लक्ष्य निर्धारित करें और प्रगति "
            f"की निरंतर निगरानी करें।"
        )
    
    # If we have fewer than 3 recommendations, add a generic one
    if len(recommendations) < 3:
        recommendations.append(
            "प्रत्येक ब्लॉक में अच्छा प्रदर्शन करने वाली पंचायतों को चिन्हित करें और उनके अनुभवों को अन्य पंचायतों के साथ "
            "साझा करें। सफल कार्यप्रणालियों का प्रसार पूरे जिले में किया जाना चाहिए।"
        )
    
    # Cap recommendations at 4
    if len(recommendations) > 4:
        recommendations = recommendations[:4]
    
    return recommendations

def prepare_district_spectrum_data(district_name: str, district_score: float, state_context: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare data for the district performance spectrum visualization using state context data.
    
    Args:
        district_name: Name of the district
        district_score: Current district score
        state_context: State context data with statistics
    
    Returns:
        Dictionary with data for the spectrum visualization
    """
    # Extract data from state context
    total_marks_stats = state_context.get("total_marks_stats", {})
    
    # Get highest and lowest districts
    highest_district = {
        "name": total_marks_stats.get("top_performer", {}).get("name", "N/A"),
        "score": total_marks_stats.get("top_performer", {}).get("score", 0)
    }
    
    lowest_district = {
        "name": total_marks_stats.get("bottom_performer", {}).get("name", "N/A"),
        "score": total_marks_stats.get("bottom_performer", {}).get("score", 0)
    }
    
    # State average and median
    state_average = total_marks_stats.get("average", 0)
    state_median = total_marks_stats.get("median", 0)
    
    # Calculate district position percentage along the spectrum
    position_percent = 0
    range_max = highest_district["score"]
    range_min = lowest_district["score"]
    
    if range_max != range_min:
        position_percent = ((district_score - range_min) / (range_max - range_min)) * 100
    
    position_percent = max(0, min(100, position_percent))  # Clamp between 0-100
    
    return {
        "district_name": district_name,
        "district_score": district_score,
        "highest_district": highest_district,
        "lowest_district": lowest_district,
        "state_average": {"score": state_average},
        "state_median": {"score": state_median},
        "district_position_percent": position_percent
    }

# =============== REPORT GENERATION FUNCTIONS ===============




logger = logging.getLogger(__name__) # Make sure logger is defined

def prepare_template_data(all_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare all data for template rendering with corrected state_context access.
    """
    logger.info("Preparing template data for rendering")

    template_data = {}

    # Basic information
    template_data["district_name"] = all_data.get("district_name", "")
    template_data["report_date"] = all_data.get("report_date", "")
    template_data["current_datetime"] = all_data.get("current_datetime", datetime.now().strftime("%d-%m-%Y %H:%M"))

    # --- CORRECTED STATE CONTEXT and KPI ASSIGNMENT ---
    # Get the dictionary returned by process_kpi_data, which is stored under the 'kpi' key in all_data
    processed_kpi_data = all_data.get("kpi", {})

    # --- FIX: Extract state_context from the correct location ---
    # state_context is directly inside processed_kpi_data, not nested further
    state_context = processed_kpi_data.get("state_context", {})
    if not state_context:
        logger.warning("State context is missing or empty in processed KPI data!")
        # Optionally create a default structure to avoid errors down the line,
        # though prepare_district_spectrum_data should handle defaults.
        state_context = {"total_marks_stats": {}, "component_stats": {}}

    template_data["state_context"] = state_context # Store for potential direct use

    # Extract the *inner* 'kpi' dictionary (containing component kpis like rank, total_marks etc.)
    template_data["kpi"] = processed_kpi_data.get("kpi", {}) # This holds rank, total_marks etc.


    # --- Update KPI marks AND counts from component data ---
    # (This part should remain the same as the previous correct version,
    # ensuring kpi.old_work.completed is updated)
    # Farm Ponds update
    if "farm_ponds" in all_data and "district_data" in all_data["farm_ponds"]:
        farm_ponds_stats = state_context.get("component_stats", {}).get("farm_ponds", {})
        marks = all_data["farm_ponds"]["district_data"].get("marks", 0)
        completed_count = all_data["farm_ponds"]["district_data"].get("actual_count", 0)
        if "farm_ponds" in template_data["kpi"]:
            template_data["kpi"]["farm_ponds"]["marks"] = marks
            template_data["kpi"]["farm_ponds"]["grade"] = get_grade_label(marks, 30, farm_ponds_stats)
            template_data["kpi"]["farm_ponds"]["grade_class"] = get_grade_class(marks, 30, farm_ponds_stats)
            template_data["kpi"]["farm_ponds"]["completed"] = completed_count
        else:
            logger.warning("Key 'farm_ponds' not found in template_data['kpi'] during update.")

    # Amrit Sarovar update
    if "amrit_sarovar" in all_data and "district_data" in all_data["amrit_sarovar"]:
        as_stats = state_context.get("component_stats", {}).get("amrit_sarovar", {})
        marks = all_data["amrit_sarovar"]["district_data"].get("marks", 0)
        completed_count = all_data["amrit_sarovar"]["district_data"].get("actual_count", 0)
        if "amrit_sarovar" in template_data["kpi"]:
            template_data["kpi"]["amrit_sarovar"]["marks"] = marks
            template_data["kpi"]["amrit_sarovar"]["grade"] = get_grade_label(marks, 20, as_stats)
            template_data["kpi"]["amrit_sarovar"]["grade_class"] = get_grade_class(marks, 20, as_stats)
            template_data["kpi"]["amrit_sarovar"]["completed"] = completed_count
        else:
             logger.warning("Key 'amrit_sarovar' not found in template_data['kpi'] during update.")

    # Dugwell update
    if "dugwell" in all_data and "district_data" in all_data["dugwell"]:
        dw_stats = state_context.get("component_stats", {}).get("dugwell", {})
        marks = all_data["dugwell"]["district_data"].get("marks", 0)
        completed_count = all_data["dugwell"]["district_data"].get("actual_count", 0)
        if "dugwell" in template_data["kpi"]:
            template_data["kpi"]["dugwell"]["marks"] = marks
            template_data["kpi"]["dugwell"]["grade"] = get_grade_label(marks, 20, dw_stats)
            template_data["kpi"]["dugwell"]["grade_class"] = get_grade_class(marks, 20, dw_stats)
            template_data["kpi"]["dugwell"]["completed"] = completed_count
        else:
             logger.warning("Key 'dugwell' not found in template_data['kpi'] during update.")

    # Old Works update
    if "old_works" in all_data and "district_data" in all_data["old_works"]:
        ow_stats = state_context.get("component_stats", {})
        target_stats = ow_stats.get("performance_target", {})
        payment_stats = ow_stats.get("performance_payment", {})
        combined_stats = {
            "average": target_stats.get("average", 0) + payment_stats.get("average", 0),
            "median": target_stats.get("median", 0) + payment_stats.get("median", 0)
        }
        marks = all_data["old_works"]["district_data"].get("overall_old_work_score", 0)
        completed_count = all_data["old_works"]["district_data"].get("total_work_completed", 0)

        if "old_work" in template_data["kpi"]:
            template_data["kpi"]["old_work"]["marks"] = marks
            template_data["kpi"]["old_work"]["grade"] = get_grade_label(marks, 20, combined_stats)
            template_data["kpi"]["old_work"]["grade_class"] = get_grade_class(marks, 20, combined_stats)
            template_data["kpi"]["old_work"]["completed"] = completed_count
        else:
             logger.warning("Key 'old_work' not found in template_data['kpi'] during update.")

    # MyBharat update
    if "mybharat" in all_data and "district_data" in all_data["mybharat"]:
        mb_stats = state_context.get("component_stats", {}).get("mybharat", {})
        marks = all_data["mybharat"]["district_data"].get("marks", 0)
        completed_count = all_data["mybharat"]["district_data"].get("count", 0)
        if "mybharat" in template_data["kpi"]:
            template_data["kpi"]["mybharat"]["marks"] = marks
            template_data["kpi"]["mybharat"]["grade"] = get_grade_label(marks, 10, mb_stats)
            template_data["kpi"]["mybharat"]["grade_class"] = get_grade_class(marks, 10, mb_stats)
            template_data["kpi"]["mybharat"]["completed"] = completed_count
        else:
            logger.warning("Key 'mybharat' not found in template_data['kpi'] during update.")


    # --- Component data ---
    template_data["farm_ponds"] = all_data.get("farm_ponds", {})
    template_data["amrit_sarovar"] = all_data.get("amrit_sarovar", {})
    template_data["dugwell"] = all_data.get("dugwell", {})
    template_data["old_works"] = all_data.get("old_works", {})
    template_data["mybharat"] = all_data.get("mybharat", {})

    # --- Chart data ---
    template_data["farm_ponds_block_labels"] = all_data.get("farm_ponds", {}).get("chart_data", {}).get("labels", "[]")
    template_data["farm_ponds_block_values"] = all_data.get("farm_ponds", {}).get("chart_data", {}).get("values", "[]")
    template_data["dugwell_block_labels"] = all_data.get("dugwell", {}).get("chart_data", {}).get("labels", "[]")
    template_data["dugwell_block_values"] = all_data.get("dugwell", {}).get("chart_data", {}).get("values", "[]")
    template_data["old_works_block_labels"] = all_data.get("old_works", {}).get("chart_data", {}).get("labels", "[]")
    template_data["old_works_block_values"] = all_data.get("old_works", {}).get("chart_data", {}).get("values", "[]")

    # --- District performance spectrum ---
    # Calculate district_score safely from the correct location
    district_score = 0 # Default score
    if "total_marks" in template_data.get("kpi", {}):
         district_score_data = template_data["kpi"].get("total_marks", {})
         score_val = district_score_data.get("current")
         if isinstance(score_val, (int, float)) and not math.isnan(score_val) and not math.isinf(score_val):
             district_score = score_val
         else:
             logger.warning(f"District score is not a valid number: {score_val}. Defaulting to 0 for spectrum.")
    else:
        logger.warning("Key 'total_marks' not found in template_data['kpi'] for spectrum calculation. Defaulting score to 0.")

    # Call prepare_district_spectrum_data with the correctly extracted state_context
    spectrum_data = prepare_district_spectrum_data(
        template_data["district_name"],
        district_score,
        state_context # Pass the correctly extracted state_context
    )

    # Assign spectrum data to template variables
    template_data["district_position_percent"] = spectrum_data.get("district_position_percent", 50) # Default needed
    template_data["district_score"] = spectrum_data.get("district_score", 0)
    template_data["highest_district"] = spectrum_data.get("highest_district", {"name": "N/A", "score": 0})
    template_data["lowest_district"] = spectrum_data.get("lowest_district", {"name": "N/A", "score": 0})
    template_data["state_average"] = spectrum_data.get("state_average", {"score": 0})
    template_data["state_median"] = spectrum_data.get("state_median", {"score": 0})
    # Also pass the calculated marker positions if using the simplified template approach
    template_data["median_position_percent"] = spectrum_data.get("median_position_percent", 50)
    template_data["average_position_percent"] = spectrum_data.get("average_position_percent", 50)


    # --- Recommendations ---
    template_data["recommendations"] = all_data.get("recommendations", [])

    return template_data


def generate_template_html(template_data: Dict[str, Any]) -> str:
    """
    Generate HTML from template and data using Jinja2.
    
    Args:
        template_data: Data for template rendering
    
    Returns:
        Rendered HTML string
    """
    logger.info("Generating HTML from template")
    
    try:
        from jinja2 import Template, FileSystemLoader, Environment
        
        # Check if template file exists
        if not os.path.exists(TEMPLATE_PATH):
            logger.error(f"Template file not found: {TEMPLATE_PATH}")
            
            # Create a basic template as fallback
            template_content = """
            <!DOCTYPE html>
            <html lang="hi">
            <head>
                <meta charset="UTF-8">
                <title>JSM Dashboard</title>
            </head>
            <body>
                <h1>{{ district_name }} - JSM Dashboard</h1>
                <p>Report Date: {{ report_date }}</p>
                <p>Error: Template file not found. Please check the path.</p>
            </body>
            </html>
            """
        else:
            # Load template from file
            with open(TEMPLATE_PATH, 'r', encoding='utf-8') as f:
                template_content = f.read()
        
        # Create template and render
        template = Template(template_content)
        html = template.render(**template_data)
        
        return html
    
    except Exception as e:
        logger.error(f"Error generating HTML from template: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Return a basic HTML in case of error
        return f"""
        <!DOCTYPE html>
        <html lang="hi">
        <head>
            <meta charset="UTF-8">
            <title>JSM Dashboard Error</title>
        </head>
        <body>
            <h1>{template_data.get('district_name', 'Unknown')} - JSM Dashboard</h1>
            <p>Report Date: {template_data.get('report_date', 'Unknown')}</p>
            <p>Error: {str(e)}</p>
        </body>
        </html>
        """

def generate_claude_prompt(district_name: str, date: str, data_json: str) -> str:
    """
    Generate the prompt for Claude to create dynamic content with enhanced styling guidance
    and structured recommendations.
    
    Args:
        district_name: District name
        date: Report date
        data_json: JSON data string
    
    Returns:
        Prompt string
    """
    # JSON example with structured recommendations (properly formatted for structured display)
    json_example = '''
    {
      "grades": {
        "total_marks": {"grade": "उत्कृष्ट/अच्छा/औसत/निम्न/अति निम्न", "class": "excellent/good/average/poor/very-poor"},
        "farm_ponds": {"grade": "...", "class": "..."},
        "amrit_sarovar": {"grade": "...", "class": "..."},
        "dugwell": {"grade": "...", "class": "..."},
        "old_works": {"grade": "...", "class": "..."},
        "mybharat": {"grade": "...", "class": "..."}
      },
      "status_text": {
        "farm_ponds": "उत्कृष्ट (राज्य औसत से बेहतर)",
        "dugwell": "..."
      },
      "summary": "जिले का संक्षिप्त विश्लेषण यहां...",
      "recommendations": [
        {
          "priority": "high",
          "component": "फार्म पोंड",
          "text": "यहां सिफारिश का विस्तृत विवरण..."
        },
        {
          "priority": "medium",
          "component": "डगवेल रिचार्ज",
          "text": "यहां सिफारिश का विस्तृत विवरण..."
        },
        {
          "priority": "low",
          "component": "अमृत सरोवर",
          "text": "यहां सिफारिश का विस्तृत विवरण..."
        }
      ]
    }
    '''
    
    # Create the prompt with enhanced instructions for statistical grading and structured recommendations
    prompt = f"""
    आप जल गंगा संवर्धन अभियान (JSM) के लिए एक विशेषज्ञ डेटा विश्लेषक और रिपोर्ट डिजाइनर हैं, जो संक्षिप्त लेकिन अंतर्दृष्टिपूर्ण रिपोर्ट बनाने में कुशल हैं।
    प्रदान किया गया डेटा {district_name} जिले के JSM प्रदर्शन का सारांश है। आपका कार्य {date} से डेटा के आधार पर जिले के प्रदर्शन का विश्लेषण और उचित 3-4 सिफारिशें देना है।

    महत्वपूर्ण निर्देश:
    
    1. ग्रेडिंग राज्य के औसत और मध्यमान (median) के आधार पर होनी चाहिए, न कि निश्चित सीमाओं पर:
       - "उत्कृष्ट": राज्य औसत से 25% या अधिक ऊपर
       - "अच्छा": राज्य औसत से ऊपर लेकिन 25% से कम
       - "औसत": राज्य औसत से नीचे लेकिन मध्यमान से ऊपर
       - "निम्न": मध्यमान से नीचे लेकिन मध्यमान का कम से कम 70%
       - "अति निम्न": मध्यमान के 70% से कम
    
    2. वित्तीय प्रगति का मूल्यांकन राज्य के मध्यमान (median) के आधार पर किया जाना चाहिए, न कि औसत के आधार पर।
    
    3. प्रत्येक घटक के लिए समझ में आने वाले प्रदर्शन स्थिति वाक्य, जैसे "उत्कृष्ट (राज्य औसत से बेहतर)", "अच्छा (औसत के करीब)" आदि।
    
    4. जिले के प्रदर्शन पर 2-3 वाक्य वाला संक्षिप्त विश्लेषण, जिससे समझ में आए कि जिला कैसा प्रदर्शन कर रहा है।
    
    5. सिफारिशें: 3-4 व्यावहारिक सिफारिशें दें, जो सुसंगत और संरचित हों। प्रत्येक सिफारिश में शामिल होना चाहिए:
       - प्राथमिकता (priority): "high", "medium", या "low" (अंग्रेजी में)
       - संबंधित घटक (component): संबंधित कार्य का नाम जैसे "डगवेल रिचार्ज", "पुराने कार्य", "फार्म पोंड", आदि
       - विवरण (text): सिफारिश का विस्तृत विवरण - यह स्पष्ट, संक्षिप्त और कार्रवाई योग्य होना चाहिए
    
    6. कृपया ग्रेड के लिए CSS क्लास नाम भी प्रदान करें, जो निम्न में से होने चाहिए:
       - उत्कृष्ट: "excellent"
       - अच्छा: "good"
       - औसत: "average"
       - निम्न: "poor"
       - अति निम्न: "very-poor"
    
    7. प्रदर्शन बैज के लिए CSS क्लास नाम इस प्रकार होने चाहिए:
       - उत्कृष्ट: "high"
       - अच्छा: "above-avg"
       - औसत: "average"
       - निम्न: "below-avg"
       - अति निम्न: "critical"

    8. सुनिश्चित करें कि सिफारिशें उचित प्रारूप में हों, क्योंकि वे HTML टेम्पलेट के अनुरूप रेंडर की जाएंगी। सिफारिशों के लिए सही JSON संरचना का उपयोग करें जैसा कि उदाहरण में दिखाया गया है।

    आपका उत्तर सिर्फ JSON फॉर्मेट में होना चाहिए, जिसमें निम्न फील्ड्स हों:

    ```
{json_example}
    ```

    <jsm_data>
    {data_json}
    </jsm_data>
    
    कृपया केवल JSON उत्तर दें, किसी भी अतिरिक्त टेक्स्ट के बिना। विशेष रूप से ध्यान दें कि recommendations फील्ड में 3-4 व्यावहारिक सिफारिशें उचित संरचना में (priority, component, text के साथ) होनी चाहिए।
    """
    return prompt


def generate_dynamic_content(district_name: str, date: str, all_data: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """
    Generate dynamic content using Claude API.
    
    Args:
        district_name: District name
        date: Report date
        all_data: Combined data from all components
        api_key: Anthropic API key
    
    Returns:
        Dictionary with dynamic content
    """
    logger.info("Generating dynamic content using Claude API")
    
    # Load API key from .env file if not provided
    if not api_key:
        try:
            from dotenv import load_dotenv
            load_dotenv()
            api_key = os.environ.get("ANTHROPIC_API_KEY")
            if not api_key:
                logger.warning("No Anthropic API key found in .env file")
        except ImportError:
            logger.warning("dotenv package not installed, cannot load API key from .env file")
        
        if not api_key:
            logger.warning("No Anthropic API key provided, using local fallback")
            # Generate recommendations locally as fallback
            recommendations = generate_recommendations(all_data)
            return create_fallback_response(all_data, recommendations)
    
    try:
        # Convert all data to JSON string for the prompt
        data_json = json.dumps(all_data, ensure_ascii=False, indent=2)
        
        # Generate prompt
        prompt = generate_claude_prompt(district_name, date, data_json)
        
        # Initialize Anthropic client
        client = anthropic.Anthropic(api_key=api_key)
        
        # Use streaming for the request to handle long generation
        dynamic_content_str = ""
        
        # Create a streaming request
        with client.messages.stream(
            model="claude-3-7-sonnet-20250219",
            max_tokens=64000,
            thinking={
                "type": "enabled",
                "budget_tokens": 25000
            },
            messages=[{"role": "user", "content": prompt}]
        ) as stream:
            # Process the stream
            for chunk in stream.text_stream:
                # Append to our full content
                dynamic_content_str += chunk
                
                # Log progress periodically
                if len(dynamic_content_str) % 10000 == 0:
                    logger.info(f"Received {len(dynamic_content_str)} characters of dynamic content so far")
            
            # Get the final message after streaming completes
            response = stream.get_final_message()
            
            # Log token usage if available
            if hasattr(response, 'usage'):
                prompt_tokens = response.usage.input_tokens
                completion_tokens = response.usage.output_tokens
                total_tokens = prompt_tokens + completion_tokens
                logger.info(f"Token usage - Prompt: {prompt_tokens}, Completion: {completion_tokens}, Total: {total_tokens}")
            
            # Check and log thinking output
            thinking_output = None
            if hasattr(response, 'thinking') and response.thinking:
                thinking_text = response.thinking.thinking_text
                thinking_tokens = response.thinking.tokens
                logger.info(f"Thinking mode used: {thinking_tokens} tokens")
                
                # Create output directory if it doesn't exist
                os.makedirs(OUTPUT_DIR, exist_ok=True)
                
                # Save thinking to file
                thinking_file = os.path.join(OUTPUT_DIR, f"report_thinking_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")
                with open(thinking_file, 'w', encoding='utf-8') as f:
                    f.write(thinking_text)
                logger.info(f"Thinking output saved to {thinking_file}")
        
        # Try to extract JSON from content
        try:
            # Find JSON content (in case there's additional text)
            json_start = dynamic_content_str.find('{')
            json_end = dynamic_content_str.rfind('}') + 1
            
            if json_start >= 0 and json_end > json_start:
                json_content = dynamic_content_str[json_start:json_end]
                dynamic_content = json.loads(json_content)
                logger.info("Successfully parsed dynamic content from Claude API")
                return dynamic_content
            else:
                logger.warning("Could not find JSON content in Claude response")
                raise ValueError("No JSON content found in response")
                
        except json.JSONDecodeError:
            logger.error(f"Error parsing JSON from Claude response: {dynamic_content_str}")
            raise
            
    except Exception as e:
        logger.error(f"Error generating dynamic content with Claude API: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Fallback to local generation
        logger.info("Using local fallback for recommendations")
        recommendations = generate_recommendations(all_data)
        return create_fallback_response(all_data, recommendations)

def create_fallback_response(all_data: Dict[str, Any], recommendations: List[str]) -> Dict[str, Any]:
    """
    Create a fallback response when Claude API fails, using statistical grading.
    
    Args:
        all_data: Combined data from all components
        recommendations: List of recommendation strings
    
    Returns:
        Dictionary with fallback dynamic content
    """
    # Try to get state statistics from the data
    state_context = all_data.get("kpi", {}).get("state_context", {})
    total_marks_stats = state_context.get("total_marks_stats", {})
    component_stats = state_context.get("component_stats", {})
    
    farm_ponds_stats = component_stats.get("farm_ponds", {})
    amrit_sarovar_stats = component_stats.get("amrit_sarovar", {})
    dugwell_stats = component_stats.get("dugwell", {})
    mybharat_stats = component_stats.get("mybharat", {})
    
    # For old works, combine target and payment stats
    old_works_stats = {
        "average": component_stats.get("performance_target", {}).get("average", 0) + component_stats.get("performance_payment", {}).get("average", 0),
        "median": component_stats.get("performance_target", {}).get("median", 0) + component_stats.get("performance_payment", {}).get("median", 0)
    }
    
    return {
        "grades": {
            "total_marks": {
                "grade": get_grade_label(all_data.get("kpi", {}).get("total_marks", {}).get("current", 0), 100, total_marks_stats), 
                "class": get_grade_class(all_data.get("kpi", {}).get("total_marks", {}).get("current", 0), 100, total_marks_stats)
            },
            "farm_ponds": {
                "grade": get_grade_label(all_data.get("farm_ponds", {}).get("district_data", {}).get("marks", 0), 30, farm_ponds_stats),
                "class": get_grade_class(all_data.get("farm_ponds", {}).get("district_data", {}).get("marks", 0), 30, farm_ponds_stats)
            },
            "amrit_sarovar": {
                "grade": get_grade_label(all_data.get("amrit_sarovar", {}).get("district_data", {}).get("marks", 0), 20, amrit_sarovar_stats),
                "class": get_grade_class(all_data.get("amrit_sarovar", {}).get("district_data", {}).get("marks", 0), 20, amrit_sarovar_stats)
            },
            "dugwell": {
                "grade": get_grade_label(all_data.get("dugwell", {}).get("district_data", {}).get("marks", 0), 20, dugwell_stats),
                "class": get_grade_class(all_data.get("dugwell", {}).get("district_data", {}).get("marks", 0), 20, dugwell_stats)
            },
            "old_works": {
                "grade": get_grade_label(all_data.get("old_works", {}).get("district_data", {}).get("overall_old_work_score", 0), 20, old_works_stats),
                "class": get_grade_class(all_data.get("old_works", {}).get("district_data", {}).get("overall_old_work_score", 0), 20, old_works_stats)
            },
            "mybharat": {
                "grade": get_grade_label(all_data.get("mybharat", {}).get("district_data", {}).get("marks", 0), 10, mybharat_stats),
                "class": get_grade_class(all_data.get("mybharat", {}).get("district_data", {}).get("marks", 0), 10, mybharat_stats)
            }
        },
        "status_text": {
            "farm_ponds": all_data.get("farm_ponds", {}).get("district_position_vs_state", {}).get("completion_status", "N/A"),
            "dugwell": all_data.get("dugwell", {}).get("district_position_vs_state", {}).get("status", "N/A")
        },
        "summary": f"{all_data.get('district_name', '')} जिले का कुल स्कोर {all_data.get('kpi', {}).get('total_marks', {}).get('current', 0)} है। जिला राज्य में {all_data.get('kpi', {}).get('rank', {}).get('current', 'N/A')}वें स्थान पर है।",
        "recommendations": recommendations
    }

def generate_combined_html(template_data: Dict[str, Any], dynamic_content: Dict[str, Any]) -> str:
    """
    Combine template data with dynamic content to generate the final HTML.
    
    Args:
        template_data: Data for template
        dynamic_content: Dynamic content from Claude
    
    Returns:
        Final HTML string
    """
    logger.info("Combining template data with dynamic content")
    
    # Update template data with dynamic content
    combined_data = template_data.copy()
    
    # Apply grades
    grades = dynamic_content.get("grades", {})
    for component, grade_info in grades.items():
        if component == "total_marks" and "kpi" in combined_data:
            if "total_marks" in combined_data["kpi"]:
                combined_data["kpi"]["total_marks"]["grade"] = grade_info.get("grade", "N/A")
                combined_data["kpi"]["total_marks"]["grade_class"] = f"grade-badge {grade_info.get('class', '')}"
        elif component in combined_data["kpi"]:
            combined_data["kpi"][component]["grade"] = grade_info.get("grade", "N/A")
            combined_data["kpi"][component]["grade_class"] = f"grade-badge {grade_info.get('class', '')}"
    
    # Apply status text
    status_text = dynamic_content.get("status_text", {})
    for component, status in status_text.items():
        if component == "farm_ponds" and "farm_ponds" in combined_data:
            if "district_position_vs_state" in combined_data["farm_ponds"]:
                combined_data["farm_ponds"]["district_position_vs_state"]["completion_status"] = status
        elif component == "dugwell" and "dugwell" in combined_data:
            if "district_position_vs_state" in combined_data["dugwell"]:
                combined_data["dugwell"]["district_position_vs_state"]["status"] = status
    
    # Apply recommendations
    combined_data["recommendations"] = dynamic_content.get("recommendations", combined_data.get("recommendations", []))
    
    # Generate HTML from template
    html = generate_template_html(combined_data)
    return html

def generate_pdf_from_html(html_filename: str, district: str, date: str) -> str:
    """
    Generate PDF from HTML file using Playwright.
    
    Args:
        html_filename: Path to HTML file
        district: District name
        date: Date string
    
    Returns:
        Path to generated PDF file
    """
    logger.info(f"Converting HTML to PDF using Playwright: {html_filename}")
    
    # Create PDF output directory if it doesn't exist
    os.makedirs(PDF_OUTPUT_DIR, exist_ok=True)
    
    # Create PDF filename
    pdf_basename = os.path.basename(html_filename).replace('.html', '')
    pdf_filename = os.path.join(PDF_OUTPUT_DIR, f"{pdf_basename}.pdf")
    
    try:
        import asyncio
        from playwright.async_api import async_playwright
        
        async def convert():
            async with async_playwright() as p:
                # Launch browser with higher timeout and font rendering options
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--font-render-hinting=none']  # Better font rendering
                )
                
                # Create context with font settings
                context = await browser.new_context(
                    viewport={'width': 1200, 'height': 1600},
                    device_scale_factor=2.0  # Higher resolution
                )
                
                page = await context.new_page()
                
                # Load HTML file with file:// protocol
                file_url = f"file://{os.path.abspath(html_filename)}"
                await page.goto(file_url, wait_until="networkidle", timeout=60000)
                
                # Add additional fonts if needed
                await page.add_script_tag(content="""
                    if (!document.getElementById('font-loader')) {
                        const fontLoader = document.createElement('link');
                        fontLoader.id = 'font-loader';
                        fontLoader.rel = 'stylesheet';
                        fontLoader.href = 'https://fonts.googleapis.com/css2?family=Noto+Sans+Devanagari:wght@400;500;700&family=Hind:wght@400;500;700&family=Poppins:wght@400;500;700&display=swap';
                        document.head.appendChild(fontLoader);
                        
                        // Force font usage
                        const style = document.createElement('style');
                        style.textContent = `
                            * {
                                font-family: 'Noto Sans Devanagari', 'Hind', 'Poppins', Arial, sans-serif !important;
                            }
                        `;
                        document.head.appendChild(style);
                    }
                """)
                
                # Wait for fonts to load
                await page.wait_for_timeout(2000)
                
                # Use A2 paper size with better PDF settings
                await page.pdf(
                    path=pdf_filename,
                    format="A2",
                    print_background=True,
                    margin={"top": "15mm", "right": "15mm", "bottom": "15mm", "left": "15mm"},
                    scale=1.0,
                    prefer_css_page_size=True,
                )
                
                await browser.close()
                return True
        
        # Run async function
        loop = asyncio.get_event_loop()
        result = loop.run_until_complete(convert())
        
        if os.path.exists(pdf_filename) and result:
            logger.info(f"Successfully converted '{html_filename}' to '{pdf_filename}' with Playwright")
            return pdf_filename
        else:
            raise Exception("PDF file was not created successfully")
            
    except ImportError:
        error_msg = "Playwright not available. Install with: pip install playwright"
        logger.error(error_msg)
        logger.error("After installation, run: python -m playwright install chromium")
        raise ImportError(error_msg)
    except Exception as e:
        error_msg = f"Error converting HTML to PDF with Playwright: {str(e)}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)

def setup_playwright():
    """Set up Playwright for PDF generation."""
    try:
        from playwright.sync_api import sync_playwright
        import platform
        
        # Check if playwright is already installed and set up
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()
            logger.info("Playwright is already set up and working correctly")
            return True
        except Exception:
            logger.info("Playwright needs installation...")
            
            # Install Playwright browsers
            if platform.system() == "Windows":
                os.system("playwright install chromium")
            else:
                os.system("python -m playwright install chromium")
                
            logger.info("Playwright setup completed")
            return True
    except ImportError:
        logger.warning("Playwright not found. PDF generation will not be available.")
        logger.warning("To enable PDF generation, install with: pip install playwright")
        return False

# =============== MAIN FUNCTION AND CLI ===============

def generate_jsm_dashboard(district: str, date: str, api_key: Optional[str] = None) -> Tuple[str, str]:
    """
    Generate JSM dashboard for a district.
    
    Args:
        district: District name
        date: Report date (YYYY-MM-DD)
        api_key: Anthropic API key for dynamic content
    
    Returns:
        Tuple of (HTML filename, PDF filename)
    """
    logger.info(f"Starting dashboard generation for {district} on {date}")
    
    try:
        # Ensure output directories exist
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        os.makedirs(PDF_OUTPUT_DIR, exist_ok=True)
        os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)
        os.makedirs(HTML_OUTPUT_DIR, exist_ok=True)
        
        # Collect data from all components with robust error handling
        try:
            kpi_data = get_district_kpis(district, date)
        except Exception as e:
            logger.error(f"Failed to get KPI data: {e}")
            raise RuntimeError(f"Failed to get KPI data: {e}")
        
        try:
            amrit_sarovar_data = get_amrit_sarovar_data(district, date)
        except Exception as e:
            logger.error(f"Failed to get Amrit Sarovar data: {e}")
            raise RuntimeError(f"Failed to get Amrit Sarovar data: {e}")
        
        try:
            dugwell_data = get_dugwell_data(district, date)
        except Exception as e:
            logger.error(f"Failed to get Dugwell data: {e}")
            raise RuntimeError(f"Failed to get Dugwell data: {e}")
        
        try:
            farm_ponds_data = get_farm_ponds_data(district, date)
        except Exception as e:
            logger.error(f"Failed to get Farm Ponds data: {e}")
            raise RuntimeError(f"Failed to get Farm Ponds data: {e}")
        
        try:
            old_works_data = get_old_works_data(district, date)
        except Exception as e:
            logger.error(f"Failed to get Old Works data: {e}")
            raise RuntimeError(f"Failed to get Old Works data: {e}")
        
        try:
            mybharat_data = get_mybharat_data(district, date)
        except Exception as e:
            logger.error(f"Failed to get MyBharat data: {e}")
            raise RuntimeError(f"Failed to get MyBharat data: {e}")
        
        # Process data with proper error handling
        processed_kpi = process_kpi_data(kpi_data)
        processed_amrit_sarovar = process_amrit_sarovar_data(amrit_sarovar_data)
        processed_dugwell = process_dugwell_data(dugwell_data)
        processed_farm_ponds = process_farm_ponds_data(farm_ponds_data)
        processed_old_works = process_old_works_data(old_works_data)
        processed_mybharat = process_mybharat_data(mybharat_data)
        
        # Combine all processed data
        all_data = {
            "district_name": district,
            "report_date": date,
            "current_datetime": datetime.now().strftime("%d-%m-%Y %H:%M"),
            "kpi": processed_kpi,
            "farm_ponds": processed_farm_ponds,
            "amrit_sarovar": processed_amrit_sarovar,
            "dugwell": processed_dugwell,
            "old_works": processed_old_works,
            "mybharat": processed_mybharat
        }
        
        # Generate dynamic content with Claude API
        dynamic_content = generate_dynamic_content(district, date, all_data, api_key)
        
        # Add recommendations to all_data
        all_data["recommendations"] = dynamic_content.get("recommendations", [])
        
        # Prepare template data
        template_data = prepare_template_data(all_data)
        
        # Combine template with dynamic content
        final_html = generate_combined_html(template_data, dynamic_content)
        
        # Save HTML to file
        html_filename = os.path.join(HTML_OUTPUT_DIR, f"jsm_dashboard_{district.lower().replace(' ', '_')}_{date.replace('-', '')}.html")
        with open(html_filename, 'w', encoding='utf-8') as f:
            f.write(final_html)
        
        logger.info(f"HTML dashboard saved to: {html_filename}")
        
        # Generate PDF
        try:
            pdf_filename = generate_pdf_from_html(html_filename, district, date)
            logger.info(f"PDF dashboard saved to: {pdf_filename}")
        except Exception as e:
            logger.error(f"Error generating PDF: {e}")
            pdf_filename = None
        
        return html_filename, pdf_filename
    
    except Exception as e:
        logger.error(f"Error generating dashboard: {e}")
        logger.error(traceback.format_exc())
        raise
    
def main():
    """Main execution function with argument parsing."""
    parser = argparse.ArgumentParser(description="Generate JSM Dashboard for a district")
    parser.add_argument("-d", "--district", required=True, help="Name of the district")
    parser.add_argument("-dt", "--date", required=True, help="Report date (YYYY-MM-DD)")
    parser.add_argument("-k", "--api-key", help="Anthropic API key for dynamic content generation")
    parser.add_argument("-t", "--template", help="Path to HTML template file")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Set template path if provided
    if args.template and os.path.exists(args.template):
        global TEMPLATE_PATH
        TEMPLATE_PATH = args.template
    
    # Get API key from environment if not provided
    api_key = args.api_key or os.environ.get("ANTHROPIC_API_KEY")
    
    # Setup Playwright for PDF generation
    setup_playwright()
    
    try:
        # Load API key from .env file if not provided
        if not api_key:
            try:
                from dotenv import load_dotenv
                load_dotenv("/home/anshu/nrega_report_claude/.env")
                api_key = os.environ.get("ANTHROPIC_API_KEY")
            except ImportError:
                logger.warning("dotenv package not installed, cannot load API key from .env file")
        
        # Generate dashboard
        html_file, pdf_file = generate_jsm_dashboard(args.district, args.date, api_key)
        
        # Print summary
        print("\nJSM Dashboard successfully generated")
        print(f"HTML: {html_file}")
        if pdf_file:
            print(f"PDF: {pdf_file}")
        else:
            print("PDF generation failed. Install playwright for PDF support:")
            print("  pip install playwright")
            print("  python -m playwright install chromium")
    
    except Exception as e:
        logger.error(f"Error generating dashboard: {str(e)}")
        logger.error(traceback.format_exc())
        print(f"\nError: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()