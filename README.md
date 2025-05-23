# Jal Ganga Samvardhan Abhiyan (JGSA) Dashboard Generator 📊

## Overview

This project generates comprehensive performance monitoring dashboards for the Jal Ganga Samvardhan Abhiyan (JGSA) initiative within Madhya Pradesh. It aggregates data from various components, analyzes district-level performance against state benchmarks, and provides actionable insights through dynamically generated reports.

The system fetches data using dedicated analysis scripts (`analyze_*.py`), processes it, optionally leverages the Anthropic Claude API for generating summaries and recommendations, and finally renders detailed HTML and PDF reports using a Jinja2 template.

The primary goal is to offer a clear, data-driven overview of a district's progress in the JGSA, facilitating monitoring and informed decision-making.

## Features

*   **Data Aggregation:** Combines data from key JGSA components:
    *   Overall KPIs (Score, Rank)
    *   Farm Ponds
    *   Amrit Sarovar
    *   Dugwell Recharge
    *   Old Works (NRM) Completion & Financial Progress
    *   MyBharat (Jaldoots)
*   **Comparative Analysis:**
    *   Compares district performance against state averages and medians.
    *   Ranks districts based on score and component counts.
    *   Identifies top/bottom performers statewide.
*   **Detailed Breakdown:** Provides block-level performance analysis within the selected district.
*   **AI-Powered Insights (Optional):** Uses Anthropic's Claude API (if API key is provided) to generate:
    *   Concise performance summaries.
    *   Actionable, prioritized recommendations.
*   **Statistical Grading:** Assigns performance grades (उत्कृष्ट, अच्छा, औसत, निम्न, अति निम्न) based on statistical comparison with state-level data, not fixed thresholds.
*   **Reporting:** Generates user-friendly reports in:
    *   **HTML:** Interactive format viewable in web browsers.
    *   **PDF:** Printable A2-sized format for distribution and archiving.

## How to Run

1.  **Ensure Requirements are Met:** Install necessary Python packages and set up Playwright (see Requirements section).
2.  **API Key (Optional but Recommended):** For AI-generated summaries and recommendations, set the `ANTHROPIC_API_KEY` environment variable. You can do this directly or by creating a `.env` file in the project root:
    ```.env
    ANTHROPIC_API_KEY=your_claude_api_key_here
    ```
    If no API key is found, the script will fall back to locally generated, less dynamic recommendations.
3.  **Run the Script:** Execute the main generator script from your terminal, providing the district name and report date.

    ```bash
    python jsm_dashboard_generator.py -d "DISTRICT_NAME" -dt "YYYY-MM-DD" [OPTIONS]
    ```

    *   `-d "DISTRICT_NAME"`: The name of the district (must be one of the supported districts listed below, case-insensitive matching).
    *   `-dt "YYYY-MM-DD"`: The date for which the report should be generated. The script will often compare this date's data with the previous day's data.

    **Example:**
    ```bash
    python jsm_dashboard_generator.py -d "SIDHI" -dt "2025-04-26"
    ```

    **Optional Arguments:**
    *   `-k YOUR_API_KEY` or `--api-key YOUR_API_KEY`: Directly provide the Anthropic API key (overrides environment variable).
    *   `-t /path/to/your/template.html` or `--template /path/to/your/template.html`: Specify a custom HTML template file.
    *   `--debug`: Enable verbose debug logging for troubleshooting.

4.  **Locate Output:** Reports will be saved in the `output/` directory:
    *   HTML files: `output/html/jsm_dashboard_{district}_{date}.html`
    *   PDF files: `output/pdf/jsm_dashboard_{district}_{date}.pdf`
    *   Intermediate JSON analysis data: `output/json/`

**Note:** The script depends on the `analyze_*.py` scripts (`analyze_district_kpis.py`, `analyze_amrit_sarovar.py`, etc.) and `utils.py` being present in the same directory or accessible via Python's path.

## Supported Districts

The following 52 districts in Madhya Pradesh are supported:

1.  AGAR-MALWA
2.  ALIRAJPUR
3.  ANUPPUR
4.  ASHOK NAGAR
5.  BALAGHAT
6.  BARWANI
7.  BETUL
8.  BHIND
9.  BHOPAL
10. BURHANPUR
11. CHHATARPUR
12. CHHINDWARA
13. DAMOH
14. DATIA
15. DEWAS
16. DHAR
17. DINDORI
18. GUNA
19. GWALIOR
20. HARDA
21. INDORE
22. JABALPUR
23. JHABUA
24. KATNI
25. KHANDWA
26. KHARGONE
27. MANDLA
28. MANDSAUR
29. MORENA
30. NARMADAPURAM
31. NARSINGHPUR
32. NEEMUCH
33. NIWARI
34. PANNA
35. RAISEN
36. RAJGARH
37. RATLAM
38. REWA
39. SAGAR
40. SATNA
41. SEHORE
42. SEONI
43. SHAHDOL
44. SHAJAPUR
45. SHEOPUR
46. SHIVPURI
47. SIDHI
48. SINGRAULI
49. TIKAMGARH
50. UJJAIN
51. UMARIA
52. VIDISHA

*(Please ensure the district name provided via the `-d` argument matches one of these names, although the script attempts case-insensitive matching).*

## Requirements ⚙️

*   **Python 3.8+**
*   **Dependencies:** Install required Python packages using pip:
    ```bash
    pip install -r requirements.txt
    ```
    See `requirements.txt` for a complete list of dependencies (includes `requests`, `anthropic`, `jinja2`, `playwright`, `python-dotenv`, etc.).
*   **Playwright Browser:** PDF generation relies on Playwright. After installing the Python package, you *must* install the necessary browser binaries:
    ```bash
    python -m playwright install chromium
    ```
    Without this step, PDF generation will fail.

## License and Warning ⚠️

*   **License:** [The code is an intellectual property of creator Anshuman Raj and is for officical internal use only, no one is allowed to use it for any commercial use with express authors permission].

*   **Disclaimer & Warning:**
    *   **Data Accuracy:** The accuracy of the generated reports depends entirely on the data provided by the source API (`dashboard.nregsmp.org/api`) and the correctness of the `analyze_*.py` scripts at the time of execution. Data may be subject to delays, errors, or inconsistencies from the source.
    *   **AI-Generated Content:** Summaries and recommendations generated using the Anthropic Claude API are for informational purposes. They should be reviewed and validated by subject matter experts before being acted upon. AI models can sometimes produce inaccurate or inappropriate content.
    *   **Not Official Advice:** This tool and its output are intended for monitoring and analysis. They do not constitute official government directives or financial/legal advice unless explicitly sanctioned by the relevant authorities.
    *   **Use Responsibly:** Users should interpret the reports critically and use the information responsibly, considering the potential limitations mentioned above.