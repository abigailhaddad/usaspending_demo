# REAP (Rural Energy for America Program) Dashboard

This project downloads and analyzes federal spending data for the Rural Energy for America Program (REAP, CFDA 10.868) from USAspending.gov.

**Note:** I'm new to working with USAspending data, but I believe this analysis is accurate based on my understanding of the data structure and federal spending processes. Please verify before using for any official purposes.

## Overview

The dashboard tracks how REAP grant money flows from obligation (commitment) to outlay (actual payment) over time. Since REAP grants have multi-year periods of performance, understanding spending patterns requires tracking awards across their full lifecycle.

## Data Pipeline

### 1. Download (`fetch_reap_custom_bulk.py`)
Downloads REAP award data from the USAspending.gov bulk download API. The script is configured to fetch fiscal years 2021-2025. The script:
- Filters to CFDA 10.868 (REAP program code)
- Downloads transaction-level data for each fiscal year
- Saves data as parquet files for efficient storage
- Includes retry logic for handling connection issues

### 2. Process (`prepare_dashboard_data_flexible.py`)
Processes the raw award data into dashboard-ready JSON format. The script:
- Loads all available fiscal year files
- Identifies initial obligation year for each award
- Calculates obligations, outlays, and unspent amounts
- Aggregates data by month, fiscal year, and expiration year
- Tracks deobligations separately from obligations
- Handles the fact that USAspending only provides cumulative outlay totals (not transaction-level outlay timing): outlays are counted once per award, and any outlay figure "by period" is attributed to the period of the award's first positive obligation (see Data Limitations)

### 3. Display (`index.html`)
Interactive dashboard showing multiple views of the data:

#### Summary Statistics
Key metrics displayed at the top (all read from `summary` in the JSON):
- Unique awards (distinct `assistance_award_unique_key`) and transaction rows
- Total obligated, total deobligated, net obligations (per transaction, from `federal_action_obligation`)
- Outlays to date, counting each award's cumulative outlay once, and the outlay rate (outlays to date / net obligations)

An "About these numbers" box on the page states the data's as-of dates, what outlays mean, the known limitation, how the counts reconcile (rows with a zero obligation are not counted in `total_transactions`), and the small state/recipient double-count.

#### Monthly Obligations by Fiscal Year
Bar chart showing new money obligated each month, colored by fiscal year. Vertical lines mark fiscal year boundaries (September 30).

#### Outlays to Date vs. Net Obligations, by Fiscal Year of First Obligation
Groups awards by the fiscal year of their first positive obligation in the data and shows, for each group:
- **Outlayed**: the group's cumulative outlays to date as a share of its net obligations
- **Not Yet Outlayed**: the rest

Each fiscal year's obligations sum to 100% across these two categories.

#### Monthly Deobligations by Fiscal Year
Bar chart showing when money was deobligated (uncommitted) each month. Deobligations can occur when projects are completed under budget or cancelled.

#### Unspent Money by Award End Year
Stacked bar chart showing unspent funds by:
- Which fiscal year the money was originally obligated
- When those awards' periods of performance end

Note: When an award's period of performance ends, obligated funds remain available for liquidation of valid obligations for 5 additional years before the account expires.

## Key Concepts

- **Obligations**: Federal commitment to pay (like signing a contract)
- **Outlays**: Actual payments made (like writing the check)
- **Deobligations**: Reducing or cancelling a previous commitment
- **Multi-year money**: REAP grants typically have 2-3 year periods of performance
- **Cumulative outlays**: USAspending only shows total outlays to date, not when each payment occurred. `total_outlayed_amount_for_overall_award` is repeated on every transaction row of an award, so it must be taken once per award; summing it over rows counts an award once per transaction.

## Data Limitations

- There is no monthly or per-fiscal-year outlay time series in this data. In `data/reap_dashboard_data.json`, `monthly_data`, `cyclical_data` and `year_comparison` give obligations by action month / fiscal year (per transaction, valid) and outlays only as "outlays to date of the awards first obligated in that month / fiscal year" (once per award; the `cohort_*` and `outlays_to_date_of_awards_first_obligated` fields).
- The data's as-of date: the latest action date and latest record-modified date are in `metadata`; the bulk-download date is not recorded in the files. The expired/active split and `years_elapsed` use the date the script is run (`metadata.expiry_reference_date`).
- Outlays are only required to be reported since FY2022 (FY2020 for COVID-19-funded awards), per the USAspending data dictionary, so outlays on FY2021 awards may be incomplete. A blank outlay counts as $0.
- Awards with no positive obligation in the data (including 256 type-8 guaranteed/insured loans with $0 obligation) still carry outlays; they are in the total but in no first-obligation group. `summary` reports their count and amount.
- State and recipient breakdowns count an award that lists several states or recipient names in full in each (`summary.state_outlays_excess_over_total`, `summary.recipient_outlays_excess_over_total`).
- `verify_dashboard_data.py` recomputes the figures independently with DuckDB (`pip install duckdb`).

- We cannot track the timing of individual outlay transactions
- Data shows cumulative outlays as of the download date
- Awards are associated with the fiscal year of their first obligation
- Some recent obligations may show as fully outlayed due to immediate payment structures

## Running the Pipeline

1. Download data: `python fetch_reap_custom_bulk.py`
2. Process data: `python prepare_dashboard_data_flexible.py`
3. View dashboard: Open `index.html` in a web browser

## Data Source

All data comes from USAspending.gov, the official source for federal spending information.

## Adapting for Other Programs

To analyze a different federal program, you would need to modify the download script (`fetch_reap_custom_bulk.py`):

### Understanding the data structure:
- **Awards vs Transactions**: This script downloads award-level summaries (one row per award with cumulative totals)
- For more detailed analysis of obligation timing, you might want transaction-level data (every modification)
- Change `"download_types": ["prime_awards"]` to `["prime_transactions"]` for transaction data

### Key modifications needed:

1. **Change the program filter**:
   ```python
   # Current filter for REAP:
   "program_numbers": ["10.868"],
   
   # For a different CFDA program:
   "program_numbers": ["15.608"],  # Fish and Wildlife
   
   # For multiple programs:
   "program_numbers": ["10.902", "10.912", "10.924"],
   ```

2. **Change or remove the agency filter**:
   ```python
   # Current filter for USDA:
   "agencies": [{"type": "awarding", "tier": "toptier", "name": "Department of Agriculture"}],
   
   # For EPA programs:
   "agencies": [{"type": "awarding", "tier": "toptier", "name": "Environmental Protection Agency"}],
   
   # Or remove entirely to get program across all agencies
   ```

3. **Adjust award types** for contracts vs grants:
   ```python
   # Current filter for grants and cooperative agreements:
   "prime_award_types": ["02", "03", "04", "05", "06", "07", "08", "09", "10", "11"],
   
   # For contracts:
   "prime_award_types": ["A", "B", "C", "D"],
   ```

4. **Modify date ranges** - The script is currently configured for FY2021-2025

The USAspending bulk download API is flexible and can filter by many criteria including federal accounts, specific agencies, award types, recipient locations, and more. The processing and visualization scripts should work without modification for most programs.