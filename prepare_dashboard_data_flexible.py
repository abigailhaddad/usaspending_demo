#!/usr/bin/env python3
"""
Prepare REAP data for dashboard - works with whatever year files are available
"""

import pandas as pd
import json
from datetime import datetime
import numpy as np
import glob
import os

# Award identity. award_id_fain is NULL for one URI-only award, so it cannot be used to
# count or group awards; assistance_award_unique_key is present on every row.
KEY = 'assistance_award_unique_key'

def load_available_data():
    """
    Load whatever REAP data files are available
    """
    # Find all available award files
    award_files = glob.glob('data/reap_fy*_awards.parquet')
    
    if not award_files:
        raise FileNotFoundError("No REAP award files found in data directory")
    
    print(f"📁 Found {len(award_files)} award files:")
    for f in sorted(award_files):
        print(f"   - {os.path.basename(f)}")
    
    # Load and combine all award files
    all_data = []
    years_loaded = []
    
    for file in sorted(award_files):
        df = pd.read_parquet(file)
        # Extract year from filename
        year = file.split('fy')[1].split('_')[0]
        years_loaded.append(year)
        
        # Add fiscal year column if not present
        if 'fiscal_year' not in df.columns:
            df['fiscal_year'] = int(year)
        
        print(f"   Loaded FY{year}: {len(df):,} records")
        all_data.append(df)
    
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n✅ Total records loaded: {len(combined_df):,}")
    
    return combined_df, years_loaded

def award_level_outlays(df, outlay_col, group_col=None):
    """
    total_outlayed_amount_for_overall_award is a cumulative figure for the whole award
    ("the total outlayed amount for a given award across time" per the USAspending data
    dictionary), repeated on every transaction row of that award. Summing it over
    transaction rows counts an award once per transaction, so take it once per award
    (or once per award within each group). A blank value means no outlay was reported
    for the award and counts as $0 (summing skips blanks).
    """
    if group_col is None:
        return float(df.groupby(KEY)[outlay_col].max().sum())
    return df.groupby([group_col, KEY])[outlay_col].max().groupby(level=0).sum()


def build_award_table(df, ob_col, out_col):
    """
    One row per award. Everything here is per award, so nothing is double counted.
      net_obligations   sum of federal_action_obligation over the award's rows in the data
      outlays           the award's cumulative outlays to date (blank counts as 0)
      first_pos_*       date / fiscal year / fiscal month of the award's first POSITIVE
                        obligation in the data (NaT for awards with none)
    """
    # The one-value-per-award assumption behind "take it once" - fail loudly if it breaks
    assert df.groupby(KEY)[out_col].nunique().max() <= 1, "outlay field differs within an award"
    assert df[KEY].notna().all(), "null award key"
    g = df.groupby(KEY)
    tbl = pd.DataFrame({
        'net_obligations': g[ob_col].sum(),
        'outlays': g[out_col].max().fillna(0.0),
        'outlay_reported': g[out_col].count() > 0,
    })
    pos = df[df[ob_col] > 0].groupby(KEY).agg(first_pos_date=('action_date', 'min'), first_pos_fy=('fiscal_year', 'min'))
    tbl = tbl.join(pos)
    m = tbl['first_pos_date'].dt.month
    tbl['first_pos_month'] = tbl['first_pos_date'].dt.to_period('M').astype(str)
    tbl['first_pos_fiscal_month_num'] = m.apply(lambda x: x - 9 if x >= 10 else x + 3)
    return tbl


def prepare_dashboard_data():
    """
    Process REAP data for dashboard visualization
    """
    
    print("📊 Preparing REAP Dashboard Data")
    print("="*70)
    
    # Load whatever data is available
    try:
        df, years_loaded = load_available_data()
        years_str = ", ".join([f"FY{y}" for y in sorted(years_loaded)])
        print(f"📅 Processing data from: {years_str}")
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        return None
    
    # Ensure date columns are datetime
    date_columns = ['action_date', 'period_of_performance_start_date', 'period_of_performance_current_end_date', 'last_modified_date']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Create month column for aggregation
    if 'action_date' in df.columns:
        df['month'] = df['action_date'].dt.to_period('M').astype(str)
        df['fiscal_month'] = df['action_date'].dt.month
        df['calendar_year'] = df['action_date'].dt.year
        
        # Add fiscal month number (1-12 where 1=October)
        df['fiscal_month_num'] = df['fiscal_month'].apply(
            lambda x: x - 9 if x >= 10 else x + 3
        )
        df['fiscal_month_name'] = df['action_date'].dt.strftime('%b')
    
    # Find obligation and outlay columns
    obligation_cols = [col for col in df.columns if 'obligat' in col.lower()]
    outlay_cols = [col for col in df.columns if 'outlay' in col.lower()]
    
    # Use the most likely columns
    obligation_col = next((col for col in obligation_cols if 'federal_action_obligation' in col), 
                         obligation_cols[0] if obligation_cols else None)
    outlay_col = next((col for col in outlay_cols if 'total' in col.lower()), 
                     outlay_cols[0] if outlay_cols else None)
    
    print(f"\n📊 Using columns:")
    print(f"   Obligations: {obligation_col}")
    print(f"   Outlays: {outlay_col}")
    
    award_tbl = build_award_table(df, obligation_col, outlay_col)
    cohort_awards = award_tbl[award_tbl['first_pos_date'].notna()]
    no_pos_awards = award_tbl[award_tbl['first_pos_date'].isna()]

    # Categorize transactions if we have obligation data
    if obligation_col:
        df['transaction_type'] = df[obligation_col].apply(
            lambda x: 'Obligation' if x > 0 else ('Deobligation' if x < 0 else 'Zero')
        )
    
    # Prepare summary statistics
    print("\n📈 Calculating summary statistics...")
    
    summary = {
        'total_transactions': len(df[df[obligation_col] != 0]) if obligation_col else len(df),  # Only count records with obligation activity
        # distinct awards (assistance_award_unique_key); not award_id_fain, which is blank on one award
        'unique_awards': int(df[KEY].nunique()),
        # all rows in the files, and the split behind total_transactions
        'total_transaction_rows': int(len(df)),
        'zero_obligation_rows': int((df[obligation_col] == 0).sum()),
        'fiscal_years': sorted(years_loaded),
        'date_range': {}
    }
    
    # Initialize with defaults to prevent undefined errors
    summary['total_obligated'] = 0.0
    summary['total_deobligated'] = 0.0
    summary['net_obligations'] = 0.0
    summary['total_outlays'] = 0.0
    summary['outlay_rate'] = 0.0
    
    if obligation_col:
        summary['total_obligated'] = float(df[df[obligation_col] > 0][obligation_col].sum())
        summary['total_deobligated'] = float(df[df[obligation_col] < 0][obligation_col].sum())
        summary['net_obligations'] = float(df[obligation_col].sum())
    
    if outlay_col:
        summary['total_outlays'] = award_level_outlays(df, outlay_col)
        summary['outlay_rate'] = (summary['total_outlays'] / summary['net_obligations'] * 100) if summary.get('net_obligations', 0) > 0 else 0
    
    summary['awards_with_outlay_reported'] = int(award_tbl['outlay_reported'].sum())
    summary['awards_without_outlay_reported'] = int((~award_tbl['outlay_reported']).sum())
    # Awards with no positive obligation in the data: they are in total_outlays but belong to
    # no first-obligation cohort. Reported so that cohort totals reconcile to the summary.
    summary['awards_without_positive_obligation'] = int(len(no_pos_awards))
    summary['outlays_on_awards_without_positive_obligation'] = round(float(no_pos_awards['outlays'].sum()), 2)
    summary['net_obligations_on_awards_without_positive_obligation'] = round(float(no_pos_awards['net_obligations'].sum()), 2)
    summary['outlays_on_awards_with_positive_obligation'] = round(float(cohort_awards['outlays'].sum()), 2)
    _cn = float(cohort_awards['net_obligations'].sum())
    summary['outlay_rate_awards_with_positive_obligation'] = (summary['outlays_on_awards_with_positive_obligation'] / _cn * 100) if _cn > 0 else 0
    if 'action_date' in df.columns:
        summary['date_range'] = {
            'start': df['action_date'].min().strftime('%Y-%m-%d'),
            'end': df['action_date'].max().strftime('%Y-%m-%d')
        }
    
    # Monthly time series (if we have dates and obligations)
    monthly_data = []
    if 'month' in df.columns and obligation_col:
        print("   Creating monthly time series...")
        monthly_agg = {
            obligation_col: [
                lambda x: float(x[x > 0].sum()),  # obligations
                lambda x: float(x[x < 0].sum()),  # deobligations
                'sum'  # net
            ],
            KEY: 'count'
        }
        
        monthly = df.groupby('month').agg(monthly_agg).round(2)
        monthly.columns = ['obligations', 'deobligations', 'net_obligations', 'transaction_count']
        # Outlays are cumulative per award, so there is no monthly outlay series. What can be
        # said: outlays to date (counted once per award) for awards whose first positive
        # obligation fell in that month. Months with no such award get 0.
        by_first = cohort_awards.groupby('first_pos_month').agg(
            awards_first_obligated=('outlays', 'size'),
            outlays_to_date_of_awards_first_obligated=('outlays', 'sum'))
        monthly = monthly.join(by_first).fillna({'awards_first_obligated': 0, 'outlays_to_date_of_awards_first_obligated': 0})
        monthly['awards_first_obligated'] = monthly['awards_first_obligated'].astype(int)
        monthly['outlays_to_date_of_awards_first_obligated'] = monthly['outlays_to_date_of_awards_first_obligated'].round(2)
        monthly_data = monthly.reset_index().to_dict('records')
    
    # State summary
    state_data = []
    if 'recipient_state_code' in df.columns and obligation_col:
        print("   Creating state summary...")
        state_agg = {
            obligation_col: [
                lambda x: float(x[x > 0].sum()),
                lambda x: float(x[x < 0].sum()),
                'sum'
            ],
            KEY: 'nunique'
        }
        
        if outlay_col:
            state_agg[outlay_col] = 'sum'
        
        state_summary = df.groupby('recipient_state_code').agg(state_agg).round(2)
        
        # Flatten column names
        state_summary.columns = ['obligations', 'deobligations', 'net_obligations', 'unique_awards'] + (['outlays'] if outlay_col else [])
        if outlay_col:
            state_summary['outlays'] = award_level_outlays(df, outlay_col, 'recipient_state_code').round(2)
        # An award that lists more than one state is counted in full in each, so the state
        # outlays add up to slightly more than total_outlays. Record the excess.
        if outlay_col:
            summary['state_outlays_excess_over_total'] = round(float(state_summary['outlays'].sum()) - summary['total_outlays'], 2)
        state_summary = state_summary[state_summary['net_obligations'] != 0]  # Remove states with no activity
        state_data = state_summary.reset_index().to_dict('records')
    
    # Top recipients
    top_recipients = []
    if 'recipient_name' in df.columns and obligation_col:
        print("   Identifying top recipients...")
        recipient_agg = {
            obligation_col: [
                lambda x: float(x[x > 0].sum()),
                lambda x: float(x[x < 0].sum()),
                'sum'
            ],
            'recipient_state_code': 'first',
            KEY: 'nunique'
        }
        
        if outlay_col:
            recipient_agg[outlay_col] = 'sum'
        
        recipient_summary = df.groupby('recipient_name').agg(recipient_agg).round(2)
        
        # Flatten column names
        recipient_summary.columns = ['obligations', 'deobligations', 'net_obligations', 'state', 'unique_awards'] + (['outlays'] if outlay_col else [])
        if outlay_col:
            recipient_summary['outlays'] = award_level_outlays(df, outlay_col, 'recipient_name').round(2)
        if outlay_col:
            summary['recipient_outlays_excess_over_total'] = round(float(recipient_summary['outlays'].sum()) - summary['total_outlays'], 2)
        top_recipients = recipient_summary.nlargest(20, 'net_obligations').reset_index().to_dict('records')
    
    # Year-over-year comparison if we have multiple years
    year_comparison = []
    if len(years_loaded) > 1 and 'fiscal_year' in df.columns:
        print("   Creating year-over-year comparison...")
        # Obligations are per transaction, so they can be grouped by the fiscal year of the
        # action. Outlays cannot (cumulative per award), so they are attributed to the fiscal
        # year of each award's first positive obligation, counted once per award, and compared
        # with the net obligations of those same awards.
        fy_ob = df.groupby('fiscal_year')[obligation_col].agg(
            obligations=lambda x: float(x[x > 0].sum()),
            deobligations=lambda x: float(x[x < 0].sum()),
            net_obligations='sum')
        fy_ob['awards_with_transactions'] = df.groupby('fiscal_year')[KEY].nunique()
        coh = cohort_awards.groupby('first_pos_fy').agg(
            awards_first_obligated=('outlays', 'size'),
            cohort_net_obligations=('net_obligations', 'sum'),
            cohort_outlays_to_date=('outlays', 'sum'))
        coh['cohort_outlay_rate'] = (coh['cohort_outlays_to_date'] / coh['cohort_net_obligations'] * 100).where(coh['cohort_net_obligations'] > 0)
        year_summary = fy_ob.join(coh).round(2)
        year_summary = year_summary.reset_index()
        year_summary['fiscal_year'] = year_summary['fiscal_year'].astype(int)
        year_comparison = year_summary.to_dict('records')
    
    # Outlay analysis by initial obligation year
    outlay_by_obligation_year = []
    if 'fiscal_year' in df.columns and KEY in df.columns and obligation_col and outlay_col:
        print("   Creating outlay analysis by initial obligation year...")
        
        # First, identify awards that have at least one positive obligation in our data
        # This excludes awards that were obligated before our data period
        awards_with_obligations = df[df[obligation_col] > 0][KEY].unique()
        
        # Filter to only include these awards
        df_with_known_obligations = df[df[KEY].isin(awards_with_obligations)]
        
        # For each award, identify when it was first obligated
        initial_obligations = df_with_known_obligations[df_with_known_obligations[obligation_col] > 0].groupby(KEY).agg({
            'fiscal_year': 'min',
            'action_date': 'min'
        }).rename(columns={'fiscal_year': 'initial_obligation_fy'})
        
        # Get current status for each award (latest values)
        current_status = df.groupby(KEY).agg({
            obligation_col: 'sum',
            outlay_col: 'last',
            'period_of_performance_current_end_date': 'last'
        })
        
        # Merge
        award_analysis = initial_obligations.merge(current_status, on=KEY)
        
        # Current date for expiration check
        current_date = pd.Timestamp.now()
        
        # Analyze by initial obligation FY
        for fy in sorted(award_analysis['initial_obligation_fy'].unique()):
            fy_awards = award_analysis[award_analysis['initial_obligation_fy'] == fy]
            
            total_obligations = float(fy_awards[obligation_col].sum())
            total_outlays = float(fy_awards[outlay_col].sum())
            outlay_rate = (total_outlays / total_obligations * 100) if total_obligations > 0 else 0
            
            # Check expiration status
            expired_awards = fy_awards[fy_awards['period_of_performance_current_end_date'] < current_date]
            active_awards = fy_awards[fy_awards['period_of_performance_current_end_date'] >= current_date]
            
            expired_obligations = float(expired_awards[obligation_col].sum()) if len(expired_awards) > 0 else 0
            expired_outlays = float(expired_awards[outlay_col].sum()) if len(expired_awards) > 0 else 0
            expired_outlay_rate = (expired_outlays / expired_obligations * 100) if expired_obligations > 0 else 0
            
            active_obligations = float(active_awards[obligation_col].sum()) if len(active_awards) > 0 else 0
            active_outlays = float(active_awards[outlay_col].sum()) if len(active_awards) > 0 else 0
            active_outlay_rate = (active_outlays / active_obligations * 100) if active_obligations > 0 else 0
            
            years_elapsed = current_date.year - fy
            
            # Calculate the three components that sum to 100%
            percent_outlayed = (total_outlays / total_obligations * 100) if total_obligations > 0 else 0
            
            # Expired unspent = expired obligations minus what they outlayed
            expired_unspent = expired_obligations - expired_outlays if len(expired_awards) > 0 else 0
            percent_expired_unspent = (expired_unspent / total_obligations * 100) if total_obligations > 0 else 0
            
            # Active unspent = active obligations minus what they've outlayed so far
            active_unspent = active_obligations - active_outlays if len(active_awards) > 0 else 0
            percent_active_unspent = (active_unspent / total_obligations * 100) if total_obligations > 0 else 0
            
            outlay_by_obligation_year.append({
                'initial_obligation_fy': int(fy),
                'years_elapsed': years_elapsed,
                'total_awards': len(fy_awards),
                'total_obligations': total_obligations,
                'total_outlays': total_outlays,
                'overall_outlay_rate': outlay_rate,
                'expired_awards': len(expired_awards),
                'expired_obligations': expired_obligations,
                'expired_outlays': expired_outlays,
                'expired_outlay_rate': expired_outlay_rate,
                'active_awards': len(active_awards),
                'active_obligations': active_obligations,
                'active_outlays': active_outlays,
                'active_outlay_rate': active_outlay_rate,
                # New fields for stacked chart
                'percent_outlayed': percent_outlayed,
                'percent_expired_unspent': percent_expired_unspent,
                'percent_active_unspent': percent_active_unspent
            })
    
    # Money expiration analysis
    money_expiration = []
    if 'period_of_performance_current_end_date' in df.columns and obligation_col and outlay_col:
        print("   Creating money expiration analysis...")
        
        # First, identify initial obligation year for each award (similar to outlay analysis)
        initial_obligations = df[df[obligation_col] > 0].groupby(KEY).agg({
            'fiscal_year': 'min'
        }).rename(columns={'fiscal_year': 'initial_obligation_fy'})
        
        # Get current status for each award (latest values)
        current_status = df.groupby(KEY).agg({
            obligation_col: 'sum',
            outlay_col: 'max',  # Use max for cumulative outlays
            'period_of_performance_current_end_date': 'last'
        })
        
        # Merge to get awards with their initial FY
        awards_with_fy = initial_obligations.merge(current_status, on=KEY)
        
        # For each initial obligation fiscal year, analyze unspent money and when it expires
        for fy in sorted(awards_with_fy['initial_obligation_fy'].unique()):
            fy_awards = awards_with_fy[awards_with_fy['initial_obligation_fy'] == fy].copy()
            
            # Group by expiration year
            fy_awards['expiration_year'] = pd.to_datetime(fy_awards['period_of_performance_current_end_date']).dt.year
            
            # Aggregate by expiration year - calculate unspent AFTER aggregation
            expiration_summary = fy_awards.groupby('expiration_year').agg({
                obligation_col: 'sum',
                outlay_col: 'sum'
            }).round(2)
            
            # Calculate unspent after aggregation (more accurate)
            expiration_summary['unspent_amount'] = expiration_summary[obligation_col] - expiration_summary[outlay_col]
            expiration_summary['unspent_amount'] = expiration_summary['unspent_amount'].clip(lower=0)
            
            # Count awards by expiration year
            awards_by_exp_year = fy_awards.groupby('expiration_year').size()
            
            for exp_year, row in expiration_summary.iterrows():
                if pd.notna(exp_year):
                    # Always include the data, even if unspent is currently small
                    money_expiration.append({
                        'obligation_fy': int(fy),
                        'expiration_year': int(exp_year),
                        'unspent_amount': float(row['unspent_amount']),
                        'total_obligations': float(row[obligation_col]),
                        'total_outlays': float(row[outlay_col]),
                        'awards_count': int(awards_by_exp_year.get(exp_year, 0)),
                        'percent_unspent': float((row['unspent_amount'] / row[obligation_col] * 100) if row[obligation_col] > 0 else 0)
                    })
    
    # Deobligation analysis
    deobligation_analysis = []
    if obligation_col:
        print("   Creating deobligation analysis...")
        
        # Get all deobligation transactions
        deobligations = df[df[obligation_col] < 0].copy()
        
        if len(deobligations) > 0:
            # Monthly deobligation patterns by fiscal year
            for fy in sorted(df['fiscal_year'].unique()):
                fy_deobs = deobligations[deobligations['fiscal_year'] == fy]
                fy_obligations = df[(df['fiscal_year'] == fy) & (df[obligation_col] > 0)]
                
                if len(fy_deobs) > 0:
                    # Monthly breakdown
                    monthly_deobs = fy_deobs.groupby('fiscal_month').agg({
                        obligation_col: ['sum', 'count'],
                        KEY: 'nunique'
                    }).round(2)
                    
                    # Total for the year
                    total_deob_amount = float(fy_deobs[obligation_col].sum())
                    total_obligations = float(fy_obligations[obligation_col].sum())
                    deob_rate = (abs(total_deob_amount) / total_obligations * 100) if total_obligations > 0 else 0
                    
                    # Awards with deobligations
                    awards_with_deobs = set(fy_deobs[KEY].unique())
                    total_awards = set(df[df['fiscal_year'] == fy][KEY].unique())
                    
                    deobligation_analysis.append({
                        'fiscal_year': int(fy),
                        'total_deobligation_amount': abs(total_deob_amount),
                        'total_obligation_amount': total_obligations,
                        'deobligation_rate': deob_rate,
                        'deobligation_count': len(fy_deobs),
                        'awards_with_deobligations': len(awards_with_deobs),
                        'total_awards': len(total_awards),
                        'percent_awards_deobligated': (len(awards_with_deobs) / len(total_awards) * 100) if len(total_awards) > 0 else 0
                    })
    
    # Cyclical analysis - obligations by fiscal month, and outlays to date of the awards first
    # obligated in that month. There is no monthly outlay time series in this data (the outlay
    # field is cumulative per award), so the "outlay" columns here are cohort figures.
    cyclical_data = []
    if 'fiscal_year' in df.columns and 'fiscal_month_num' in df.columns and obligation_col:
        print("   Creating cyclical analysis...")
        first_by_fm = cohort_awards.groupby(['first_pos_fy', 'first_pos_fiscal_month_num']).agg(
            awards_first_obligated=('outlays', 'size'),
            cohort_net_obligations=('net_obligations', 'sum'),
            cohort_outlays_to_date=('outlays', 'sum'))
        for fy in sorted(df['fiscal_year'].unique()):
            fy_df = df[df['fiscal_year'] == fy]
            monthly_fy = fy_df.groupby(['fiscal_month_num', 'fiscal_month_name']).agg(
                monthly_obligations=(obligation_col, 'sum')).reset_index()
            monthly_fy['cumulative_obligations'] = monthly_fy['monthly_obligations'].cumsum()
            for c in ['awards_first_obligated', 'cohort_net_obligations', 'cohort_outlays_to_date']:
                monthly_fy[c] = [first_by_fm[c].get((fy, int(n)), 0) for n in monthly_fy['fiscal_month_num']]
            monthly_fy['cumulative_cohort_net_obligations'] = monthly_fy['cohort_net_obligations'].cumsum()
            monthly_fy['cumulative_cohort_outlays_to_date'] = monthly_fy['cohort_outlays_to_date'].cumsum()
            den = monthly_fy['cumulative_cohort_net_obligations']
            monthly_fy['cohort_outlay_rate'] = (monthly_fy['cumulative_cohort_outlays_to_date'] / den * 100).where(den > 0, 0.0)
            for _, row in monthly_fy.iterrows():
                cyclical_data.append({
                    'fiscal_year': int(fy),
                    'fiscal_month_num': int(row['fiscal_month_num']),
                    'fiscal_month_name': row['fiscal_month_name'],
                    'monthly_obligations': float(row['monthly_obligations']),
                    'cumulative_obligations': float(row['cumulative_obligations']),
                    'awards_first_obligated': int(row['awards_first_obligated']),
                    'cohort_net_obligations': float(row['cohort_net_obligations']),
                    'cohort_outlays_to_date': float(row['cohort_outlays_to_date']),
                    'cumulative_cohort_net_obligations': float(row['cumulative_cohort_net_obligations']),
                    'cumulative_cohort_outlays_to_date': float(row['cumulative_cohort_outlays_to_date']),
                    'cohort_outlay_rate': round(float(row['cohort_outlay_rate']), 1)
                })
    
    # Pooled across fiscal years: outlays to date / net obligations of the awards first obligated
    # in each fiscal month (not cumulative within the year)
    outlay_progression = []
    if cyclical_data:
        cyclical_df = pd.DataFrame(cyclical_data)
        pooled = cyclical_df.groupby(['fiscal_month_num', 'fiscal_month_name']).agg(
            outlays=('cohort_outlays_to_date', 'sum'),
            net=('cohort_net_obligations', 'sum'),
            total_obligations=('monthly_obligations', 'sum')).reset_index()
        month_order = ['Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']
        for month in month_order:
            m = pooled[pooled['fiscal_month_name'] == month]
            if not m.empty:
                net = float(m['net'].iloc[0])
                outlay_progression.append({
                    'month': month,
                    'pooled_outlay_rate_to_date': (float(m['outlays'].iloc[0]) / net * 100) if net > 0 else 0.0,
                    'total_obligations': float(m['total_obligations'].iloc[0])
                })
    
    # Prepare sample of recent transactions for table
    transactions_data = []
    if obligation_col:
        print("   Preparing transaction details...")
        
        # Select relevant columns for the table
        table_columns = [
            'award_id_fain',
            'recipient_name',
            'recipient_state_code',
            'recipient_city_name',
            'action_date',
            obligation_col,
            'award_description',
            'awarding_sub_agency_name',
            'fiscal_year'
        ]
        
        if outlay_col:
            table_columns.append(outlay_col)
        
        if 'transaction_type' in df.columns:
            table_columns.append('transaction_type')
        
        # Keep only columns that exist
        table_columns = [col for col in table_columns if col in df.columns]
        
        transactions_table = df[table_columns].copy()
        
        # Format dates
        if 'action_date' in transactions_table.columns:
            transactions_table['action_date'] = transactions_table['action_date'].dt.strftime('%Y-%m-%d')
            transactions_table = transactions_table.sort_values('action_date', ascending=False)
        
        # Round numeric columns
        if obligation_col in transactions_table.columns:
            transactions_table[obligation_col] = transactions_table[obligation_col].round(2)
        if outlay_col and outlay_col in transactions_table.columns:
            transactions_table[outlay_col] = transactions_table[outlay_col].round(2)
        
        # Convert to records
        transactions_data = transactions_table.to_dict('records')
    
    # Compile all data
    dashboard_data = {
        'summary': summary,
        'monthly_data': monthly_data,
        'state_data': state_data,
        'top_recipients': top_recipients,
        'year_comparison': year_comparison,
        'cyclical_data': cyclical_data,
        'outlay_by_obligation_year': outlay_by_obligation_year,
        'money_expiration': money_expiration,
        'deobligation_analysis': deobligation_analysis,
        'outlay_progression': outlay_progression,
        'transactions': transactions_data,
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'source': 'USASpending API - CFDA 10.868 (REAP)',
            'fiscal_years': sorted(years_loaded),
            'fiscal_years_string': years_str,
            # What the data is "as of". Outlays are cumulative as of the bulk-download date, which
            # the files do not record; the latest dates inside the files bound it from below.
            'data_latest_action_date': summary['date_range'].get('end') if summary.get('date_range') else None,
            'data_latest_record_modified': df['last_modified_date'].max().strftime('%Y-%m-%d') if 'last_modified_date' in df.columns and pd.notna(df['last_modified_date'].max()) else None,
            # expired/active in outlay_by_obligation_year and years_elapsed use the date the
            # script is RUN, not the date of the data
            'expiry_reference_date': pd.Timestamp.now().strftime('%Y-%m-%d'),
        }
    }
    
    # Save to JSON (handle NaN and Infinity values)
    output_file = 'data/reap_dashboard_data.json'
    # Convert NaN/Infinity to None for JSON compatibility
    dashboard_json = json.dumps(dashboard_data, indent=2, default=str)
    dashboard_json = dashboard_json.replace('NaN', 'null')
    dashboard_json = dashboard_json.replace('Infinity', 'null')
    dashboard_json = dashboard_json.replace('-Infinity', 'null')
    with open(output_file, 'w') as f:
        f.write(dashboard_json)
    
    print(f"\n💾 Dashboard data saved to: {output_file}")
    print(f"   File size: {len(dashboard_json) / 1024 / 1024:.1f} MB")
    
    return dashboard_data

if __name__ == "__main__":
    data = prepare_dashboard_data()