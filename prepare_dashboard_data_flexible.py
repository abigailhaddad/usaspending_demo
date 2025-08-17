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
    date_columns = ['action_date', 'period_of_performance_start_date', 'period_of_performance_current_end_date']
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
    
    # Categorize transactions if we have obligation data
    if obligation_col:
        df['transaction_type'] = df[obligation_col].apply(
            lambda x: 'Obligation' if x > 0 else ('Deobligation' if x < 0 else 'Zero')
        )
    
    # Prepare summary statistics
    print("\n📈 Calculating summary statistics...")
    
    summary = {
        'total_transactions': len(df[df[obligation_col] != 0]) if obligation_col else len(df),  # Only count records with obligation activity
        'unique_awards': df['award_id_fain'].nunique() if 'award_id_fain' in df.columns else 0,
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
        summary['total_outlays'] = float(df[outlay_col].sum())
        summary['outlay_rate'] = (summary['total_outlays'] / summary['net_obligations'] * 100) if summary.get('net_obligations', 0) > 0 else 0
    
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
            'award_id_fain': 'count'
        }
        
        if outlay_col:
            monthly_agg[outlay_col] = 'sum'
        
        monthly = df.groupby('month').agg(monthly_agg).round(2)
        
        # Flatten column names
        monthly.columns = ['obligations', 'deobligations', 'net_obligations', 'transaction_count'] + (['outlays'] if outlay_col else [])
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
            'award_id_fain': 'nunique'
        }
        
        if outlay_col:
            state_agg[outlay_col] = 'sum'
        
        state_summary = df.groupby('recipient_state_code').agg(state_agg).round(2)
        
        # Flatten column names
        state_summary.columns = ['obligations', 'deobligations', 'net_obligations', 'unique_awards'] + (['outlays'] if outlay_col else [])
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
            'award_id_fain': 'nunique'
        }
        
        if outlay_col:
            recipient_agg[outlay_col] = 'sum'
        
        recipient_summary = df.groupby('recipient_name').agg(recipient_agg).round(2)
        
        # Flatten column names
        recipient_summary.columns = ['obligations', 'deobligations', 'net_obligations', 'state', 'unique_awards'] + (['outlays'] if outlay_col else [])
        top_recipients = recipient_summary.nlargest(20, 'net_obligations').reset_index().to_dict('records')
    
    # Year-over-year comparison if we have multiple years
    year_comparison = []
    if len(years_loaded) > 1 and 'fiscal_year' in df.columns:
        print("   Creating year-over-year comparison...")
        year_agg = {
            'award_id_fain': 'nunique'
        }
        
        if obligation_col:
            year_agg[obligation_col] = 'sum'
        if outlay_col:
            year_agg[outlay_col] = 'sum'
        
        year_summary = df.groupby('fiscal_year').agg(year_agg).round(2)
        year_summary = year_summary.rename(columns={
            'award_id_fain': 'unique_awards',
            obligation_col: 'total_obligations' if obligation_col else None,
            outlay_col: 'total_outlays' if outlay_col else None
        })
        
        # Calculate outlay rate for each year
        if obligation_col and outlay_col:
            year_summary['outlay_rate'] = (year_summary['total_outlays'] / year_summary['total_obligations'] * 100).round(1)
        
        year_comparison = year_summary.reset_index().to_dict('records')
    
    # Outlay analysis by initial obligation year
    outlay_by_obligation_year = []
    if 'fiscal_year' in df.columns and 'award_id_fain' in df.columns and obligation_col and outlay_col:
        print("   Creating outlay analysis by initial obligation year...")
        
        # First, identify awards that have at least one positive obligation in our data
        # This excludes awards that were obligated before our data period
        awards_with_obligations = df[df[obligation_col] > 0]['award_id_fain'].unique()
        
        # Filter to only include these awards
        df_with_known_obligations = df[df['award_id_fain'].isin(awards_with_obligations)]
        
        # For each award, identify when it was first obligated
        initial_obligations = df_with_known_obligations[df_with_known_obligations[obligation_col] > 0].groupby('award_id_fain').agg({
            'fiscal_year': 'min',
            'action_date': 'min'
        }).rename(columns={'fiscal_year': 'initial_obligation_fy'})
        
        # Get current status for each award (latest values)
        current_status = df.groupby('award_id_fain').agg({
            obligation_col: 'sum',
            outlay_col: 'last',
            'period_of_performance_current_end_date': 'last'
        })
        
        # Merge
        award_analysis = initial_obligations.merge(current_status, on='award_id_fain')
        
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
        initial_obligations = df[df[obligation_col] > 0].groupby('award_id_fain').agg({
            'fiscal_year': 'min'
        }).rename(columns={'fiscal_year': 'initial_obligation_fy'})
        
        # Get current status for each award (latest values)
        current_status = df.groupby('award_id_fain').agg({
            obligation_col: 'sum',
            outlay_col: 'max',  # Use max for cumulative outlays
            'period_of_performance_current_end_date': 'last'
        })
        
        # Merge to get awards with their initial FY
        awards_with_fy = initial_obligations.merge(current_status, on='award_id_fain')
        
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
                        'award_id_fain': 'nunique'
                    }).round(2)
                    
                    # Total for the year
                    total_deob_amount = float(fy_deobs[obligation_col].sum())
                    total_obligations = float(fy_obligations[obligation_col].sum())
                    deob_rate = (abs(total_deob_amount) / total_obligations * 100) if total_obligations > 0 else 0
                    
                    # Awards with deobligations
                    awards_with_deobs = set(fy_deobs['award_id_fain'].unique())
                    total_awards = set(df[df['fiscal_year'] == fy]['award_id_fain'].unique())
                    
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
    
    # Cyclical analysis - cumulative obligations and outlays by fiscal month
    cyclical_data = []
    if 'fiscal_year' in df.columns and 'fiscal_month_num' in df.columns and obligation_col:
        print("   Creating cyclical analysis...")
        
        # For each fiscal year, calculate cumulative metrics by month
        for fy in sorted(df['fiscal_year'].unique()):
            fy_df = df[df['fiscal_year'] == fy].copy()
            
            # Group by fiscal month
            monthly_fy = fy_df.groupby(['fiscal_month_num', 'fiscal_month_name']).agg({
                obligation_col: 'sum',
                outlay_col: 'sum' if outlay_col else 'count',
                'award_id_fain': 'count'
            }).reset_index()
            
            # Calculate cumulative values
            monthly_fy['cumulative_obligations'] = monthly_fy[obligation_col].cumsum()
            if outlay_col:
                monthly_fy['cumulative_outlays'] = monthly_fy[outlay_col].cumsum()
                # Handle division by zero
                monthly_fy['outlay_rate'] = 0.0
                mask = monthly_fy['cumulative_obligations'] > 0
                monthly_fy.loc[mask, 'outlay_rate'] = (
                    monthly_fy.loc[mask, 'cumulative_outlays'] / 
                    monthly_fy.loc[mask, 'cumulative_obligations'] * 100
                ).round(1)
            else:
                monthly_fy['cumulative_outlays'] = 0
                monthly_fy['outlay_rate'] = 0
            
            # Add to cyclical data
            for _, row in monthly_fy.iterrows():
                cyclical_data.append({
                    'fiscal_year': int(fy),
                    'fiscal_month_num': int(row['fiscal_month_num']),
                    'fiscal_month_name': row['fiscal_month_name'],
                    'monthly_obligations': float(row[obligation_col]),
                    'monthly_outlays': float(row[outlay_col]) if outlay_col else 0,
                    'cumulative_obligations': float(row['cumulative_obligations']),
                    'cumulative_outlays': float(row['cumulative_outlays']),
                    'outlay_rate': float(row['outlay_rate'])
                })
    
    # Calculate average outlay progression across all years
    outlay_progression = []
    if cyclical_data:
        cyclical_df = pd.DataFrame(cyclical_data)
        avg_by_month = cyclical_df.groupby(['fiscal_month_num', 'fiscal_month_name']).agg({
            'outlay_rate': 'mean',
            'monthly_obligations': 'sum'
        }).reset_index()
        
        month_order = ['Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']
        for month in month_order:
            month_data = avg_by_month[avg_by_month['fiscal_month_name'] == month]
            if not month_data.empty:
                outlay_progression.append({
                    'month': month,
                    'avg_outlay_rate': float(month_data['outlay_rate'].iloc[0]),
                    'total_obligations': float(month_data['monthly_obligations'].iloc[0])
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
            'fiscal_years_string': years_str
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