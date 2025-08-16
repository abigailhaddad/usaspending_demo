#!/usr/bin/env python3
"""
Fetch REAP (CFDA 10.868) data using custom bulk download request
Can fetch either award summaries or transaction details
"""

import requests
import pandas as pd
import zipfile
import io
import os
import time
from datetime import datetime
import warnings
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from pathlib import Path
import hashlib

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION - Change this to control what type of data to fetch
# ============================================================================
DOWNLOAD_TYPE = "awards"  # Options: "awards" or "transactions"
# "awards" = Award summaries with total obligations and outlays
# "transactions" = Individual transaction details

# Resume download configuration
TEMP_DOWNLOAD_DIR = "temp_downloads"

# Create a session with retry logic
def create_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=1,  # Exponential backoff: 1, 2, 4, 8, 16 seconds
        status_forcelist=(500, 502, 503, 504)
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=10,
        pool_maxsize=10
    )
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def request_custom_bulk_download(start_date, end_date, fiscal_year):
    """
    Request a custom bulk download for REAP data for a specific date range
    """
    
    base_url = "https://api.usaspending.gov/api/v2/"
    
    # Request custom award download
    print(f"📝 Requesting custom bulk download for REAP (10.868) - FY{fiscal_year}...")
    print(f"   Date range: {start_date} to {end_date}")
    
    payload = {
        "filters": {
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "toptier",
                    "name": "Department of Agriculture"
                }
            ],
            "program_numbers": ["10.868"],  # REAP CFDA
            "prime_award_types": [
                "02",  # Block Grant
                "03",  # Formula Grant  
                "04",  # Project Grant
                "05",  # Cooperative Agreement
                "06",  # Direct Payment for Specified Use
                "07",  # Direct Loan
                "08",  # Guaranteed/Insured Loan
                "09",  # Insurance
                "10",  # Direct Payment with Unrestricted Use
                "11"   # Other Financial Assistance
            ],
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        },
        "file_format": "csv",
        "columns": [],  # Empty means all columns
        "download_types": ["prime_awards"]  # Get award summaries, not transactions
    }
    
    # Start the download request - using award summaries endpoint
    response = requests.post(
        base_url + "bulk_download/awards/",
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ Error initiating download: {response.status_code}")
        print(f"Response: {response.text}")
        return None
    
    result = response.json()
    
    if 'file_url' in result:
        # File is ready immediately
        return result['file_url']
    elif 'status_url' in result:
        # Need to poll for status
        return check_download_status(result['status_url'])
    else:
        print(f"❌ Unexpected response: {result}")
        return None

def check_download_status(status_url):
    """
    Poll the status URL until the file is ready
    """
    
    print(f"⏳ Checking download status...")
    print(f"   Status URL: {status_url}")
    
    max_attempts = 60  # Max 10 minutes
    attempt = 0
    
    while attempt < max_attempts:
        attempt += 1
        
        # Check status
        response = requests.get(status_url)
        
        if response.status_code != 200:
            print(f"❌ Error checking status: {response.status_code}")
            return None
        
        data = response.json()
        status = data.get('status', '').lower()
        
        if status == 'finished':
            file_url = data.get('file_url')
            if file_url:
                print(f"✅ Download ready!")
                return file_url
            else:
                # Sometimes the URL is in a different field
                url = data.get('url')
                if url:
                    print(f"✅ Download ready!")
                    return url
                else:
                    print(f"❌ File ready but no URL found in response: {data}")
                    return None
                    
        elif status == 'failed':
            print(f"❌ Download generation failed")
            print(f"   Response: {data}")
            return None
            
        elif status in ['pending', 'running', 'started']:
            # Still processing
            if attempt % 10 == 0:
                print(f"   Still processing... (attempt {attempt}/{max_attempts})")
            time.sleep(10)  # Wait 10 seconds before next check
            
        else:
            print(f"   Status: {status}")
            time.sleep(10)
    
    print(f"❌ Timeout waiting for download to complete")
    return None

def download_and_process_file(file_url, fiscal_year=None, max_attempts=10):
    """
    Download the bulk file with resume capability
    """
    
    print(f"\n📥 Downloading file...")
    print(f"   URL: {file_url}")
    
    # Setup temp file for resumable download
    temp_dir = Path(TEMP_DOWNLOAD_DIR)
    temp_dir.mkdir(exist_ok=True)
    
    # Create unique temp filename based on URL hash
    url_hash = hashlib.md5(file_url.encode()).hexdigest()[:10]
    temp_file = temp_dir / f"download_{fiscal_year or url_hash}.tmp"
    
    # Check if server supports range requests
    session = create_session()
    try:
        head_response = session.head(file_url, timeout=30)
        total_size = int(head_response.headers.get('content-length', 0))
        supports_resume = head_response.headers.get('accept-ranges', '').lower() == 'bytes'
    except:
        total_size = 0
        supports_resume = False
    
    # Check existing download progress
    downloaded_size = 0
    if temp_file.exists():
        downloaded_size = temp_file.stat().st_size
        if downloaded_size >= total_size and total_size > 0:
            print(f"✅ Download already complete, processing file...")
            with open(temp_file, 'rb') as f:
                content = f.read()
            df = process_downloaded_content(content)
            if not df.empty:
                temp_file.unlink()  # Clean up temp file
            return df
    
    if total_size > 0:
        print(f"📦 File size: {total_size / 1024 / 1024:.1f} MB")
    if downloaded_size > 0:
        print(f"🔄 Resuming from: {downloaded_size / 1024 / 1024:.1f} MB ({downloaded_size/total_size*100:.1f}%)")
    
    # Download with resume support
    for attempt in range(1, max_attempts + 1):
        try:
            headers = {}
            if supports_resume and downloaded_size > 0:
                headers['Range'] = f'bytes={downloaded_size}-'
            
            response = session.get(file_url, headers=headers, stream=True, timeout=120)
            
            if response.status_code == 403:
                print(f"   File not ready yet (will retry later)")
                return pd.DataFrame()
            elif response.status_code == 416:  # Range not satisfiable
                print("⚠️  Invalid range, restarting download...")
                temp_file.unlink()
                downloaded_size = 0
                continue
            elif response.status_code not in [200, 206]:
                print(f"❌ Error response: {response.status_code}")
                if attempt < max_attempts:
                    time.sleep(min(30, 2 ** attempt))
                    continue
                return pd.DataFrame()
            
            # Download in chunks
            mode = 'ab' if downloaded_size > 0 else 'wb'
            with open(temp_file, mode) as f:
                chunk_size = 1024 * 1024  # 1MB chunks
                last_update = time.time()
                
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Update progress every 2 seconds
                        if time.time() - last_update > 2:
                            if total_size > 0:
                                progress = (downloaded_size / total_size) * 100
                                print(f"\r⏳ Download progress: {progress:.1f}%", end='', flush=True)
                            else:
                                print(f"\r⏳ Downloaded: {downloaded_size/1024/1024:.1f} MB", end='', flush=True)
                            last_update = time.time()
            
            print("\n✓ Download complete!")
            break
            
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                requests.exceptions.ChunkedEncodingError) as e:
            print(f"\n⚠️  Connection error (attempt {attempt}/{max_attempts}): {str(e)[:100]}...")
            
            if attempt < max_attempts:
                wait_time = min(60, 2 ** attempt)
                print(f"🔄 Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                
                # Update downloaded size for next attempt
                if temp_file.exists():
                    downloaded_size = temp_file.stat().st_size
            else:
                print(f"❌ Failed after {max_attempts} attempts")
                return pd.DataFrame()
    
    # Process the downloaded file
    try:
        with open(temp_file, 'rb') as f:
            content = f.read()
        
        df = process_downloaded_content(content)
        
        # Clean up temp file on success
        if not df.empty:
            temp_file.unlink()
        
        return df
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        return pd.DataFrame()


def process_downloaded_content(content):
    """
    Process downloaded content (zip or csv)
    """
    try:
        # Try to open as zip
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            print("📂 Extracting zip file...")
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            
            if not csv_files:
                print("❌ No CSV files found in zip")
                return pd.DataFrame()
            
            print(f"   Found {len(csv_files)} CSV files")
            
            all_data = []
            for csv_file in csv_files:
                print(f"   Reading {csv_file}...")
                with zf.open(csv_file) as f:
                    df = pd.read_csv(f, low_memory=False)
                    all_data.append(df)
                    print(f"     Loaded {len(df):,} rows")
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                print(f"✓ Total rows loaded: {len(combined_df):,}")
                return combined_df
                
    except zipfile.BadZipFile:
        # Not a zip file, try as CSV directly
        print("📄 Processing as CSV file...")
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        print(f"✓ Loaded {len(df):,} rows")
        return df
    
    return pd.DataFrame()

def process_reap_data(df):
    """
    Process and clean REAP data
    """
    
    if df.empty:
        return df
    
    print("\n🔧 Processing data...")
    print(f"   Total rows before filtering: {len(df):,}")
    
    # Filter for REAP (CFDA 10.868)
    cfda_columns = [col for col in df.columns if 'cfda' in col.lower() or 'program_number' in col.lower()]
    
    if cfda_columns:
        cfda_col = cfda_columns[0]
        print(f"   Using column '{cfda_col}' to filter for CFDA 10.868")
        
        # Convert to string and filter
        df[cfda_col] = df[cfda_col].astype(str)
        reap_df = df[df[cfda_col] == '10.868'].copy()
        
        print(f"   REAP records found: {len(reap_df):,}")
        
        if reap_df.empty:
            print("   ⚠️  No REAP records found! Checking unique CFDA values...")
            unique_cfdas = df[cfda_col].value_counts().head(20)
            print("   Top CFDAs in dataset:")
            for cfda, count in unique_cfdas.items():
                if '10.' in str(cfda):  # Show USDA programs
                    print(f"     {cfda}: {count:,} records")
        
        df = reap_df
    else:
        print("   ⚠️  No CFDA column found! Available columns:")
        for col in df.columns[:20]:
            print(f"     - {col}")
    
    # Identify date columns
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Identify amount columns
    amount_cols = [col for col in df.columns if any(term in col.lower() for term in ['amount', 'obligation', 'outlay', 'value'])]
    for col in amount_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    print(f"✓ Processed {len(df):,} REAP rows")
    
    return df

def analyze_reap_data(df):
    """
    Analyze REAP data
    """
    
    if df.empty:
        print("No data to analyze")
        return
    
    print("\n" + "="*70)
    print("📈 REAP PROGRAM ANALYSIS")
    print("="*70)
    
    print(f"\n📊 Dataset Info:")
    print(f"  Total rows: {len(df):,}")
    print(f"  Total columns: {len(df.columns)}")
    
    # Look for key columns and analyze
    if 'federal_action_obligation' in df.columns:
        total = df['federal_action_obligation'].sum()
        print(f"  Total obligations: ${total:,.2f}")
    elif 'total_obligation' in df.columns:
        total = df['total_obligation'].sum()
        print(f"  Total obligations: ${total:,.2f}")
    
    # Check for unique awards
    award_id_cols = [col for col in df.columns if 'award' in col.lower() and 'id' in col.lower()]
    if award_id_cols:
        unique_awards = df[award_id_cols[0]].nunique()
        print(f"  Unique awards: {unique_awards:,}")
    
    # Date range
    date_cols = [col for col in df.columns if 'date' in col.lower()]
    for col in date_cols:
        if col in df.columns and df[col].notna().any():
            valid_dates = df[col].dropna()
            if not valid_dates.empty:
                print(f"  {col} range: {valid_dates.min()} to {valid_dates.max()}")
                break
    
    # Recipients
    recipient_cols = [col for col in df.columns if 'recipient' in col.lower() and 'name' in col.lower()]
    if recipient_cols:
        print(f"\n🏢 Top 10 Recipients:")
        recipient_col = recipient_cols[0]
        
        # Find obligation column
        obligation_col = None
        for col in ['federal_action_obligation', 'total_obligation', 'award_amount']:
            if col in df.columns:
                obligation_col = col
                break
        
        if obligation_col:
            top_recipients = df.groupby(recipient_col)[obligation_col].sum().sort_values(ascending=False).head(10)
            for recipient, amount in top_recipients.items():
                if pd.notna(recipient):
                    print(f"  {str(recipient)[:50]}: ${amount:,.2f}")
    
    # States
    state_cols = [col for col in df.columns if 'state' in col.lower() and 'code' in col.lower()]
    if state_cols and obligation_col:
        print(f"\n📍 Top 10 States:")
        state_totals = df.groupby(state_cols[0])[obligation_col].sum().sort_values(ascending=False).head(10)
        for state, amount in state_totals.items():
            if pd.notna(state):
                print(f"  {state}: ${amount:,.2f}")

def initiate_download_request(start_date, end_date, fiscal_year):
    """
    Initiate a bulk download request and return the status URL
    """
    base_url = "https://api.usaspending.gov/api/v2/"
    
    print(f"📝 Initiating download request for FY{fiscal_year}...")
    
    payload = {
        "filters": {
            "agencies": [
                {
                    "type": "awarding",
                    "tier": "toptier",
                    "name": "Department of Agriculture"
                }
            ],
            "program_numbers": ["10.868"],  # REAP CFDA
            "prime_award_types": [
                "02",  # Block Grant
                "03",  # Formula Grant  
                "04",  # Project Grant
                "05",  # Cooperative Agreement
                "06",  # Direct Payment for Specified Use
                "07",  # Direct Loan
                "08",  # Guaranteed/Insured Loan
                "09",  # Insurance
                "10",  # Direct Payment with Unrestricted Use
                "11"   # Other Financial Assistance
            ],
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        },
        "file_format": "csv",
        "columns": [],  # Empty means all columns
        "download_types": ["prime_awards" if DOWNLOAD_TYPE == "awards" else "prime_transactions"]
    }
    
    # Start the download request with session
    session = create_session()
    response = session.post(
        base_url + "bulk_download/awards/",
        json=payload,
        headers={"Content-Type": "application/json"}
    )
    
    if response.status_code != 200:
        print(f"❌ Error initiating download for FY{fiscal_year}: {response.status_code}")
        return None
    
    result = response.json()
    
    if 'file_url' in result:
        # File is ready immediately
        return {"fy": fiscal_year, "status": "ready", "url": result['file_url']}
    elif 'status_url' in result:
        # Need to poll for status
        return {"fy": fiscal_year, "status": "pending", "url": result['status_url']}
    else:
        print(f"❌ Unexpected response for FY{fiscal_year}: {result}")
        return None


def process_downloads_as_ready(download_requests):
    """
    Check status of all pending downloads and process them as they become ready
    Also retries downloads that weren't ready on first attempt
    Returns list of successfully processed fiscal years
    """
    # Track different states
    pending_status = download_requests.copy()  # Waiting for API to prepare
    pending_download = []  # API ready but download not yet successful
    processed_years = []
    failed_downloads = []
    
    print(f"\n⏳ Monitoring {len(pending_status)} downloads...")
    
    # Show status URLs for debugging
    for download in pending_status[:2]:  # Show first 2
        print(f"   FY{download['fy']} status URL: {download['url']}")
    
    max_wait_time = 3600  # 60 minutes per session
    start_time = time.time()
    last_status_time = time.time()
    cycle_count = 0
    total_elapsed = 0
    
    while (pending_status or pending_download):
        # Check if an hour has passed
        current_elapsed = time.time() - start_time
        if current_elapsed > max_wait_time:
            total_elapsed += current_elapsed
            hours_elapsed = int(total_elapsed / 3600)
            
            print(f"\n⏰ {hours_elapsed} hour(s) have elapsed.")
            status_fys = [d['fy'] for d in pending_status]
            download_fys = [d['fy'] for d in pending_download]
            
            if status_fys:
                print(f"   Still waiting for API: FY{', FY'.join(map(str, status_fys))}")
            if download_fys:
                print(f"   Still waiting for download: FY{', FY'.join(map(str, download_fys))}")
            print(f"   Successfully processed: {len(processed_years)} files")
            
            # Ask user if they want to continue
            user_input = input("\n❓ Continue waiting for another hour? (y/n): ").strip().lower()
            if user_input != 'y':
                print("⏹️  Stopping download monitoring...")
                break
            else:
                print("⏳ Continuing for another hour...")
                start_time = time.time()  # Reset the hour timer
        
        cycle_count += 1
        time.sleep(20)  # Check every 20 seconds
        
        # Check pending status downloads
        still_pending_status = []
        
        for download in pending_status:
            # Check if this is already a direct file URL
            if download['url'].endswith('.zip') or download['url'].endswith('.csv'):
                print(f"\n✅ FY{download['fy']} is already a direct download URL!")
                pending_download.append({
                    'fy': download['fy'],
                    'url': download['url'],
                    'attempts': 0
                })
                continue
                
            try:
                session = create_session()
                response = session.get(download['url'], timeout=30)
                
                if response.status_code != 200:
                    still_pending_status.append(download)
                    continue
                
                # Try to parse JSON response
                try:
                    data = response.json()
                    status = data.get('status', '').lower()
                except ValueError as e:
                    # Not JSON - might be the actual file
                    if response.headers.get('content-type', '').startswith('application/zip'):
                        print(f"\n✅ FY{download['fy']} returned zip file directly!")
                        pending_download.append({
                            'fy': download['fy'],
                            'url': download['url'],
                            'attempts': 0
                        })
                        continue
                    else:
                        print(f"⚠️  Non-JSON response for FY{download['fy']}")
                        still_pending_status.append(download)
                        continue
                
                if status == 'finished':
                    file_url = data.get('file_url') or data.get('url')
                    if file_url:
                        print(f"\n✅ FY{download['fy']} API ready! Moving to download queue...")
                        pending_download.append({
                            'fy': download['fy'],
                            'url': file_url,
                            'attempts': 0
                        })
                    else:
                        print(f"❌ FY{download['fy']} finished but no URL found")
                        failed_downloads.append(download['fy'])
                elif status == 'failed':
                    print(f"❌ FY{download['fy']} API generation failed")
                    failed_downloads.append(download['fy'])
                else:
                    still_pending_status.append(download)
            except Exception as e:
                if cycle_count % 5 == 0:  # Only print every 5th cycle to reduce noise
                    print(f"⚠️  Error checking FY{download['fy']} status: {str(e)[:50]}...")
                still_pending_status.append(download)
        
        pending_status = still_pending_status
        
        # Try to download files that are ready
        still_pending_download = []
        
        for download in pending_download:
            download['attempts'] += 1
            
            # Skip if we've tried too many times recently
            if download['attempts'] > 1 and download['attempts'] % 3 != 0:
                still_pending_download.append(download)
                # Occasionally show what we're waiting for
                if download['attempts'] % 9 == 0:
                    print(f"   Still waiting for FY{download['fy']}: {download['url']}")
                continue
            
            print(f"\n📥 Attempting to download FY{download['fy']} (attempt {download['attempts']})...")
            
            # Try to download with minimal retries
            try:
                session = create_session()
                response = session.get(download['url'], stream=True, timeout=60)
                
                if response.status_code == 200:
                    # Success! Process the file
                    print(f"✅ Download started for FY{download['fy']}!")
                    
                    # Download in chunks
                    total_size = int(response.headers.get('content-length', 0))
                    if total_size > 0:
                        print(f"📦 File size: {total_size / 1024 / 1024:.1f} MB")
                    
                    chunks = []
                    downloaded = 0
                    retries_during_download = 0
                    max_retries_during_download = 5
                    
                    try:
                        while True:
                            try:
                                # If resuming after connection error, create new request
                                if downloaded > 0 and retries_during_download > 0:
                                    print(f"\n🔄 Resuming download from {downloaded/1024/1024:.1f} MB...")
                                    headers = {'Range': f'bytes={downloaded}-'}
                                    response = session.get(download['url'], headers=headers, stream=True, timeout=60)
                                    if response.status_code not in [200, 206]:
                                        raise Exception(f"Failed to resume: {response.status_code}")
                                
                                for chunk in response.iter_content(chunk_size=1024*1024):
                                    if chunk:
                                        chunks.append(chunk)
                                        downloaded += len(chunk)
                                        if total_size > 0 and downloaded % (10*1024*1024) == 0:  # Update every 10MB
                                            progress = (downloaded / total_size) * 100
                                            print(f"   Progress: {progress:.1f}%")
                                
                                # Download completed successfully
                                break
                                
                            except (requests.exceptions.ConnectionError, 
                                    requests.exceptions.Timeout,
                                    requests.exceptions.ChunkedEncodingError) as conn_error:
                                retries_during_download += 1
                                if retries_during_download >= max_retries_during_download:
                                    raise conn_error
                                print(f"\n⚠️  Connection interrupted at {downloaded/1024/1024:.1f} MB, retrying ({retries_during_download}/{max_retries_during_download})...")
                                time.sleep(5)
                                session = create_session()  # New session for retry
                        
                        print(f"✓ Download complete for FY{download['fy']}!")
                        
                        # Process the downloaded data
                        content = b''.join(chunks)
                        df = process_downloaded_content(content)
                        
                        if not df.empty:
                            # Process and save
                            df = process_reap_data(df)
                            df['fiscal_year'] = download['fy']
                            
                            # Save
                            os.makedirs('data', exist_ok=True)
                            year_parquet = f'data/reap_fy{download["fy"]}_{DOWNLOAD_TYPE}.parquet'
                            df.to_parquet(year_parquet, index=False)
                            print(f"💾 FY{download['fy']} saved: {len(df):,} rows")
                            
                            processed_years.append(download['fy'])
                        else:
                            print(f"❌ No data in downloaded file for FY{download['fy']}")
                            still_pending_download.append(download)
                            
                    except Exception as e:
                        print(f"❌ Error downloading FY{download['fy']}: {str(e)[:100]}")
                        still_pending_download.append(download)
                        
                elif response.status_code == 403:
                    print(f"   File not ready yet for FY{download['fy']}, will retry later")
                    if download['attempts'] % 6 == 0:  # Show URL every 6th attempt
                        print(f"   URL: {download['url']}")
                    still_pending_download.append(download)
                else:
                    print(f"❌ Download error for FY{download['fy']}: {response.status_code}")
                    if download['attempts'] < 20:  # Keep trying
                        still_pending_download.append(download)
                    else:
                        failed_downloads.append(download['fy'])
                        
            except Exception as e:
                print(f"⚠️  Connection error for FY{download['fy']}: {str(e)[:100]}")
                still_pending_download.append(download)
        
        pending_download = still_pending_download
        
        # Status update
        if (pending_status or pending_download) and (time.time() - last_status_time) > 120:
            elapsed = int((time.time() - start_time) / 60)
            status_fys = [d['fy'] for d in pending_status]
            download_fys = [d['fy'] for d in pending_download]
            
            print(f"\n⏱️  {elapsed} minutes elapsed")
            if status_fys:
                print(f"   Waiting for API: FY{', FY'.join(map(str, status_fys))}")
            if download_fys:
                print(f"   Waiting for download: FY{', FY'.join(map(str, download_fys))}")
            print(f"   Successfully processed: {len(processed_years)} files")
            
            last_status_time = time.time()
    
    # Final status
    if pending_status or pending_download:
        print(f"\n📊 Final Status:")
        print(f"   Total time elapsed: {int(total_elapsed/60)} minutes")
        if pending_status:
            status_fys = [d['fy'] for d in pending_status]
            print(f"   Still waiting for API: FY{', FY'.join(map(str, status_fys))}")
        if pending_download:
            download_fys = [d['fy'] for d in pending_download]
            print(f"   Still waiting for download: FY{', FY'.join(map(str, download_fys))}")
        print(f"   Successfully processed: {len(processed_years)} files")
    
    return processed_years, failed_downloads


def process_downloaded_content(content):
    """
    Process downloaded content (zip or csv)
    """
    try:
        # Try to open as zip
        with zipfile.ZipFile(io.BytesIO(content)) as zf:
            print("📂 Extracting zip file...")
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
            
            if not csv_files:
                print("❌ No CSV files found in zip")
                return pd.DataFrame()
            
            all_data = []
            for csv_file in csv_files:
                with zf.open(csv_file) as f:
                    df = pd.read_csv(f, low_memory=False)
                    all_data.append(df)
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                return combined_df
                
    except zipfile.BadZipFile:
        # Not a zip file, try as CSV directly
        df = pd.read_csv(io.BytesIO(content), low_memory=False)
        return df
    
    return pd.DataFrame()


def process_single_download(fy, file_url):
    """
    Process a single fiscal year download
    Returns True if successful, False otherwise
    """
    try:
        print(f"\n{'='*50}")
        print(f"📅 Processing FY{fy}")
        print(f"{'='*50}")
        
        # Download and process the file
        df = download_and_process_file(file_url)
        
        if not df.empty:
            # Process the data
            df = process_reap_data(df)
            
            # Add fiscal year column
            df['fiscal_year'] = fy
            
            # Analyze
            analyze_reap_data(df)
            
            # Save individual year file
            os.makedirs('data', exist_ok=True)
            year_parquet = f'data/reap_fy{fy}_awards.parquet'
            df.to_parquet(year_parquet, index=False)
            print(f"\n💾 FY{fy} data saved to: {year_parquet}")
            print(f"   Total rows: {len(df):,}")
            
            return True
        else:
            print(f"❌ No data retrieved for FY{fy}")
            return False
    except Exception as e:
        print(f"❌ Error processing FY{fy}: {str(e)}")
        return False


def main():
    """
    Main function - fetches multiple years of REAP data
    """
    
    print(f"🌱 FETCHING REAP (CFDA 10.868) {DOWNLOAD_TYPE.upper()} DATA - MULTIPLE YEARS")
    print("="*70)
    print(f"   Data type: {DOWNLOAD_TYPE}")
    print(f"   {'Award summaries with obligations and outlays' if DOWNLOAD_TYPE == 'awards' else 'Individual transaction details'}")
    
    # Define fiscal years to fetch
    fiscal_years = [
        {"fy": 2021, "start": "2020-10-01", "end": "2021-09-30"},
        {"fy": 2022, "start": "2021-10-01", "end": "2022-09-30"},
        {"fy": 2023, "start": "2022-10-01", "end": "2023-09-30"},
        {"fy": 2024, "start": "2023-10-01", "end": "2024-09-30"},
        {"fy": 2025, "start": "2024-10-01", "end": "2025-09-30"},  # Current fiscal year
    ]
    
    # Step 1: Initiate all download requests
    print("\n📤 STEP 1: Initiating download requests for all fiscal years...")
    print("="*70)
    
    download_requests = []
    for year_info in fiscal_years:
        request_info = initiate_download_request(
            year_info["start"], 
            year_info["end"], 
            year_info["fy"]
        )
        if request_info:
            download_requests.append(request_info)
    
    if not download_requests:
        print("❌ No download requests were successful")
        return pd.DataFrame()
    
    print(f"\n✅ Successfully initiated {len(download_requests)} download requests")
    
    # Step 2: Process downloads as they become ready
    print("\n⏳ STEP 2: Monitoring downloads and processing as they become ready...")
    print("="*70)
    print("   Downloads will be processed out of order as they complete")
    print("   Maximum wait time: 60 minutes")
    
    processed_years, failed_years = process_downloads_as_ready(download_requests)  # Processing is now handled internally
    
    if not processed_years:
        print("❌ No downloads were successfully processed")
        return pd.DataFrame()
    
    # Step 3: Combine all successfully processed years
    print(f"\n{'='*70}")
    print("📊 STEP 3: COMBINING ALL SUCCESSFULLY PROCESSED YEARS")
    print(f"{'='*70}")
    
    all_data = []
    for fy in sorted(processed_years):
        parquet_file = f'data/reap_fy{fy}_{DOWNLOAD_TYPE}.parquet'
        if os.path.exists(parquet_file):
            df = pd.read_parquet(parquet_file)
            all_data.append(df)
            print(f"   Loaded FY{fy}: {len(df):,} rows")
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"\n✅ Successfully combined data from {len(processed_years)} fiscal years:")
        print(f"   Years: {', '.join(f'FY{y}' for y in sorted(processed_years))}")
        print(f"   Total rows: {len(combined_df):,}")
        
        if failed_years:
            print(f"   ⚠️  Failed years: {', '.join(f'FY{y}' for y in sorted(failed_years))}")
        
        # Save combined file
        combined_parquet = f'data/reap_all_years_{DOWNLOAD_TYPE}.parquet'
        combined_df.to_parquet(combined_parquet, index=False)
        print(f"\n💾 Combined data saved to: {combined_parquet}")
        
        # Also save a CSV sample
        csv_sample = f'data/reap_all_years_{DOWNLOAD_TYPE}_sample.csv'
        combined_df.head(5000).to_csv(csv_sample, index=False)
        print(f"💾 Sample (first 5000 rows) saved to: {csv_sample}")
        
        # Final summary analysis
        print(f"\n{'='*70}")
        print("📈 FINAL SUMMARY - ALL YEARS")
        print(f"{'='*70}")
        
        # Analyze by fiscal year
        print("\n📅 Data by Fiscal Year:")
        for fy in sorted(combined_df['fiscal_year'].unique()):
            fy_data = combined_df[combined_df['fiscal_year'] == fy]
            print(f"   FY{fy}: {len(fy_data):,} awards")
            
            # Try to get obligation totals
            for col in ['federal_action_obligation', 'total_obligated_amount', 'award_amount']:
                if col in fy_data.columns:
                    total = fy_data[col].sum()
                    if total > 0:
                        print(f"         Total: ${total:,.2f}")
                        break
        
        # Outlay analysis if available
        outlay_col = None
        for col in combined_df.columns:
            if 'outlay' in col.lower() and 'total' in col.lower():
                outlay_col = col
                break
        
        if outlay_col:
            print(f"\n💰 Outlay Analysis (using '{outlay_col}'):")
            for fy in sorted(combined_df['fiscal_year'].unique()):
                fy_data = combined_df[combined_df['fiscal_year'] == fy]
                
                # Get obligations and outlays
                obligation_col = None
                for col in ['federal_action_obligation', 'total_obligated_amount', 'award_amount']:
                    if col in fy_data.columns:
                        obligation_col = col
                        break
                
                if obligation_col:
                    total_obligated = fy_data[obligation_col].sum()
                    total_outlayed = fy_data[outlay_col].sum()
                    if total_obligated > 0:
                        outlay_rate = (total_outlayed / total_obligated) * 100
                        print(f"   FY{fy}: ${total_obligated:,.0f} obligated, ${total_outlayed:,.0f} outlayed ({outlay_rate:.1f}%)")
        
        print(f"\n✅ Analysis complete! Processed {len(processed_years)} out of {len(fiscal_years)} requested years.")
        return combined_df
    else:
        print("❌ No data files found to combine")
        return pd.DataFrame()

if __name__ == "__main__":
    reap_data = main()