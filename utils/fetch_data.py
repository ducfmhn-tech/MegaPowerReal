import os, time, re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
from utils.logger import log

# --- Configuration ---
HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

MEGA_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp"
]
POWER_URLS = [
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp"
]

# --- Core Utility Functions ---

def get_html(url, retry=3, timeout=15):
    """Fetches HTML content from a URL with retry logic."""
    for i in range(retry):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.encoding = r.apparent_encoding or "utf-8"
            r.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            log(f"✔ Fetched HTML OK: {url}")
            return r.text
        except Exception as e:
            log(f"[Retry {i+1}/{retry}] fetch error {url}: {e}")
            time.sleep(1)
    return None

def normalize_date(text):
    """Attempts to parse various date formats into YYYY-MM-DD."""
    if not isinstance(text, str):
        return None
    text = text.strip()
    
    # Remove weekday text if present (e.g., "Thứ 4, 01/01/2025")
    text = re.sub(r"Thứ \d, ?", "", text)
    
    # Regex for common date patterns (dd/mm/yyyy or dd-mm-yyyy)
    m = re.search(r"(\d{1,2}[/\-]\d{1,2}[/\-]\d{4})", text)
    if m:
        for fmt in ("%d/%m/%Y","%d-%m-%Y","%Y-%m-%d"):
            try:
                # Use the matched group for strict parsing
                return datetime.strptime(m.group(1), fmt).strftime("%Y-%m-%d")
            except:
                pass
    
    # Regex for ISO format (yyyy-mm-dd)
    m2 = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m2:
        return m2.group(1)
    
    # Fallback to pandas date parsing
    try:
        return pd.to_datetime(text, dayfirst=True, errors='coerce').strftime("%Y-%m-%d")
    except:
        return None

# --- Specialized Parsers ---

def _extract_and_sort_nums(balls):
    """Extracts numbers from BeautifulSoup elements and returns them sorted."""
    nums = []
    for b in balls[:6]:
        try:
            num = int(b.get_text(strip=True))
            nums.append(num)
        except ValueError:
            continue
    if len(nums) < 6:
        return None
    return sorted(nums)

# Parse ketquadientoan Mega (Custom structure)
def parse_mega_ketquad(html):
    """Parser for ketquadientoan.com (Mega 6/45)."""
    soup = BeautifulSoup(html, "lxml")
    rows = []
    # Targeting the result list structure
    items = soup.select(".result-list.mega .result-row") 
    if not items:
        # Fallback to older .item structure
        items = soup.select(".item")
        
    for it in items:
        # Check for different date/ball selectors
        date_el = it.select_one(".draw-date") or it.select_one(".day")
        balls = it.select(".ball-mega") or it.select(".b45 div")

        if not date_el or len(balls) < 6:
            continue
        
        date = normalize_date(date_el.get_text(strip=True))
        nums = _extract_and_sort_nums(balls)
        
        if not date or not nums:
            continue
            
        rows.append({"date":date, "n1":nums[0],"n2":nums[1],"n3":nums[2],
                     "n4":nums[3],"n5":nums[4],"n6":nums[5], "source":"ketquadientoan_mega"})
    return pd.DataFrame(rows)

# Parse ketquadientoan Power (Custom structure)
def parse_power_ketquad(html):
    """Parser for ketquadientoan.com (Power 6/55)."""
    soup = BeautifulSoup(html, "lxml")
    rows = []
    items = soup.select(".result-list.power .result-row")
    if not items:
        items = soup.select(".item")

    for it in items:
        date_el = it.select_one(".draw-date") or it.select_one(".day")
        balls = it.select(".ball-power") or it.select(".b55 div")
        bonus_el = it.select_one(".ball-bonus") or it.select_one(".ball-yellow")
        
        if not date_el or len(balls) < 6:
            continue
            
        date = normalize_date(date_el.get_text(strip=True))
        nums = _extract_and_sort_nums(balls)
        
        if not date or not nums:
            continue
            
        bonus = int(bonus_el.get_text(strip=True)) if bonus_el else None 
        
        rows.append({"date":date,"n1":nums[0],"n2":nums[1],"n3":nums[2],
                     "n4":nums[3],"n5":nums[4],"n6":nums[5],
                     "bonus":bonus, "source":"ketquadientoan_power"})
    return pd.DataFrame(rows)

# Generalized Parser for table-based pages
def parse_table_html(html, source_name):
    """Uses pandas.read_html to parse tabular data from MinhNgoc and Lotto-8."""
    try:
        df_list = pd.read_html(StringIO(html), flavor='bs4')
    except Exception as e:
        log(f"⚠ Pandas read_html failed for {source_name}: {e}")
        return pd.DataFrame()
        
    if not df_list:
        return pd.DataFrame()
        
    # Assume the largest table is the one with the results
    df = df_list[0] 
    if len(df_list) > 1:
        df = max(df_list, key=lambda x: x.shape[0])
        
    rows = []
    
    # Iterate through each row and attempt to extract date and 6 numbers
    for _, r in df.iterrows():
        try:
            # Join all row values into a single string for regex search
            row_text = " ".join(map(str, r.values)) 
            
            # Find the date first
            date = normalize_date(row_text)
            if not date:
                continue
                
            # Find all numbers in the row text
            nums_raw = re.findall(r"\b\d+\b", row_text)
            # Filter and take only the first 6 valid numbers (max 55 is a safe filter)
            nums = [int(x) for x in nums_raw if 1 <= int(x) <= 55] 
            
            if len(nums) < 6:
                continue
                
            nums = sorted(nums[:6])
            
            rec = {"date":date, "n1":nums[0],"n2":nums[1],"n3":nums[2],
                   "n4":nums[3],"n5":nums[4],"n6":nums[5], "source":source_name}
            rows.append(rec)
            
        except Exception as e:
            log(f"⚠ Error processing row from {source_name}: {e}")
            continue
            
    return pd.DataFrame(rows)

# --- Merging and Finalization ---

def merge_dfs(dfs, game_name, limit=None):
    """Combines and cleans dataframes from multiple sources."""
    dfs = [d for d in dfs if d is not None and not d.empty]
    if not dfs:
        log(f"⚠ No data fetched for {game_name}.")
        return pd.DataFrame()
        
    df = pd.concat(dfs, ignore_index=True)
    
    # Ensure date is properly normalized and drop rows where date failed to normalize
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).apply(normalize_date)
        df = df.dropna(subset=["date"])
        
    # Drop duplicates based on the draw numbers for a specific date
    df = df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"])
    
    # Sort by date descending and limit rows
    df = df.sort_values("date", ascending=False).reset_index(drop=True)
    if limit:
        df = df.head(limit).reset_index(drop=True)
        
    return df

def fetch_all_data(limit=100, save_dir="data"):
    """Public API: Fetches, cleans, merges, and saves all lottery data."""
    os.makedirs(save_dir, exist_ok=True)
    log(f"🔹 Bắt đầu thu thập dữ liệu (limit={limit})...")

    # --- Fetch Mega Data ---
    mega_dfs = []
    for url in MEGA_URLS:
        html = get_html(url)
        if not html:
            continue
            
        if "ketquadientoan" in url:
            mega_dfs.append(parse_mega_ketquad(html))
        elif "minhngoc" in url:
            mega_dfs.append(parse_table_html(html, "minhngoc_mega"))
        elif "lotto-8" in url:
            mega_dfs.append(parse_table_html(html, "lotto8_mega"))
            
    mega_df = merge_dfs(mega_dfs, "Mega", limit=limit)

    # --- Fetch Power Data ---
    power_dfs = []
    for url in POWER_URLS:
        html = get_html(url)
        if not html:
            continue
            
        if "ketquadientoan" in url:
            power_dfs.append(parse_power_ketquad(html))
        elif "minhngoc" in url:
            power_dfs.append(parse_table_html(html, "minhngoc_power"))
        elif "lotto-8" in url:
            power_dfs.append(parse_table_html(html, "lotto8_power"))

    power_df = merge_dfs(power_dfs, "Power", limit=limit)

    # --- Save Results ---
    try:
        mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
        power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)
    except Exception as e:
        log(f"⚠ Không thể lưu CSV thô: {e}")

    log(f"✅ Dữ liệu cuối cùng - Mega: {len(mega_df)} dòng, Power: {len(power_df)} dòng")
    return mega_df, power_df

__all__ = ["fetch_all_data"]
