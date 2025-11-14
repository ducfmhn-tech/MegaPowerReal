# utils/fetch_data.py
import os, re, time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from utils.logger import log
from config import CFG

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Referer": "https://www.google.com/",
}

def _normalize_date(text):
    if not text:
        return None
    # find dd/mm/yyyy or dd-mm-yyyy
    m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text)
    if m:
        try:
            return pd.to_datetime(m.group(1), dayfirst=True, errors="coerce").strftime("%Y-%m-%d")
        except:
            return None
    try:
        return pd.to_datetime(text, dayfirst=True, errors="coerce").strftime("%Y-%m-%d")
    except:
        return None

def _parse_html_text(html, limit=500):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.find_all("tr")
    out = []
    for tr in rows[:limit]:
        text = tr.get_text(" ", strip=True)
        if not text:
            continue
        nums = list(map(int, re.findall(r"\d+", text))) if re.search(r"\d", text) else []
        if len(nums) < 6:
            continue
        date_iso = _normalize_date(text)
        if not date_iso:
            continue
        last6 = nums[-6:]
        last6.sort()
        out.append({
            "date": date_iso,
            "n1": last6[0], "n2": last6[1], "n3": last6[2],
            "n4": last6[3], "n5": last6[4], "n6": last6[5]
        })
    df = pd.DataFrame(out)
    if not df.empty:
        df = df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"]).sort_values("date").reset_index(drop=True)
    return df

def _fetch_url(url, limit=500, retries=3, pause=2):
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=20, headers=HEADERS)
            r.encoding = r.apparent_encoding
            return _parse_html_text(r.text, limit=limit)
        except Exception as e:
            log(f"[Retry {attempt+1}/{retries}] fetch error {url}: {e}")
            if attempt < retries-1:
                time.sleep(pause)
    return pd.DataFrame()

def fetch_all_data(limit=100, save_dir=None):
    if save_dir is None:
        save_dir = CFG["data_dir"]
    os.makedirs(save_dir, exist_ok=True)
    log("🔹 Fetching Mega 6/45...")
    mega_df = _fetch_url(CFG["mega_url"], limit=limit)
    log("🔹 Fetching Power 6/55...")
    power_df = _fetch_url(CFG["power_url"], limit=limit)
    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    try:
        mega_df.to_csv(mega_path, index=False)
        power_df.to_csv(power_path, index=False)
    except Exception as e:
        log(f"⚠️ Error saving raw CSV: {e}")
    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df
