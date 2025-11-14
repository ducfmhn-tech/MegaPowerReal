# utils/fetch_data.py
import requests, re, os, pandas as pd
from bs4 import BeautifulSoup
from utils.logger import log

MEGA_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"

def _normalize_date(text):
    if not text: return None
    m = re.search(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})", text)
    if not m:
        try:
            return pd.to_datetime(text, dayfirst=True, errors='coerce').strftime("%Y-%m-%d")
        except:
            return None
    raw = m.group(1).replace("/", "-")
    try:
        return pd.to_datetime(raw, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None

def _parse_page(url, limit=500):
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "lxml")
    except Exception as e:
        log(f"fetch error {url}: {e}")
        return pd.DataFrame()

    rows = soup.find_all("tr")
    out = []
    for tr in rows[:limit]:
        text = tr.get_text(" ", strip=True)
        nums = list(map(int, re.findall(r"\d+", text)))
        if len(nums) < 6:
            continue
        date_iso = _normalize_date(text)
        if not date_iso:
            continue
        six = nums[-6:]
        six.sort()
        out.append({
            "date": date_iso,
            "n1": six[0], "n2": six[1], "n3": six[2],
            "n4": six[3], "n5": six[4], "n6": six[5]
        })
    df = pd.DataFrame(out)
    if "date" in df.columns:
        df = df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"])
        df = df.sort_values("date").reset_index(drop=True)
    return df

def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    log("🔹 Fetching Mega 6/45...")
    mega_df = _parse_page(MEGA_URL, limit=limit)
    log("🔹 Fetching Power 6/55...")
    power_df = _parse_page(POWER_URL, limit=limit)
    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    mega_df.to_csv(mega_path, index=False)
    power_df.to_csv(power_path, index=False)
    log(f"✅ Fetched Mega: {len(mega_df)} rows, Power: {len(power_df)} rows")
    return mega_df, power_df
