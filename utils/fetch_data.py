"""
fetch_data.py
- fetch 6/45 & 6/55 results from ketquadientoan.com (table parser)
- save CSV to data/
"""
import requests, re, os
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

MEGA_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"

def _normalize_date(text):
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    try:
        return pd.to_datetime(text, dayfirst=True, errors="coerce").strftime("%Y-%m-%d")
    except:
        return None

def _parse_table_html(html, limit=200):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tbody tr")
    if not rows:
        rows = soup.select("table tr")
    out = []
    for tr in rows[:limit]:
        cols = [td.get_text(strip=True) for td in tr.find_all(["td","th"])]
        if not cols:
            continue

        # tìm chuỗi số trong hàng
        nums = re.findall(r"\d+", " ".join(cols))
        if len(nums) < 6:
            continue

        # cố tìm ngày (từ cột đầu hoặc regex)
        date_token = None
        for c in cols:
            if re.search(r"\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", c):
                date_token = c
                break
        date_iso = _normalize_date(date_token) if date_token else None

        # lấy 6 số cuối
        last6 = list(map(int, nums[-6:]))
        last6.sort()

        row = {
            "date": date_iso or "unknown",
            "n1": last6[0], "n2": last6[1], "n3": last6[2],
            "n4": last6[3], "n5": last6[4], "n6": last6[5],
        }
        out.append(row)

    df = pd.DataFrame(out)

    # Nếu không có cột 'date', thêm cột giả để tránh lỗi
    if "date" not in df.columns:
        df["date"] = "unknown"

    return df

def _fetch_url(url, limit=200):
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent":"Mozilla/5.0"})
        r.encoding = r.apparent_encoding
        return _parse_table_html(r.text, limit=limit)
    except Exception as e:
        print("⚠️ fetch error", url, e)
        return pd.DataFrame(columns=["date","n1","n2","n3","n4","n5","n6"])

def clean(df):
    """Làm sạch dữ liệu, đảm bảo cột date luôn tồn tại"""
    if "date" not in df.columns:
        df["date"] = "unknown"
    df = df.dropna(subset=["date"]).drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"])
    df = df[df["date"] != "unknown"]
    return df.sort_values("date").reset_index(drop=True)

def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    print("🔹 Fetching Mega 6/45...")
    mega_df = _fetch_url(MEGA_URL, limit=limit)
    print("🔹 Fetching Power 6/55...")
    power_df = _fetch_url(POWER_URL, limit=limit)

    mega_df = clean(mega_df)
    power_df = clean(power_df)

    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    mega_df.to_csv(mega_path, index=False)
    power_df.to_csv(power_path, index=False)

    print(f"✅ Fetched mega:{len(mega_df)} rows, power:{len(power_df)} rows")
    return mega_df, power_df
