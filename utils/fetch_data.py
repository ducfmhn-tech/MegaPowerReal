"""
fetch_data.py — phiên bản ổn định
Tải kết quả Mega 6/45 & Power 6/55 từ ketquadientoan.com
"""
import requests, re, os
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

MEGA_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html"
POWER_URL = "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html"

def _normalize_date(text):
    """Chuẩn hoá chuỗi ngày về ISO (YYYY-MM-DD)"""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(text.strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    # fallback
    try:
        return pd.to_datetime(text, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None

def _parse_table_html(html, limit=200):
    soup = BeautifulSoup(html, "lxml")
    rows = soup.select("table tr")
    data = []

    for tr in rows[:limit]:
        tds = [td.get_text(" ", strip=True) for td in tr.find_all(["td", "th"])]
        if not tds:
            continue

        # tìm chuỗi chứa ngày
        date_token = None
        for t in tds:
            if re.search(r"\d{1,2}/\d{1,2}/\d{2,4}", t):
                date_token = t
                break
        date_iso = _normalize_date(date_token) if date_token else None

        # lấy tất cả số trong hàng
        nums = re.findall(r"\d+", " ".join(tds))
        if len(nums) < 6:
            continue
        nums = list(map(int, nums[-6:]))
        nums.sort()

        row = {"date": date_iso}
        for i in range(6):
            row[f"n{i+1}"] = nums[i]
        data.append(row)

    if not data:
        print("⚠️ Không tìm thấy dữ liệu hợp lệ.")
    return pd.DataFrame(data)

def _fetch_url(url, limit=200):
    try:
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = r.apparent_encoding
        df = _parse_table_html(r.text, limit=limit)
        if "date" not in df.columns:
            df["date"] = None
        return df
    except Exception as e:
        print("⚠️ fetch error", url, e)
        return pd.DataFrame(columns=["date", "n1", "n2", "n3", "n4", "n5", "n6"])

def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    mega_df = _fetch_url(MEGA_URL, limit=limit)
    power_df = _fetch_url(POWER_URL, limit=limit)

    def clean(df):
        df = df.dropna(subset=["date"], how="any").drop_duplicates()
        df = df[df["date"].apply(lambda x: isinstance(x, str) and len(x) >= 8)]
        return df.sort_values("date").reset_index(drop=True)

    mega_df = clean(mega_df)
    power_df = clean(power_df)

    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    mega_df.to_csv(mega_path, index=False)
    power_df.to_csv(power_path, index=False)

    print(f"✅ Fetched Mega: {len(mega_df)} rows | Power: {len(power_df)} rows")
    return mega_df, power_df
