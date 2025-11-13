"""
fetch_data.py — tải và chuẩn hoá dữ liệu Mega 6/45 & Power 6/55 từ ketquadientoan.com
"""
import os, re, requests, pandas as pd
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
        return pd.to_datetime(text, dayfirst=True).strftime("%Y-%m-%d")
    except:
        return None


def _parse_table_any(html, limit=200):
    soup = BeautifulSoup(html, "lxml")
    out = []

    # lấy tất cả các dòng có thể chứa kết quả
    for tr in soup.find_all("tr"):
        txt = tr.get_text(" ", strip=True)
        if not txt or len(re.findall(r"\d+", txt)) < 6:
            continue

        # tìm ngày (đầu tiên có dạng dd/mm/yyyy)
        m = re.search(r"\d{1,2}/\d{1,2}/\d{4}", txt)
        date_iso = _normalize_date(m.group(0)) if m else None

        # tìm tất cả số trong dòng
        nums = re.findall(r"\b\d{1,2}\b", txt)
        if len(nums) < 6:
            continue

        nums = list(map(int, nums[-6:]))  # lấy 6 số cuối
        nums.sort()
        row = {"date": date_iso}
        for i, n in enumerate(nums, 1):
            row[f"n{i}"] = n
        out.append(row)

    df = pd.DataFrame(out)
    if not df.empty and "date" in df.columns:
        df = df.drop_duplicates(subset=["date"]).dropna(subset=["date"])
        df = df.head(limit).sort_values("date").reset_index(drop=True)
    return df


def _fetch_url(url, limit=200):
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        r.encoding = r.apparent_encoding
        df = _parse_table_any(r.text, limit=limit)
        if df.empty:
            print(f"⚠️ No valid rows parsed from {url}")
        return df
    except Exception as e:
        print(f"❌ fetch error {url}: {e}")
        return pd.DataFrame()


def fetch_all_data(limit=100, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)

    print("🔹 Fetching Mega 6/45...")
    mega_df = _fetch_url(MEGA_URL, limit=limit)
    print("🔹 Fetching Power 6/55...")
    power_df = _fetch_url(POWER_URL, limit=limit)

    if mega_df.empty or "date" not in mega_df.columns:
        print("⚠️ Mega data empty or missing 'date' column!")
        mega_df = pd.DataFrame(columns=["date","n1","n2","n3","n4","n5","n6"])
    if power_df.empty or "date" not in power_df.columns:
        print("⚠️ Power data empty or missing 'date' column!")
        power_df = pd.DataFrame(columns=["date","n1","n2","n3","n4","n5","n6"])

    mega_path = os.path.join(save_dir, "mega_6_45_raw.csv")
    power_path = os.path.join(save_dir, "power_6_55_raw.csv")
    mega_df.to_csv(mega_path, index=False)
    power_df.to_csv(power_path, index=False)

    print(f"✅ Saved: Mega {len(mega_df)} rows | Power {len(power_df)} rows")
    if not mega_df.empty: print("🔸 Mega latest:", mega_df.tail(2))
    if not power_df.empty: print("🔸 Power latest:", power_df.tail(2))

    return mega_df, power_df


# Manual test
if __name__ == "__main__":
    m, p = fetch_all_data(limit=10)
    print(m.tail())
    print(p.tail())
