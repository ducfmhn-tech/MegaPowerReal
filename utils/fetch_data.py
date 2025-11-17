import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.90 Safari/537.36"
}

def fetch_table(url, table_index=2, timeout_sec=30):
    """
    Fetch bảng Mega/Power từ url, với timeout, retry và table_index
    Trả về: df, số rows
    """
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502,503,504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    try:
        print(f"🔹 Fetching {url} ...")
        r = session.get(url, headers=HEADERS, timeout=timeout_sec)
        r.raise_for_status()
        tables = pd.read_html(r.text)

        if len(tables) > table_index:
            df = tables[table_index]
            # Kiểm tra ít nhất 2 cột
            if df.empty or df.shape[1] < 2:
                print(f"⚠ Bảng fetch về có thể không đúng định dạng")
                return pd.DataFrame(), 0
            # Loại bỏ trùng
            df = df.drop_duplicates().reset_index(drop=True)
            print(f"✔ Fetched {len(df)} rows from {url}")
            return df, len(df)
        else:
            print(f"⚠ Không tìm thấy bảng thứ {table_index} trên {url}")
            return pd.DataFrame(), 0

    except Exception as e:
        print(f"❌ Lỗi fetch {url}: {e}")
        return pd.DataFrame(), 0

def fetch_all_data(limit=100):
    mega_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "https://www.lotto-8.com/Vietnam/listltoVM45.asp"
    ]
    power_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
        "https://www.lotto-8.com/Vietnam/listltoVM55.asp"
    ]

    # --- Fetch Mega ---
    mega_df = pd.DataFrame()
    total_mega = 0
    for url in mega_urls:
        if total_mega >= limit:
            break
        df, n = fetch_table(url, table_index=2, timeout_sec=30)
        mega_df = pd.concat([mega_df, df], ignore_index=True).drop_duplicates().reset_index(drop=True)
        total_mega = len(mega_df)
        print(f"🔹 Tổng số rows Mega hiện tại: {total_mega}")

    # --- Fetch Power ---
    power_df = pd.DataFrame()
    total_power = 0
    for url in power_urls:
        if total_power >= limit:
            break
        df, n = fetch_table(url, table_index=2, timeout_sec=30)
        power_df = pd.concat([power_df, df], ignore_index=True).drop_duplicates().reset_index(drop=True)
        total_power = len(power_df)
        print(f"🔹 Tổng số rows Power hiện tại: {total_power}")

    # --- Giới hạn về limit ---
    mega_df = mega_df.head(limit)
    power_df = power_df.head(limit)
    print(f"✅ Fetch xong: Mega={len(mega_df)} rows, Power={len(power_df)} rows")
    
    return mega_df, power_df
from utils.fetch_data import fetch_all_data

LIMIT = 100
mega_df, power_df = fetch_all_data(limit=LIMIT)

print(f"🔥 Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")
