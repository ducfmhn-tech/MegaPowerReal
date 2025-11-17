# utils/fetch_data.py
import requests
import pandas as pd
from io import StringIO
from time import sleep

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

# cấu hình table index/class riêng theo website
SITE_CONFIGS = {
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html": {"table_index": 2},
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html": {"table_index": 2},
    "https://www.lotto-8.com/Vietnam/listltoVM45.asp": {"table_index": 0},
    "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html": {"table_index": 2},
    "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html": {"table_index": 2},
    "https://www.lotto-8.com/Vietnam/listltoVM55.asp": {"table_index": 0},
}

def fetch_table(url, table_index=2, timeout_sec=30, retries=2):
    """Fetch bảng kết quả từ URL, trả về DataFrame và số rows"""
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout_sec)
            r.raise_for_status()
            tables = pd.read_html(StringIO(r.text))
            if len(tables) > table_index:
                df = tables[table_index]
                return df, len(df)
            else:
                print(f"⚠ Không tìm thấy bảng thứ {table_index} trên {url}")
                return pd.DataFrame(), 0
        except Exception as e:
            print(f"❌ Lỗi fetch {url} (attempt {attempt+1}): {e}")
            sleep(2)
    return pd.DataFrame(), 0

def fetch_all_data(limit_mega=100, limit_power=100):
    mega_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
    ]
    power_urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-power-655.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/power-6x55.html",
        "https://www.lotto-8.com/Vietnam/listltoVM55.asp",
    ]

    mega_df = pd.DataFrame()
    for url in mega_urls:
        config = SITE_CONFIGS.get(url, {"table_index": 2})
        df, n_rows = fetch_table(url, table_index=config["table_index"])
        mega_df = pd.concat([mega_df, df], ignore_index=True).drop_duplicates()
        print(f"🔹 Tổng số rows Mega hiện tại: {len(mega_df)}")
        if len(mega_df) >= limit_mega:
            break
    mega_df = mega_df.head(limit_mega)

    power_df = pd.DataFrame()
    for url in power_urls:
        config = SITE_CONFIGS.get(url, {"table_index": 2})
        df, n_rows = fetch_table(url, table_index=config["table_index"])
        power_df = pd.concat([power_df, df], ignore_index=True).drop_duplicates()
        print(f"🔹 Tổng số rows Power hiện tại: {len(power_df)}")
        if len(power_df) >= limit_power:
            break
    power_df = power_df.head(limit_power)
    
    return mega_df, power_df
