# utils/fetch_data.py

import requests
import pandas as pd
from utils.logger import log

API_MEGA = "https://api.minhngoc.net.vn/v2/kqxsdien-toan/mega-6x45"
API_POWER = "https://api.minhngoc.net.vn/v2/kqxsdien-toan/power-6x55"


def fetch_api(url: str, name: str):
    """Fetch JSON API MinhNgoc."""
    try:
        log(f"🔹 Fetching {name} API: {url}")
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        js = r.json()

        if not js.get("success"):
            log(f"⚠ API {name} trả về success=false")
            return pd.DataFrame()

        rows = js.get("data", [])
        cleaned = []
        for row in rows:
            if not row.get("ketqua"):
                continue

            nums = row["ketqua"]
            if len(nums) < 6:
                continue

            cleaned.append({
                "draw": row.get("ki"),
                "date": row.get("ngay"),
                "n1": int(nums[0]),
                "n2": int(nums[1]),
                "n3": int(nums[2]),
                "n4": int(nums[3]),
                "n5": int(nums[4]),
                "n6": int(nums[5]),
                "bonus": int(row.get("bonus")) if row.get("bonus") else None,
            })

        df = pd.DataFrame(cleaned)
        return df.sort_values("draw", ascending=False)

    except Exception as e:
        log(f"❌ Lỗi API {name}: {e}")
        return pd.DataFrame()


def fetch_all_mega():
    df = fetch_api(API_MEGA, "Mega")
    log(f"➡ Mega rows: {len(df)}")
    return df


def fetch_all_power():
    df = fetch_api(API_POWER, "Power")
    log(f"➡ Power rows: {len(df)}")
    return df


def fetch_all_data():
    """Called by main.py"""
    mega = fetch_all_mega()
    power = fetch_all_power()
    return mega, power
