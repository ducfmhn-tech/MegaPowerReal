"""
features.py
- create window-based frequency features for each number
- add lunar (âm lịch) and ngũ hành features as optional reference features
"""
import pandas as pd, numpy as np, os
from collections import Counter
from lunardate import LunarDate

# map last digit or year to five elements (simplified mapping)
ELEMENT_MAP = {0:"Kim",1:"Mộc",2:"Hoả",3:"Thổ",4:"Thuỷ",5:"Kim",6:"Mộc",7:"Hoả",8:"Thổ",9:"Kim"}

def lunar_element_from_date(date_str):
    try:
        y,m,d = map(int, date_str.split("-"))
        ld = LunarDate.fromSolarDate(y,m,d)
        # simplistic: use lunar year mod 10 to get element
        return ELEMENT_MAP.get(ld.year % 10, "")
    except:
        return ""

def compute_window_counts(df, window=50, max_num=45):
    """
    Compute frequency counts for each number across sliding window (last `window` draws)
    Return series indexed by number 1..max_num
    """
    recent = df.tail(window)
    nums = []
    for i in range(1,7):
        nums += recent[f"n{i}"].dropna().astype(int).tolist()
    counts = Counter(nums)
    out = pd.Series({n: counts.get(n,0) for n in range(1, max_num+1)})
    return out

def build_features_for_all(mega_df, power_df, window=50, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    # mega features (1..45)
    m_counts = compute_window_counts(mega_df, window=window, max_num=45)
    p_counts_for_m = compute_window_counts(power_df, window=window, max_num=45)  # map power counts to mega numbers (if overlap)
    dfm = pd.DataFrame({"num": list(range(1,46))})
    dfm["freq_mega"] = dfm["num"].map(m_counts).fillna(0).astype(int)
    dfm["freq_power_mapped"] = dfm["num"].map(p_counts_for_m).fillna(0).astype(int)
    # add lunar element for last date for reference: not per-number but overall
    last_date = mega_df["date"].dropna().iloc[-1] if not mega_df.empty else None
    dfm["last_lunar_element"] = lunar_element_from_date(last_date) if last_date else ""
    dfm.to_csv(os.path.join(save_dir, "mega_features.csv"), index=False)

    # power features (1..55)
    pc = compute_window_counts(power_df, window=window, max_num=55)
    pmapped = compute_window_counts(mega_df, window=window, max_num=55)
    dfp = pd.DataFrame({"num": list(range(1,56))})
    dfp["freq_power"] = dfp["num"].map(pc).fillna(0).astype(int)
    dfp["freq_mega_mapped"] = dfp["num"].map(pmapped).fillna(0).astype(int)
    last_date_p = power_df["date"].dropna().iloc[-1] if not power_df.empty else None
    dfp["last_lunar_element"] = lunar_element_from_date(last_date_p) if last_date_p else ""
    dfp.to_csv(os.path.join(save_dir, "power_features.csv"), index=False)

    return {"mega_features": dfm, "power_features": dfp}
