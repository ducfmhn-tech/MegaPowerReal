# utils/fetch_checks.py
import os, pandas as pd
from utils.logger import log

def load_saved(data_dir="data"):
    mega_fp = os.path.join(data_dir, "mega_6_45_raw.csv")
    power_fp = os.path.join(data_dir, "power_6_55_raw.csv")
    mega = pd.read_csv(mega_fp) if os.path.exists(mega_fp) else None
    power = pd.read_csv(power_fp) if os.path.exists(power_fp) else None
    return mega, power

def quick_validate(df, kind="mega"):
    if df is None or df.empty:
        log(f"⚠ {kind} missing or empty")
        return False
    rng = (1,45) if kind=="mega" else (1,55)
    ok = True
    for i in range(1,7):
        col = f"n{i}"
        if col not in df.columns:
            log(f"⚠ {kind} missing column {col}")
            ok = False
        else:
            if not df[col].between(rng[0], rng[1]).all():
                log(f"⚠ {kind} column {col} has values outside {rng}")
                ok = False
    if 'date' not in df.columns:
        log(f"⚠ {kind} missing date column")
        ok = False
    log(f"ℹ {kind}: {len(df)} rows; date range: {df['date'].min()} → {df['date'].max()}" if ok else f"ℹ {kind} check failed")
    return ok

def print_head(df, n=5):
    if df is None or df.empty:
        log("Empty DF")
    else:
        log(df.head(n).to_string(index=False))
