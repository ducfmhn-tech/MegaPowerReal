# utils/report.py
import os, pandas as pd
from datetime import datetime
from utils.logger import log
from config import CFG
import glob

def generate_report(mega_df, power_df, pred_mega, pred_power, save_dir=None):
    if save_dir is None:
        save_dir = CFG["reports_dir"]
    os.makedirs(save_dir, exist_ok=True)
    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(save_dir, f"mega_power_report_{now_tag}.xlsx")
    try:
        with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
            if mega_df is not None and not mega_df.empty:
                mega_df.to_excel(writer, index=False, sheet_name="Mega_6_45")
            if power_df is not None and not power_df.empty:
                power_df.to_excel(writer, index=False, sheet_name="Power_6_55")
            pd.DataFrame({
                "mega_pred":[pred_mega],
                "power_pred":[pred_power],
                "timestamp":[datetime.now().isoformat()]
            }).to_excel(writer, index=False, sheet_name="Prediction")
        log(f"📁 Report saved to: {report_path}")
        return report_path
    except Exception as e:
        log(f"⚠️ Error writing report: {e}")
        return None

def get_latest_report(folder=None):
    if folder is None:
        folder = CFG["reports_dir"]
    pattern = os.path.join(folder, "mega_power_report_*.xlsx")
    files = glob.glob(pattern)
    if not files:
        log("⚠️ No report files found in data/")
        return None
    latest = max(files, key=os.path.getctime)
    log(f"📁 Latest report detected: {latest}")
    return latest
