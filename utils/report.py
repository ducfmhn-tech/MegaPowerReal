# utils/report.py
import os, pandas as pd
from datetime import datetime
from utils.logger import log

def generate_report(mega_df, power_df, pred_mega, pred_power, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(save_dir, f"mega_power_report_{now_tag}.xlsx")
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        mega_df.to_excel(writer, index=False, sheet_name="Mega_6_45")
        power_df.to_excel(writer, index=False, sheet_name="Power_6_55")
        pd.DataFrame({
            "mega_pred":[pred_mega],
            "power_pred":[pred_power],
            "timestamp":[datetime.now().isoformat()]
        }).to_excel(writer, index=False, sheet_name="Prediction")
    log(f"📁 Report saved to: {report_path}")
    return report_path

def get_latest_report(folder="data"):
    if not os.path.exists(folder): return None
    files = [f for f in os.listdir(folder) if f.startswith("mega_power_report_") and f.endswith(".xlsx")]
    if not files: return None
    files = sorted(files, key=lambda f: os.path.getmtime(os.path.join(folder,f)), reverse=True)
    latest = os.path.join(folder, files[0])
    log(f"📁 Latest report found: {latest}")
    return latest
