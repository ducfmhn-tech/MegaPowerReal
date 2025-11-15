# utils/report.py
import os, pandas as pd
from datetime import datetime
from utils.logger import log

def generate_report(mega_df, power_df, pred_mega, pred_power, metrics, save_dir="data"):
    os.makedirs(save_dir, exist_ok=True)
    fname = f"mega_power_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(save_dir, fname)
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        if mega_df is not None: mega_df.to_excel(writer, sheet_name='Mega_raw', index=False)
        if power_df is not None: power_df.to_excel(writer, sheet_name='Power_raw', index=False)
        pd.DataFrame([{"pred_mega": ", ".join(map(str,pred_mega)), "pred_power": ", ".join(map(str,pred_power)), "acc_rf": metrics.get("acc_rf"), "acc_gb": metrics.get("acc_gb"), "time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")}]).to_excel(writer, sheet_name='Prediction', index=False)
    log(f"📁 Report saved to: {path}")
    return path
