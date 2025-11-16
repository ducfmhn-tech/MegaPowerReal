# utils/report.py
import os
from datetime import datetime
import pandas as pd
from utils.logger import log

def generate_report(mega_df, power_df, metrics, save_dir="data", pred_mega=None, pred_power=None):
    os.makedirs(save_dir, exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(save_dir, f"mega_power_report_{now}.xlsx")
    try:
        with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
            if mega_df is not None and not mega_df.empty:
                mega_df.to_excel(writer, sheet_name="Mega_Full_Data", index=False)
            if power_df is not None and not power_df.empty:
                power_df.to_excel(writer, sheet_name="Power_Full_Data", index=False)
            if pred_mega is not None:
                pd.DataFrame({"predicted_mega": [", ".join(map(str,pred_mega))]}).to_excel(writer, sheet_name="Predicted_Mega", index=False)
            if pred_power is not None:
                pd.DataFrame({"predicted_power": [", ".join(map(str,pred_power))]}).to_excel(writer, sheet_name="Predicted_Power", index=False)
            pd.DataFrame([metrics]).to_excel(writer, sheet_name="Metrics", index=False)
        log(f"📁 Report saved to: {report_path}")
        return report_path
    except Exception as e:
        log(f"⚠ Error generating report: {e}")
        return None
