import os, pandas as pd
from datetime import datetime

def get_latest_report():
    files = sorted(
        [f for f in os.listdir("data") if f.startswith("mega_power_report_") and f.endswith(".xlsx")],
        reverse=True
    )
    if not files:
        return None
    return os.path.join("data", files[0])
    
def save_report_xlsx(save_dir, reports_dir, mega_df, power_df, pred_mega, pred_power, metrics=None, retrain_info=None):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    os.makedirs(reports_dir, exist_ok=True)
    path = os.path.join(reports_dir, f"mega_power_report_{ts}.xlsx")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        if mega_df is not None: mega_df.to_excel(writer, sheet_name="Mega_raw", index=False)
        if power_df is not None: power_df.to_excel(writer, sheet_name="Power_raw", index=False)
        pd.DataFrame([{"Predicted_Mega":", ".join(map(str,pred_mega)), "Predicted_Power":", ".join(map(str,pred_power))}]).to_excel(writer, sheet_name="Prediction", index=False)
        if metrics:
            pd.DataFrame([metrics]).to_excel(writer, sheet_name="Metrics", index=False)
        if retrain_info:
            pd.DataFrame([retrain_info]).to_excel(writer, sheet_name="Retrain", index=False)
    return path
