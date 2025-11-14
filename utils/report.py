import os, pandas as pd
from datetime import datetime
import os
from utils.logger import log

def get_latest_report(folder="data"):
    """
    Tìm file báo cáo gần nhất theo thời gian trong thư mục data/.
    Trả về đường dẫn file hoặc None nếu không có file nào.
    """
    if not os.path.exists(folder):
        log("⚠️ Folder 'data' does not exist.")
        return None

    files = [f for f in os.listdir(folder) if f.endswith(".xlsx")]

    if not files:
        log("⚠️ No report files found in data/")
        return None

    # sort theo thời gian tạo file
    files = sorted(files, key=lambda f: os.path.getmtime(os.path.join(folder, f)), reverse=True)
    latest = os.path.join(folder, files[0])

    log(f"📁 Latest report found: {latest}")
    return latest
    
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
