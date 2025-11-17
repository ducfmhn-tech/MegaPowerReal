# utils/report.py
import os, pandas as pd
from datetime import datetime
from utils.logger import log

def generate_report(mega_df, power_df, metrics, save_dir, pred_mega=None, pred_power=None):
    os.makedirs(save_dir, exist_ok=True)
    fname = f"mega_power_report_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx"
    path = os.path.join(save_dir, fname)
    try:
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            if mega_df is not None and not mega_df.empty:
                mega_df.to_excel(writer, sheet_name="Mega_raw", index=False)
            else:
                pd.DataFrame().to_excel(writer, sheet_name="Mega_raw", index=False)
            if power_df is not None and not power_df.empty:
                power_df.to_excel(writer, sheet_name="Power_raw", index=False)
            else:
                pd.DataFrame().to_excel(writer, sheet_name="Power_raw", index=False)
            meta = {
                "pred_mega": [", ".join(map(str, pred_mega or []))],
                "pred_power": [", ".join(map(str, pred_power or []))],
                "metrics": [str(metrics or {})],
                "generated_at": [datetime.utcnow().isoformat()]
            }
            pd.DataFrame(meta).to_excel(writer, sheet_name="Summary", index=False)
        return path
    except Exception as e:
        log(f"⚠ Report generation failed: {e}")
        return None
