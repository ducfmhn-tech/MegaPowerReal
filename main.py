import os
import json
from datetime import datetime
import pandas as pd

from config import CFG
from utils.logger import log
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.email_utils import send_email_with_attachment


# ---------------------------------------------------------
# MAIN PIPELINE
# ---------------------------------------------------------
def run_pipeline():
    log("🚀 Starting MegaPowerReal Pipeline...")

    # Load config
    SAVE_DIR = CFG["data_dir"]
    os.makedirs(SAVE_DIR, exist_ok=True)

    # -----------------------------------------------------
    # STEP 1 — Fetch Mega & Power data
    # -----------------------------------------------------
    mega_df, power_df = fetch_all_data(
        limit=CFG["n_periods"],
        save_dir=SAVE_DIR
    )

    log(f"✅ Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    if len(mega_df) < 20 or len(power_df) < 20:
        log("❌ Not enough data to train.")
        return

    # -----------------------------------------------------
    # STEP 2 — Train models
    # -----------------------------------------------------
    log("🧠 Training models...")

    rf_path = os.path.join(SAVE_DIR, "rf_model.pkl")
    gb_path = os.path.join(SAVE_DIR, "gb_model.pkl")

    metrics = train_models_and_save(
        mega_df,
        power_df,
        rf_path,
        gb_path
    )

    log(f"✅ Training completed | RF acc={metrics.get('acc_rf'):.3f} | GB acc={metrics.get('acc_gb'):.3f}")

    # -----------------------------------------------------
    # STEP 3 — Predict next numbers
    # -----------------------------------------------------
    log("🎯 Predicting next numbers...")

    pred_mega, pred_power = ensemble_predict_topk(
        mega_df, power_df, rf_path, gb_path, topk=6
    )

    log(f"🎲 Mega 6/45 predicted: {pred_mega}")
    log(f"💫 Power 6/55 predicted: {pred_power}")

    # -----------------------------------------------------
    # STEP 4 — Save last_prediction.json
    # -----------------------------------------------------
    ts = datetime.now().isoformat()
    pred_json = {
        "timestamp": ts,
        "mega_pred": pred_mega,
        "power_pred": pred_power,
        "metrics": metrics,
    }

    json_path = os.path.join(SAVE_DIR, "last_prediction.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(pred_json, f, ensure_ascii=False, indent=2)

    log(f"Saved last_prediction.json: {pred_json}")

    # -----------------------------------------------------
    # STEP 5 — Save Excel report
    # -----------------------------------------------------
    now_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(SAVE_DIR, f"mega_power_report_{now_str}.xlsx")

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        mega_df.to_excel(writer, index=False, sheet_name="Mega_Data")
        power_df.to_excel(writer, index=False, sheet_name="Power_Data")

        pd.DataFrame({
            "Mega_Prediction": pred_mega,
            "Power_Prediction": pred_power
        }).to_excel(writer, index=False, sheet_name="Prediction")

        pd.DataFrame([metrics]).to_excel(writer, index=False, sheet_name="Metrics")

    log(f"📁 Report saved to: {report_path}")

    # -----------------------------------------------------
    # STEP 6 — Send email
    # -----------------------------------------------------
    try:
        status = send_email_with_attachment(
            subject="MegaPowerReal – Daily Prediction Report",
            body="Attached is the latest lottery prediction report.",
            attachment_path=report_path
        )

        if status == "ok":
            log(f"📧 Email sent successfully.")
        else:
            log(f"⚠️ Email sending failed: {status}")

    except Exception as e:
        log(f"⚠️ Email sending failed: {e}")

    log("✅ Pipeline completed successfully.")


# ---------------------------------------------------------
# ENTRY
# ---------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
