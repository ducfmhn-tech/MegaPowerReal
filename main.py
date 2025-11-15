import os
import json
from datetime import datetime
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models, ensemble_predict_topk
from utils.email_utils import send_email_with_attachment
from utils.logger import log
import pandas as pd

CFG = {
    "data_dir": "data",
    "model_dir": "models",
    "n_periods": 120,
}


def get_latest_report():
    data_dir = CFG["data_dir"]
    if not os.path.exists(data_dir):
        return None

    files = [f for f in os.listdir(data_dir) if f.endswith(".xlsx")]
    if not files:
        return None

    files = sorted(files, reverse=True)
    return os.path.join(data_dir, files[0])


def run_pipeline():
    log("🚀 Starting MegaPowerReal Pipeline...")

    # -------------------------------------------------------
    # FETCH DATA
    # -------------------------------------------------------
    mega_df, power_df = fetch_all_data(limit=CFG["n_periods"], save_dir=CFG["data_dir"])

    log(f"✅ Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    if len(mega_df) < 20 or len(power_df) < 20:
        log("❌ Not enough data to train.")
        return

    # -------------------------------------------------------
    # TRAIN MODELS
    # -------------------------------------------------------
    mega_model = train_models(mega_df, CFG["model_dir"], max_number=45)
    power_model = train_models(power_df, CFG["model_dir"], max_number=55)

    # -------------------------------------------------------
    # PREDICT NEXT NUMBERS
    # -------------------------------------------------------
    log("🎯 Predicting next numbers...")

    mega_pred = ensemble_predict_topk(
        mega_df,
        max_number=45,
        rf_path=mega_model["rf_path"],
        gb_path=mega_model["gb_path"],
        topk=6,
    )

    power_pred = ensemble_predict_topk(
        power_df,
        max_number=55,
        rf_path=power_model["rf_path"],
        gb_path=power_model["gb_path"],
        topk=6,
    )

    log(f"🎲 Mega 6/45 predicted: {mega_pred}")
    log(f"💫 Power 6/55 predicted: {power_pred}")

    # -------------------------------------------------------
    # SAVE JSON
    # -------------------------------------------------------
    info = {
        "timestamp": datetime.now().isoformat(),
        "mega_pred": mega_pred,
        "power_pred": power_pred,
        "metrics": {
            "acc_rf": mega_model["acc_rf"],
            "acc_gb": mega_model["acc_gb"],
        }
    }

    with open("last_prediction.json", "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)

    # -------------------------------------------------------
    # SAVE REPORT
    # -------------------------------------------------------
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"{CFG['data_dir']}/mega_power_report_{timestamp}.xlsx"

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
mega_df.to_excel(writer, index=False, sheet_name="Mega_Data")
        power_df.to_excel(writer, index=False, sheet_name="Power_Data")

        pd.DataFrame({"Mega Prediction": mega_pred}).to_excel(
            writer, index=False, sheet_name="Mega_Predict"
        )
        pd.DataFrame({"Power Prediction": power_pred}).to_excel(
            writer, index=False, sheet_name="Power_Predict"
        )

    log(f"📁 Report saved to: {report_path}")

    # -------------------------------------------------------
    # EMAIL
    # -------------------------------------------------------
    send_email_with_attachment(report_path)

    log("✅ Pipeline completed successfully.")


if __name__ == "__main__":
    run_pipeline()
