# main.py
import os, sys, json
ROOT = os.path.dirname(os.path.abspath(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from utils.logger import log
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.report import generate_report, get_latest_report
from utils.email_utils import send_email_with_attachment
from config import CFG

def run_pipeline():
    log("🚀 Starting MegaPowerReal Pipeline...")
    mega_df, power_df = fetch_all_data(limit=CFG["n_periods"], save_dir=CFG["data_dir"])
    log(f"✅ Mega rows: {len(mega_df)}, Power rows: {len(power_df)}")

    rf_path, gb_path, metrics = train_models_and_save(mega_df, power_df, window=CFG["window"], models_dir=CFG["models_dir"])
    if rf_path is None and not metrics:
        log("❌ Not enough data to train.")
    else:
        log(f"✅ Training completed | RF acc={metrics.get('acc_rf',0):.3f}")

    log("🎯 Predicting next numbers...")
    pred_mega, pred_power = ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6)
    log(f"🎲 Mega 6/45 predicted: {pred_mega}")
    log(f"💫 Power 6/55 predicted: {pred_power}")

    # save last prediction
    last = {
        "timestamp": __import__("datetime").datetime.now().isoformat(),
        "mega_pred": pred_mega,
        "power_pred": pred_power,
        "metrics": metrics
    }
    os.makedirs(CFG["data_dir"], exist_ok=True)
    last_json = os.path.join(CFG["data_dir"], "last_prediction.json")
    with open(last_json, "w") as f:
        json.dump(last, f)
    log(f"Saved last_prediction.json: {last}")

    # generate report (if possible)
    report_path = generate_report(mega_df, power_df, pred_mega, pred_power, save_dir=CFG["reports_dir"])
    if report_path and os.path.exists(report_path):
        body = f"Mega predicted: {pred_mega}\nPower predicted: {pred_power}\nMetrics: {metrics}"
        subj = os.getenv("EMAIL_SUBJECT", CFG.get("email_subject"))
        ok = send_email_with_attachment(subj, body, report_path)
        if ok:
            log("📧 Email sent successfully.")
        else:
            log("⚠️ Email sending failed.")
    else:
        # fallback: still send predictions (no attachment)
        log("⚠️ No report available — will send prediction email without attachment.")
        body = f"Mega predicted: {pred_mega}\nPower predicted: {pred_power}\nMetrics: {metrics}"
        subj = os.getenv("EMAIL_SUBJECT", CFG.get("email_subject"))
        ok = send_email_with_attachment(subj, body, attachment_path=None)
        if ok:
            log("📧 Fallback email (no attachment) sent successfully.")
        else:
            log("❌ Fallback email sending failed.")

    log("✅ Pipeline completed successfully.")

if __name__ == "__main__":
    run_pipeline()
