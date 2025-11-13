"""
MegaPowerReal - main entry
"""
import os, json
from utils.fetch_data import fetch_all_data
from utils.preprocess import preprocess_dfs
from utils.features import build_features_for_all
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.error_analysis import check_and_retrain_if_needed
from utils.report import save_report_xlsx
from utils.email_sender import send_email_smtp

# load config
with open("config.json", "r", encoding="utf-8") as f:
    CFG = json.load(f)

SAVE_DIR = CFG.get("save_dir", "data")
MODELS_DIR = CFG.get("models_dir", "models")
REPORTS_DIR = CFG.get("reports_dir", "outputs")
os.makedirs(SAVE_DIR, exist_ok=True)
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

def run_pipeline():
    # 1. fetch raw
    mega_df, power_df = fetch_all_data(limit=CFG.get("n_periods",100), save_dir=SAVE_DIR)

    # 2. preprocess
    mega_df, power_df = preprocess_dfs(mega_df, power_df, save_dir=SAVE_DIR)

    # 3. feature engineering (window-based & lunar/ngũ hành)
    feat_meta = build_features_for_all(mega_df, power_df, window=CFG.get("window",50), save_dir=SAVE_DIR)

    # 4. Train RF + GB and save models
    rf_model_path, gb_model_path, metrics = train_models_and_save(
        mega_df, power_df,
        window=CFG.get("window",50),
        save_dir=SAVE_DIR, models_dir=MODELS_DIR
    )

    # 5. Ensemble predict top-6 for Mega and Power
    pred_mega, pred_power, probs = ensemble_predict_topk(
        mega_df, power_df, rf_model_path, gb_model_path,
        topk=6, save_dir=SAVE_DIR
    )

    # persist last_prediction.json
    last_pred = {
        "timestamp": __import__("datetime").datetime.now().isoformat(),
        "Mega": pred_mega,
        "Power": pred_power,
        "probs": probs
    }
    with open(os.path.join(SAVE_DIR, "last_prediction.json"), "w", encoding="utf-8") as f:
        json.dump(last_pred, f, ensure_ascii=False, indent=2)

    # 6. When there's a new actual draw (check inside function), compare & retrain if needed
    retrain_info = check_and_retrain_if_needed(
        save_dir=SAVE_DIR, models_dir=MODELS_DIR, config=CFG
    )

    # 7. Save report
    report_path = save_report_xlsx(
        save_dir=SAVE_DIR, reports_dir=REPORTS_DIR,
        mega_df=mega_df, power_df=power_df,
        pred_mega=pred_mega, pred_power=pred_power,
        metrics=metrics, retrain_info=retrain_info
    )

    # 8. Send email (reads env secrets)
    subject = f"[MegaPowerReal] Report {__import__('datetime').datetime.now().date()}"
    body = f"Predicted Mega: {pred_mega}\nPredicted Power: {pred_power}\nReport: {report_path}"
    send_email_smtp(subject, body, attachment_path=report_path)

    print("Pipeline finished. Report:", report_path)
    return report_path

if __name__ == "__main__":
    run_pipeline()
