"""
main.py — Entry point for MegaPowerReal
--------------------------------------
- Fetch historical data (50–100 draws)
- Train + Predict
- Compare predictions vs real results (if new data)
- Save reports (.xlsx + JSON + logs)
- Send email results
"""

import os, json, pandas as pd
from datetime import datetime
from utils.fetch_data import fetch_all_data
from utils.train_model import train_models_and_save, ensemble_predict_topk
from utils.send_email import send_report_email

# ================================
# CONFIG
# ================================
CFG = {
    "n_periods": 100,
    "save_dir": "data",
    "models_dir": "models",
    "reports_dir": "reports",
    "email_sender": "asusgo202122@gmail.com",
    "email_receiver": "ducfm.hn@gmail.com",
    "email_pass": os.getenv("EMAIL_APP_PASS", "YOUR_APP_PASSWORD"),  # GitHub Secret
}

os.makedirs(CFG["save_dir"], exist_ok=True)
os.makedirs(CFG["models_dir"], exist_ok=True)
os.makedirs(CFG["reports_dir"], exist_ok=True)


# ================================
# 1️⃣  LOAD OR INIT LAST PREDICTION
# ================================
LAST_JSON = os.path.join(CFG["save_dir"], "last_prediction.json")
if os.path.exists(LAST_JSON):
    with open(LAST_JSON, "r", encoding="utf-8") as f:
        last_pred = json.load(f)
else:
    last_pred = {
        "date": None,
        "pred_mega": [],
        "pred_power": [],
        "accuracy_mega": None,
        "accuracy_power": None,
    }


# ================================
# 2️⃣  PIPELINE
# ================================
def run_pipeline():
    print("🔹 Fetching Mega 6/45...")
    mega_df, power_df = fetch_all_data(limit=CFG["n_periods"], save_dir=CFG["save_dir"])

    # If fetch failed
    if mega_df.empty or power_df.empty:
        print("⚠️ No data fetched → skip training")
        return

    print(f"✅ Got Mega={len(mega_df)}, Power={len(power_df)} rows")

    # Train (or retrain)
    rf_path, gb_path, metrics = train_models_and_save(
        mega_df, power_df, window=50,
        save_dir=CFG["save_dir"], models_dir=CFG["models_dir"]
    )

    # Predict
    pred_mega, pred_power, probs = ensemble_predict_topk(
        mega_df, power_df, rf_path, gb_path, topk=6
    )

    # Save new prediction
    today = datetime.now().strftime("%Y-%m-%d")
    new_pred = {
        "date": today,
        "pred_mega": pred_mega,
        "pred_power": pred_power,
        "metrics": metrics,
    }

    with open(LAST_JSON, "w", encoding="utf-8") as f:
        json.dump(new_pred, f, ensure_ascii=False, indent=2)

    print(f"💾 Saved last_prediction.json")

    # ===============================
    # 3️⃣ Compare with new real result
    # ===============================
    try:
        latest_real_mega = list(map(int, mega_df.tail(1)[[f"n{i}" for i in range(1, 7)]].values.flatten()))
        latest_real_power = list(map(int, power_df.tail(1)[[f"n{i}" for i in range(1, 7)]].values.flatten()))
        latest_date = mega_df.tail(1)["date"].values[0]
        
        if last_pred["date"] and last_pred["date"] != latest_date:
            acc_mega = len(set(last_pred["pred_mega"]) & set(latest_real_mega)) / 6
            acc_power = len(set(last_pred["pred_power"]) & set(latest_real_power)) / 6
            print(f"📈 Accuracy Mega={acc_mega:.2%}, Power={acc_power:.2%}")
            new_pred["accuracy_mega"] = acc_mega
            new_pred["accuracy_power"] = acc_power
        else:
            print("⚠️ No new draw found or same date → skip accuracy check")

    except Exception as e:
        print("⚠️ Error comparing with real results:", e)

    # ===============================
    # 4️⃣ Save report
    # ===============================
    report_path = os.path.join(CFG["reports_dir"], f"mega_power_report_{today}.xlsx")
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        mega_df.to_excel(writer, sheet_name="Mega_6_45", index=False)
        power_df.to_excel(writer, sheet_name="Power_6_55", index=False)
        pd.DataFrame([new_pred]).to_excel(writer, sheet_name="Prediction", index=False)
    print(f"📊 Report saved → {report_path}")

    # ===============================
    # 5️⃣ Send email
    # ===============================
    subject = f"Mega-Power Report {today}"
    body = f"""
📅 Date: {today}
🎯 Mega prediction: {pred_mega}
🎯 Power prediction: {pred_power}
📈 RF acc: {metrics.get('acc_rf',0):.3f}, GB acc: {metrics.get('acc_gb',0):.3f}
"""
    send_report_email(
        sender=CFG["email_sender"],
        password=CFG["email_pass"],
        receiver=CFG["email_receiver"],
        subject=subject,
        body=body,
        attachment_path=report_path,
    )

    print("✅ Email sent successfully!")


# ================================
# ENTRY POINT
# ================================
if __name__ == "__main__":
    run_pipeline()
