"""
train_model.py
- Build dataset X,y per-number
- Train RandomForest + GradientBoosting (XGBoost)
- Save models
- Provide ensemble predict top-k
"""

import os, joblib, numpy as np, pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from xgboost import XGBClassifier


# ==========================================================
# 1️⃣ Build feature-label dataset
# ==========================================================
def build_Xy(mega_df, power_df, window=50, max_num=45):
    """
    X features: for each number 1..45,
      [mega_count, power_count, mega_pct, power_pct]
    y label: 1 if number appears in next draw (t+1), else 0
    """
    if mega_df.empty or power_df.empty:
        print("⚠️ Empty DataFrame(s) → cannot build X,y")
        return None, None

    X, y = [], []
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        print(f"⚠️ Not enough rows (need >{window}, got {minlen})")
        return None, None

    for end in range(window, minlen):
        mw = mega_df.iloc[end - window:end]
        pw = power_df.iloc[end - window:end]

        m_counts = []
        p_counts = []
        for n in range(1, max_num + 1):
            m_counts.append(sum(mw[f"n{i}"].isin([n]).sum() for i in range(1, 7)))
            p_counts.append(sum(pw[f"n{i}"].isin([n]).sum() for i in range(1, 7)))

        m_counts = np.array(m_counts)
        p_counts = np.array(p_counts)

        try:
            next_draw = [int(mega_df.iloc[end][f"n{i}"]) for i in range(1, 7)]
        except Exception:
            next_draw = []

        for n in range(1, max_num + 1):
            feats = [
                int(m_counts[n - 1]),
                int(p_counts[n - 1]),
                float(m_counts[n - 1] / (window * 6)),
                float(p_counts[n - 1] / (window * 6)),
            ]
            X.append(feats)
            y.append(1 if n in next_draw else 0)

    return np.array(X), np.array(y)


# ==========================================================
# 2️⃣ Train & Save models
# ==========================================================
def train_models_and_save(mega_df, power_df, window=50, save_dir="data", models_dir="models"):
    os.makedirs(models_dir, exist_ok=True)

    X, y = build_Xy(mega_df, power_df, window=window)
    if X is None or len(X) == 0:
        print("⚠️ Not enough data to train — creating placeholder models")
        # Save dummy models (so pipeline continues)
        rf_path = os.path.join(models_dir, "rf_pernum_mega.joblib")
        gb_path = os.path.join(models_dir, "gb_pernum_mega.joblib")
        joblib.dump(None, rf_path)
        joblib.dump(None, gb_path)
        return rf_path, gb_path, {"acc_rf": 0.0, "acc_gb": 0.0}

    # Train-test split
    Xtr, Xval, ytr, yval = train_test_split(X, y, test_size=0.2, random_state=42)
    rf = RandomForestClassifier(n_estimators=300, random_state=42, class_weight="balanced")
    rf.fit(Xtr, ytr)
    rf_path = os.path.join(models_dir, "rf_pernum_mega.joblib")
    joblib.dump(rf, rf_path)

    gb = XGBClassifier(
        n_estimators=200, use_label_encoder=False, eval_metric="logloss", verbosity=0
    )
    gb.fit(Xtr, ytr)
    gb_path = os.path.join(models_dir, "gb_pernum_mega.joblib")
    joblib.dump(gb, gb_path)

    ypred_rf = rf.predict(Xval)
    ypred_gb = gb.predict(Xval)
    acc_rf = accuracy_score(yval, ypred_rf)
    acc_gb = accuracy_score(yval, ypred_gb)

    print(f"✅ Models trained: RF={acc_rf:.3f}, GB={acc_gb:.3f}")
    return rf_path, gb_path, {"acc_rf": acc_rf, "acc_gb": acc_gb}


# ==========================================================
# 3️⃣ Ensemble predict top-k
# ==========================================================
def ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6, save_dir="data"):
    max_num = 45
    window = min(50, len(mega_df))

    if mega_df.empty or power_df.empty:
        print("⚠️ Empty data → cannot predict")
        return [], [], {}

    # Build feature for each number
    mw = mega_df.tail(window)
    pw = power_df.tail(window)
    m_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1, 7)) for n in range(1, max_num + 1)]
    p_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1, 7)) for n in range(1, max_num + 1)]
    Xcur = np.array(
        [
            [int(m_counts[n - 1]), int(p_counts[n - 1]),
             float(m_counts[n - 1] / (window * 6)), float(p_counts[n - 1] / (window * 6))]
            for n in range(1, max_num + 1)
        ]
    )

    rf = joblib.load(rf_path) if os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if os.path.exists(gb_path) else None

    probs_rf = rf.predict_proba(Xcur)[:, 1] if rf else np.zeros(len(Xcur))
    probs_gb = gb.predict_proba(Xcur)[:, 1] if gb else np.zeros(len(Xcur))
    probs = (probs_rf + probs_gb) / 2.0

    idxs = probs.argsort()[-topk:][::-1]
    pred_nums = sorted([int(i + 1) for i in idxs])

    # For Power: similar pattern but up to 55
    max_p = 55
    pp_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1, 7)) for n in range(1, max_p + 1)]
    sp = np.array(pp_counts)
    idxs_p = sp.argsort()[-topk:][::-1]
    pred_power = sorted([int(i + 1) for i in idxs_p])

    probs_dict = {"mega_probs": probs.tolist(), "power_scores": sp.tolist()}

    print("🎯 Predicted Mega:", pred_nums)
    print("🎯 Predicted Power:", pred_power)
    return pred_nums, pred_power, probs_dict
