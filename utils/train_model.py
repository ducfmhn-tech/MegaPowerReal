import os
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from utils.logger import log


# ---------------------------------------------------------
# FEATURE ENGINEERING
# ---------------------------------------------------------
def build_feature_matrix(df, max_number):
    """
    Build simple frequency–based features for ML models.
    Each number is treated as a sample with features:
      - recent frequency
      - long frequency
    """
    df = df.copy()

    # flatten all drawn numbers
    nums = df[[f"n{i}" for i in range(1, 7)]].values.flatten()

    # frequency
    freq = pd.Series(nums).value_counts()

    X = []
    y = []

    for n in range(1, max_number + 1):
        recent = (nums[-60:] == n).sum() if len(nums) >= 60 else (nums == n).sum()
        total = freq.get(n, 0)

        X.append([recent, total])

    # label: last draw acts as "future"
    last = df.iloc[-1][[f"n{i}" for i in range(1, 7)]].values
    last = set(map(int, last))

    for n in range(1, max_number + 1):
        y.append(1 if n in last else 0)

    return np.array(X), np.array(y)


# ---------------------------------------------------------
# TRAIN MODELS & SAVE
# ---------------------------------------------------------
def train_models_and_save(mega_df, power_df, rf_path, gb_path):
    """Train RF + GB models for Mega (1..45) and Power (1..55)."""

    metrics = {}

    # ------------------ MEGA ------------------
    X, y = build_feature_matrix(mega_df, 45)

    rf = RandomForestClassifier(n_estimators=200, random_state=42)
    gb = GradientBoostingClassifier(random_state=42)

    rf.fit(X, y)
    gb.fit(X, y)

    # save
    joblib.dump(rf, rf_path)
    joblib.dump(gb, gb_path)

    metrics["acc_rf"] = rf.score(X, y)
    metrics["acc_gb"] = gb.score(X, y)

    return metrics


# ---------------------------------------------------------
# PREDICT NEXT DRAW
# ---------------------------------------------------------
def ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6):
    """Predict next Mega & Power using simple model ensemble."""

    # ------------------ MEGA ------------------
    Xmega, _ = build_feature_matrix(mega_df, 45)

    rf = joblib.load(rf_path)
    gb = joblib.load(gb_path)

    p_rf = rf.predict_proba(Xmega)[:, 1]
    p_gb = gb.predict_proba(Xmega)[:, 1]

    p_mean = (p_rf + p_gb) / 2
    idx = np.argsort(p_mean)[-topk:][::-1]
    pred_mega = sorted([int(i + 1) for i in idx])

    # ------------------ POWER ------------------
    if len(power_df) < 10:
        pred_power = []
    else:
        Xpw, _ = build_feature_matrix(power_df, 55)

        # reuse same RF & GB model (lightweight approach)
        pp_rf = rf.predict_proba(Xpw)[:, 1]
        pp_gb = gb.predict_proba(Xpw)[:, 1]
        p2 = (pp_rf + pp_gb) / 2

        idx2 = np.argsort(p2)[-topk:][::-1]
        pred_power = sorted([int(i + 1) for i in idx2])
    return pred_mega, pred_power


__all__ = ["train_models_and_save", "ensemble_predict_topk"]
