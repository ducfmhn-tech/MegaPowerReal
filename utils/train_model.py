import os
import numpy as np
import joblib
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from utils.logger import log


# ------------------------------------------------------------
# Feature engineering (Mega/Power)
# ------------------------------------------------------------
def build_Xy(df, max_number):
    """
    Input df = rows with date + n1..n6
    Output:
        X: (N * max_number) x 4 features
        y: (N * max_number)
    """
    if df.empty:
        return None, None

    rows = []
    labels = []

    for _, row in df.iterrows():
        nums = set(int(row[f"n{i}"]) for i in range(1, 7))

        for n in range(1, max_number + 1):
            # -------------------------------
            # 4 features cho mỗi số n:
            #   1) normalized number
            #   2) number % 2
            #   3) number % 3
            #   4) number % 5
            # -------------------------------
            feat = [
                n / max_number,
                n % 2,
                n % 3,
                n % 5,
            ]
            rows.append(feat)
            labels.append(1 if n in nums else 0)

    X = np.array(rows)
    y = np.array(labels)
    return X, y


# ------------------------------------------------------------
# Train both models
# ------------------------------------------------------------
def train_models(df, model_dir, max_number):
    os.makedirs(model_dir, exist_ok=True)

    X, y = build_Xy(df, max_number)
    if X is None or len(X) == 0:
        return None, None

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, shuffle=True
    )

    # RF
    rf = RandomForestClassifier(n_estimators=120, random_state=42)
    rf.fit(X_train, y_train)
    acc_rf = rf.score(X_test, y_test)

    # GB
    gb = GradientBoostingClassifier()
    gb.fit(X_train, y_train)
    acc_gb = gb.score(X_test, y_test)

    # Save models
    rf_path = os.path.join(model_dir, f"rf_{max_number}.joblib")
    gb_path = os.path.join(model_dir, f"gb_{max_number}.joblib")

    joblib.dump(rf, rf_path)
    joblib.dump(gb, gb_path)

    log(f"✅ Models trained: RF={acc_rf:.3f}, GB={acc_gb:.3f}")

    return {
        "rf": rf,
        "gb": gb,
        "acc_rf": acc_rf,
        "acc_gb": acc_gb,
        "rf_path": rf_path,
        "gb_path": gb_path,
    }


# ------------------------------------------------------------
# Predict using ensemble
# ------------------------------------------------------------
def ensemble_predict_topk(df, max_number, rf_path, gb_path, topk=6):
    """
    df: latest df (not used directly, model predicts based only on features)
    max_number: Mega=45 / Power=55
    """

    # Build Xcur: các features cho every number
    Xcur = np.array([
        [n / max_number, n % 2, n % 3, n % 5]
        for n in range(1, max_number + 1)
    ])

    # Load models safely
    rf = joblib.load(rf_path) if rf_path and os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if gb_path and os.path.exists(gb_path) else None

    # RF predict
    if rf:
        try:
            p_rf = rf.predict_proba(Xcur)[:, 1]
        except:
            p_rf = np.zeros(max_number)
    else:
        p_rf = np.zeros(max_number)

    # GB predict
    if gb:
        try:
            p_gb = gb.predict_proba(Xcur)[:, 1]
        except:
            p_gb = np.zeros(max_number)
    else:
        p_gb = np.zeros(max_number)

    # Ensemble
    scores = (p_rf + p_gb) / 2
    best = np.argsort(scores)[-topk:] + 1
    best = sorted(best.tolist())

    return best
