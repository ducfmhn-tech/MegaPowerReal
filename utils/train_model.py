# utils/train_model.py
import os, joblib, numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from utils.logger import log
from config import CFG

def build_Xy(mega_df, power_df, window=50, max_num=45):
    if mega_df is None or power_df is None:
        return None, None
    minlen = min(len(mega_df), len(power_df))
    if minlen <= window:
        return None, None
    X=[]; y=[]
    for end in range(window, minlen):
        mw = mega_df.iloc[end-window:end].reset_index(drop=True)
        pw = power_df.iloc[end-window:end].reset_index(drop=True)
        # counts for 1..max_num
        m_counts = []
        p_counts = []
        for n in range(1, max_num+1):
            mcount = sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7))
            pcount = sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7))
            m_counts.append(mcount); p_counts.append(pcount)
        # next draw labels
        try:
            next_draw = [int(mega_df.iloc[end][f"n{i}"]) for i in range(1,7)]
        except:
            next_draw = []
        for i,n in enumerate(range(1,max_num+1)):
            feats = [int(m_counts[i]), int(p_counts[i]), float(m_counts[i]/(window*6)), float(p_counts[i]/(window*6))]
            X.append(feats)
            y.append(1 if n in next_draw else 0)
    return np.array(X), np.array(y)

def train_models_and_save(mega_df, power_df, window=None, models_dir=None):
    if window is None:
        window = CFG["window"]
    if models_dir is None:
        models_dir = CFG["models_dir"]
    os.makedirs(models_dir, exist_ok=True)
    X,y = build_Xy(mega_df, power_df, window=window)
    if X is None:
        log("❌ Not enough data to train.")
        return None, None, {}
    Xtr, Xval, ytr, yval = train_test_split(X,y,test_size=0.2, random_state=42)
    rf = RandomForestClassifier(n_estimators=200, random_state=42, class_weight="balanced")
    rf.fit(Xtr, ytr)
    rf_path = os.path.join(models_dir, "rf_pernum_mega.joblib")
    joblib.dump(rf, rf_path)
    # optional: simple GB (skip heavy xgboost to reduce install issues)
    # metrics
    acc_rf = rf.score(Xval, yval)
    metrics = {"acc_rf": float(acc_rf)}
    log(f"✅ Models trained: RF={acc_rf:.3f}")
    return rf_path, None, metrics

def ensemble_predict_topk(mega_df, power_df, rf_path=None, gb_path=None, topk=6):
    max_num=45
    window = CFG["window"]
    mw = mega_df.tail(window)
    pw = power_df.tail(window)
    m_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_num+1)]
    p_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, 56)]
    # try load rf
    probs = None
    if rf_path and os.path.exists(rf_path):
        rf = joblib.load(rf_path)
        Xcur = []
        for i in range(max_num):
            Xcur.append([int(m_counts[i]), int(p_counts[i] if i < len(p_counts) else 0),
                         float(m_counts[i]/(window*6)), float((p_counts[i] if i < len(p_counts) else 0)/(window*6))])
        import numpy as _np
        Xcur = _np.array(Xcur)
        try:
            probs = rf.predict_proba(Xcur)[:,1]
        except:
            probs = None
    if probs is None:
        # fallback to frequency
        import numpy as _np
        probs = _np.array(m_counts, dtype=float)
    import numpy as _np
    idxs = probs.argsort()[-topk:][::-1]
    pred_nums = sorted([int(i+1) for i in idxs])
    # power: pick by p_counts
    idxs_p = _np.array(p_counts).argsort()[-topk:][::-1]
    pred_power = sorted([int(i+1) for i in idxs_p])
    return pred_nums, pred_power

def ensemble_predict_topk(mega_df, power_df, rf_path, gb_path, topk=6):
    import numpy as _np
    import joblib

    window = 50

    # ==========================
    # 1) MEGA features (1..45)
    # ==========================
    max_mega = 45
    mw = mega_df.tail(window)
    pw = power_df.tail(window)

    m_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_mega+1)]
    p_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_mega+1)]

    Xcur = []
    for idx in range(max_mega):
        mc = m_counts[idx]
        pc = p_counts[idx]
        Xcur.append([
            mc,
            pc,
            mc/(window*6),
            pc/(window*6)
        ])
    Xcur = _np.array(Xcur)

    rf = joblib.load(rf_path) if os.path.exists(rf_path) else None
    gb = joblib.load(gb_path) if os.path.exists(gb_path) else None

    probs_rf = rf.predict_proba(Xcur)[:, 1] if rf else _np.zeros(max_mega)
    probs_gb = gb.predict_proba(Xcur)[:, 1] if gb else _np.zeros(max_mega)

    probs = (probs_rf + probs_gb) / 2
    idxs = probs.argsort()[-topk:][::-1]
    pred_mega = sorted([int(i+1) for i in idxs])

    # ==========================
    # 2) POWER features (1..55)
    # ==========================
    max_power = 55
    mp_counts = [sum(mw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_power+1)]
    pp_counts = [sum(pw[f"n{i}"].isin([n]).sum() for i in range(1,7)) for n in range(1, max_power+1)]

    Xcur_p = []
    for idx in range(max_power):
        mc = mp_counts[idx] if idx < 45 else 0       # Mega max 45
        pc = pp_counts[idx]
        Xcur_p.append([
            mc,
            pc,
            mc/(window*6),
            pc/(window*6)
        ])
    Xcur_p = _np.array(Xcur_p)

    # POWER = dùng GB nếu có, nếu không fallback theo tần suất
    if gb:
        probs_p = gb.predict_proba(Xcur_p)[:, 1]
        idxs_p = probs_p.argsort()[-topk:][::-1]
        pred_power = sorted([int(i+1) for i in idxs_p])
    else:
        # fallback tần suất
        score = [pp_counts[i] + 0.3 * (mp_counts[i] if i < 45 else 0) for i in range(max_power)]
        idxs_p = _np.array(score).argsort()[-topk:][::-1]
        pred_power = sorted([int(i+1) for i in idxs_p])

    return pred_mega, pred_power
