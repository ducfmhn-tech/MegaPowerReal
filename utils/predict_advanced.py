# utils/predict_advanced.py
import os
import joblib
import numpy as np
import pandas as pd
from collections import Counter
from sklearn.multioutput import MultiOutputClassifier

# optional libs
HAS_LGB = False
HAS_CAT = False
try:
    from lightgbm import LGBMClassifier
    HAS_LGB = True
except Exception:
    HAS_LGB = False
try:
    from catboost import CatBoostClassifier
    HAS_CAT = True
except Exception:
    HAS_CAT = False

def build_count_features(df, window=50, max_num=55):
    df = df.sort_values("draw_date").reset_index(drop=True)
    if len(df) <= window:
        return None, None
    X, Y = [], []
    for i in range(window, len(df)):
        prev = df.iloc[i-window:i]
        counts = np.zeros(max_num, dtype=int)
        for c in range(1,7):
            for v in prev[f"n{c}"].dropna().astype(int).tolist():
                if 1 <= v <= max_num:
                    counts[v-1] += 1
        X.append(counts)
        Y.append(df.loc[i, [f"n{j}" for j in range(1,7)]].astype(int).values)
    return np.vstack(X), np.vstack(Y)

def train_lightgbm(X, Y, n_estimators=100, random_state=42):
    if not HAS_LGB:
        raise RuntimeError("LightGBM not available")
    base = LGBMClassifier(n_estimators=n_estimators, random_state=random_state, n_jobs=-1)
    moc = MultiOutputClassifier(base, n_jobs=1)
    moc.fit(X, Y)
    return moc

def train_catboost(X, Y, iterations=100, random_state=42, verbose=False):
    if not HAS_CAT:
        raise RuntimeError("CatBoost not available")
    base = CatBoostClassifier(iterations=iterations, random_state=random_state, verbose=verbose, task_type="CPU")
    moc = MultiOutputClassifier(base)
    moc.fit(X, Y)
    return moc

def train_mlp(X, Y, hidden_layer_sizes=(128,64), max_iter=200, random_state=42):
    from sklearn.neural_network import MLPClassifier
    base = MLPClassifier(hidden_layer_sizes=hidden_layer_sizes, max_iter=max_iter, random_state=random_state)
    moc = MultiOutputClassifier(base)
    moc.fit(X, Y)
    return moc

def ensemble_predict(models, X_last, max_num=55):
    """Ensemble via majority vote per position, then fix duplicates."""
    preds_per_model = []
    for m in models:
        if m is None:
            preds_per_model.append(None)
            continue
        try:
            p = m.predict(X_last.reshape(1,-1))[0].tolist()
            preds_per_model.append([int(x) for x in p])
        except Exception:
            preds_per_model.append(None)

    final = []
    for pos in range(6):
        votes = [p[pos] for p in preds_per_model if p is not None]
        if not votes:
            final.append(None)
            continue
        c = Counter(votes)
        final.append(c.most_common(1)[0][0])

    # fill None using overall vote frequency
    all_votes = []
    for p in preds_per_model:
        if p:
            all_votes.extend(p)
    freq_order = [x for x,_ in Counter(all_votes).most_common()]

    used = set([v for v in final if v])
    for i,v in enumerate(final):
      if v is None:
            for cand in freq_order:
                if cand not in used:
                    final[i] = cand
                    used.add(cand)
                    break
            else:
                for cand in range(1, max_num+1):
                    if cand not in used:
                        final[i] = cand
                        used.add(cand)
                        break

    # enforce uniqueness
    uniq = []
    pool = [n for n in range(1, max_num+1) if n not in final]
    for i,v in enumerate(final):
        if v in uniq:
            final[i] = pool.pop(0)
        uniq.append(final[i])

    return sorted(final)

def save_model(obj, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    joblib.dump(obj, path)

def load_model(path):
    return joblib.load(path)
