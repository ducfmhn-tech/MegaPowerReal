# utils/predict.py
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.multioutput import MultiOutputClassifier

def build_features(df, window=50, max_num=55):
    """Return X (n_samples x max_num) and Y (n_samples x 6) or (None,None)."""
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

def train_multioutput_rf(X, Y, n_estimators=200, random_state=42):
    clf = MultiOutputClassifier(RandomForestClassifier(n_estimators=n_estimators, random_state=random_state, n_jobs=-1))
    clf.fit(X, Y)
    return clf

def predict_next(model, last_feat):
    pred = model.predict(last_feat.reshape(1,-1))[0].tolist()
    # remove duplicates while preserving order
    seen = set()
    unique = []
    for p in pred:
        if p not in seen:
            unique.append(int(p))
            seen.add(p)
    # fill if less than 6
    import random
    max_num = len(last_feat)
    pool = [i for i in range(1, max_num+1) if i not in unique]
    while len(unique) < 6:
        unique.append(random.choice(pool))
    return sorted(unique)
