# train_tf_model.py
import os
import numpy as np
import pandas as pd
from pathlib import Path
from utils.fetch_data import fetch_all_sources
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.optimizers import Adam
from sklearn.model_selection import train_test_split

MODEL_DIR = Path("models")
METRICS_DIR = Path("metrics")
MODEL_DIR.mkdir(parents=True, exist_ok=True)
METRICS_DIR.mkdir(parents=True, exist_ok=True)

def build_features(df, window=50, max_num=45):
    df = df.sort_values("draw_date").reset_index(drop=True)
    if len(df) <= window:
        return None, None
    X, Y = [], []
    for i in range(window, len(df)):
        prev = df.iloc[i-window:i]
        counts = np.zeros(max_num)
        for c in range(1,7):
            for v in prev[f"n{c}"].dropna().astype(int).tolist():
                counts[v-1] += 1
        X.append(counts)
        Y.append(df.loc[i, [f"n{j}" for j in range(1,7)]].astype(int).values)
    return np.array(X), np.array(Y)

def build_tf_model(input_dim, output_dim=45):
    from tensorflow.keras import Sequential
    from tensorflow.keras.layers import Dense, Dropout
    model = Sequential([
        Dense(256, activation="relu", input_shape=(input_dim,)),
        Dropout(0.2),
        Dense(128, activation="relu"),
        Dropout(0.2),
        Dense(output_dim, activation="linear")
    ])
    model.compile(optimizer=Adam(1e-3), loss="mse")
    return model

if __name__ == "__main__":
    urls = [
        "https://www.ketquadientoan.com/tat-ca-ky-xo-so-mega-6-45.html",
        "https://www.minhngoc.net.vn/ket-qua-xo-so/dien-toan-vietlott/mega-6x45.html",
        "https://www.lotto-8.com/Vietnam/listltoVM45.asp",
    ]
    df = fetch_all_sources(urls, limit=400)
    X, Y = build_features(df, window=50, max_num=45)
    if X is None:
        print("Not enough data")
        exit(1)

    # multi-hot target
    Y_multi = np.zeros((Y.shape[0], 45), dtype=int)
    for i in range(Y.shape[0]):
        for v in Y[i]:
            Y_multi[i, int(v)-1] = 1

    X_train, X_val, Y_train, Y_val = train_test_split(X, Y_multi, test_size=0.2, random_state=42)

    model = build_tf_model(X.shape[1], output_dim=45)
    model.fit(X_train, Y_train, epochs=50, batch_size=32, validation_data=(X_val, Y_val))

    # predict top-6
    preds = model.predict(X_val)
    top_preds = []
    for row in preds:
        idxs = np.argsort(row)[::-1][:6] + 1
        top_preds.append(sorted(idxs.tolist()))

    truth = []
    for row in Y_val:
        idxs = np.where(row==1)[0] + 1
        truth.append(sorted(idxs.tolist()))

    exact = float(np.mean([set(a)==set(b) for a,b in zip(top_preds, truth)]))
    pd.DataFrame([{"exact_match": exact}]).to_csv(METRICS_DIR / "tf_mega_metrics.csv", index=False)
    model.save(MODEL_DIR / "mega_tf_model.h5")
    print("TF model saved and metrics written.")
