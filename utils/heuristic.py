# utils/heuristic.py
import random

def heuristic_predict(freq_df, k=6, max_num=45):
    """Simple heuristic: choose top-k by frequency; if not enough, fill random."""
    if freq_df is None or freq_df.empty:
        return sorted(random.sample(range(1, max_num+1), k))
    top = list(freq_df.head(k)["number"].astype(int).tolist())
    if len(top) < k:
        pool = [n for n in range(1, max_num+1) if n not in top]
        while len(top) < k:
            top.append(random.choice(pool))
    return sorted(top)
