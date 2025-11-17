# utils/stats.py
import pandas as pd
from collections import Counter

def frequency_stats(df):
    """Return DataFrame with columns 'number','frequency' sorted desc."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["number","frequency"])
    nums = df[[f"n{i}" for i in range(1,7)]].values.flatten()
    nums = [int(n) for n in nums if pd.notna(n)]
    cnt = Counter(nums)
    freq = pd.DataFrame(cnt.items(), columns=["number","frequency"])
    freq = freq.sort_values("frequency", ascending=False).reset_index(drop=True)
    return freq

def pair_frequency_stats(df):
    """Return DataFrame of pairs (num1,num2,frequency)."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["num1","num2","frequency"])
    pairs = []
    for _, row in df.iterrows():
        nums = sorted([int(row[f"n{i}"]) for i in range(1,7)])
        for i in range(6):
            for j in range(i+1, 6):
                pairs.append((nums[i], nums[j]))
    cnt = Counter(pairs)
    df_pairs = pd.DataFrame([(a,b,c) for (a,b),c in cnt.items()], columns=["num1","num2","frequency"])
    df_pairs = df_pairs.sort_values("frequency", ascending=False).reset_index(drop=True)
    return df_pairs

def repeat_stats(df):
    """Return sorted list of numbers that repeat from last draw to previous draw."""
    if df is None or len(df) < 2:
        return []
    df_sorted = df.sort_values("draw_date", ascending=False).reset_index(drop=True)
    latest = set(int(x) for x in df_sorted.loc[0, [f"n{i}" for i in range(1,7)]].values)
    prev = set(int(x) for x in df_sorted.loc[1, [f"n{i}" for i in range(1,7)]].values)
    return sorted(list(latest & prev))
