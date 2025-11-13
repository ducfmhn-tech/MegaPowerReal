"""
preprocess: standardize date, ensure numeric, sort numbers in each row, remove invalid/dup
"""
import pandas as pd, os

def _sort_row_numbers(row):
    nums = [int(row[f"n{i}"]) for i in range(1,7)]
    nums_sorted = sorted(nums)
    for i,v in enumerate(nums_sorted, start=1):
        row[f"n{i}"] = v
    return row

def preprocess_dfs(mega_df, power_df, save_dir="data"):
    # ensure columns
    for df in (mega_df, power_df):
        for i in range(1,7):
            if f"n{i}" not in df.columns:
                df[f"n{i}"] = pd.NA
    # drop rows without date or missing numbers
    mega_df = mega_df.dropna(subset=["date"]).reset_index(drop=True)
    power_df = power_df.dropna(subset=["date"]).reset_index(drop=True)
    # enforce ints and sort numbers row-wise
    mega_df = mega_df.apply(_sort_row_numbers, axis=1)
    power_df = power_df.apply(_sort_row_numbers, axis=1)
    # remove duplicates by date
    mega_df = mega_df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"]).reset_index(drop=True)
    power_df = power_df.drop_duplicates(subset=["date","n1","n2","n3","n4","n5","n6"]).reset_index(drop=True)
    # save back
    os.makedirs(save_dir, exist_ok=True)
    mega_df.to_csv(os.path.join(save_dir, "mega_6_45_raw.csv"), index=False)
    power_df.to_csv(os.path.join(save_dir, "power_6_55_raw.csv"), index=False)
    return mega_df, power_df
