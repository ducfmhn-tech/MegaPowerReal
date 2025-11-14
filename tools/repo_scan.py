# tools/repo_scan.py
import os, sys, importlib, traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
print("Repository root:", ROOT)

print("\n=== Top-level files/folders ===")
for p in sorted(ROOT.iterdir()):
    print(" ", p.name)

required = ["main.py", "config.py", "requirements.txt", "utils"]
print("\n=== Required presence ===")
for r in required:
    print(f" {r}: {'OK' if (ROOT / r).exists() else 'MISSING'}")

utils_dir = ROOT / "utils"
expected_utils = ["__init__.py", "logger.py", "fetch_data.py", "train_model.py", "report.py", "email_utils.py"]
print("\n=== utils/ presence ===")
if utils_dir.exists():
    for f in expected_utils:
        print(f" {f}: {'OK' if (utils_dir / f).exists() else 'MISSING'}")
else:
    print(" utils/ folder is MISSING")

print("\n=== Quick import test ===")
sys.path.insert(0, str(ROOT))
modules = ["config", "utils.logger", "utils.fetch_data", "utils.train_model", "utils.report", "utils.email_utils"]
for m in modules:
    try:
        print(" import", m, "->", end=" ")
        importlib.import_module(m)
        print("OK")
    except Exception:
        print("FAIL")
        traceback.print_exc(limit=2)

print("\n=== ENV checks ===")
envs = ["EMAIL_SENDER","EMAIL_PASSWORD","EMAIL_RECEIVER","GMAIL_USER","GMAIL_APP_PASSWORD"]
for e in envs:
    print(f" {e}: {'SET' if os.getenv(e) else 'NOT SET'}")

print("\n=== fetch_all_data dry-run (limit=5) ===")
try:
    mod = importlib.import_module("utils.fetch_data")
    if hasattr(mod, "fetch_all_data"):
        dfm, dfp = mod.fetch_all_data(limit=5, save_dir=str(ROOT / "data"))
        print(" -> fetched rows:", (len(dfm) if dfm is not None else None),(len(dfp) if dfp is not None else None))
    else:
        print(" fetch_all_data not found in utils.fetch_data")
except Exception:
    print(" Exception during fetch dry-run:")
    traceback.print_exc(limit=2)

print("\n=== SCAN DONE ===")
