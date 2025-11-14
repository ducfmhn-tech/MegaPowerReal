# tools/repo_scan.py
import os, sys, importlib, pkgutil, traceback
from pathlib import Path
import subprocess, json

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
print("Repository root:", ROOT)

# 1. list files
print("\n=== Files / folders at repo root ===")
for p in sorted(ROOT.iterdir()):
    print(" ", p.name)

# 2. required files check
required = [
    "main.py", "config.py", "requirements.txt",
    "utils", "data"
]
print("\n=== Required presence ===")
for r in required:
    exists = (ROOT / r).exists()
    print(f" {r}: {'OK' if exists else 'MISSING'}")

# 3. check utils modules
utils_dir = ROOT / "utils"
expected_utils = ["fetch_data.py", "train_model.py", "email_utils.py", "report.py", "logger.py", "__init__.py"]
print("\n=== utils/* presence ===")
if utils_dir.exists():
    for f in expected_utils:
        print(f" {f}: {'OK' if (utils_dir / f).exists() else 'MISSING'}")
else:
    print(" utils/ missing")

# 4. check python imports quickly (attempt import)
print("\n=== Quick import test (will try to import key modules) ===")
sys.path.insert(0, str(ROOT))
modules = ["config", "utils.fetch_data", "utils.train_model", "utils.email_utils", "utils.report", "utils.logger"]
for m in modules:
    try:
        print(" import", m, "->", end=" ")
        importlib.import_module(m)
        print("OK")
    except Exception as e:
        print("FAIL")
        traceback.print_exc(limit=2)

# 5. check env secrets
print("\n=== Check ENV variables (for GH Actions) ===")
envs = ["EMAIL_SENDER","EMAIL_PASSWORD","EMAIL_RECEIVER","GMAIL_USER","GMAIL_APP_PASSWORD","RECEIVER_EMAIL"]
for e in envs:
    print(f" {e}: {'SET' if os.getenv(e) else 'NOT SET'}")

# 6. try to run a safe dry-run: fetch 1st page using fetch_data (no write)
try:
    print("\n=== Running fetch dry-run (utils.fetch_data.fetch_all_data limit=5) ===")
    mod = importlib.import_module("utils.fetch_data")
    f = getattr(mod, "fetch_all_data", None)
    if f:
        dfm, dfp = f(limit=5, save_dir=str(ROOT / "data"))
        print(" Fetch result sizes:", len(dfm) if dfm is not None else None, len(dfp) if dfp is not None else None)
    else:
        print(" fetch_all_data() not found in utils.fetch_data")
except Exception as e:
    print(" Exception during fetch dry-run:")
    traceback.print_exc(limit=2)

print("\n=== Done scan ===")
