import os, sys, importlib, traceback
from pathlib import Path

# Xác định thư mục gốc của repository
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
print("Repository root:", ROOT)

print("\n=== Kiểm tra Files/Folders Top-level ===")
for p in sorted(ROOT.iterdir()):
    print(" ", p.name)

# Danh sách các file/folder bắt buộc
required = ["main.py", "config.py", "requirements.txt", "utils", "data", "models", "reports"]
print("\n=== Kiểm tra Sự hiện diện Bắt buộc ===")
for r in required:
    # Kiểm tra sự tồn tại, đồng thời tạo thư mục nếu là thư mục cấu hình
    path = ROOT / r
    if r in ["data", "models", "reports"]:
        path.mkdir(exist_ok=True)
    print(f" {r}: {'OK' if path.exists() else 'MISSING'}")

# Danh sách các file tiện ích bắt buộc
utils_dir = ROOT / "utils"
expected_utils = ["__init__.py", "logger.py", "fetch_data.py", "preprocess.py", "train_model.py", "report.py", "fetch_checks.py", "email_utils.py"]
print("\n=== Kiểm tra utils/ ===")
if utils_dir.exists():
    for f in expected_utils:
        print(f" {f}: {'OK' if (utils_dir / f).exists() else 'MISSING'}")
else:
    print(" utils/ folder is MISSING")

print("\n=== Kiểm tra Import nhanh ===")
# Thêm root vào sys.path để cho phép import các module
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

print("\n=== Kiểm tra Biến Môi trường (ENV) ===")
# Các biến môi trường cần thiết cho việc gửi email
envs = ["EMAIL_SENDER","EMAIL_PASSWORD","EMAIL_RECEIVER","GMAIL_USER","GMAIL_APP_PASSWORD"]
for e in envs:
    print(f" {e}: {'SET' if os.getenv(e) else 'NOT SET'}")

print("\n=== Chạy thử fetch_all_data (limit=5) ===")
try:
    mod = importlib.import_module("utils.fetch_data")
    if hasattr(mod, "fetch_all_data"):
        data_dir = ROOT / "data"
        os.makedirs(data_dir, exist_ok=True)
        # Thực thi fetch tối thiểu
        dfm, dfp = mod.fetch_all_data(limit=5, save_dir=str(data_dir))
        print(" -> số dòng được fetch:", (len(dfm) if dfm is not None else 0),(len(dfp) if dfp is not None else 0))
    else:
        print(" fetch_all_data không tìm thấy trong utils.fetch_data")
except Exception:
    print(" Lỗi trong quá trình chạy thử fetch:")
    traceback.print_exc(limit=2)

print("\n=== QUÉT HOÀN TẤT ===")
