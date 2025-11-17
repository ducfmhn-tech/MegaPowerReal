# utils/logger.py
import datetime,sys
def log(msg):
    t = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{t}] {msg}")
    sys.stdout.flush()
