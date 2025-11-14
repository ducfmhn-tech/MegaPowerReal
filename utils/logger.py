import datetime

def log(msg):
    """In ra log có timestamp, đồng thời flush để GitHub Actions hiển thị real-time."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)
