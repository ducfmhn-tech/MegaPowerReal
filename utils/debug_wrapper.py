# utils/debug_wrapper.py
import os
import re
from datetime import datetime
import requests
import pandas as pd

os.makedirs("data", exist_ok=True)

def _safe(s):
    return re.sub(r"[^A-Za-z0-9._-]", "_", s)

def save_debug_html(html, tag="unknown", limit=5_000_000):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    fname = f"debug_{_safe(tag)}_{ts}.html"
    path = os.path.join("data", fname)

    # shorten if too big
    if isinstance(html, str):
        data = html
    else:
        data = str(html)

    if len(data.encode("utf-8")) > limit:
        head = data[:4_000_000]
        tail = data[-500_000:]
        data = head + "\n\n<!--TRUNCATED-->\n\n" + tail

    with open(path, "w", encoding="utf-8", errors="ignore") as f:
        f.write(data)

    print(f"📁 Saved debug HTML → {path}")


# ========== PATCH requests.get ==========

_original_get = requests.get

def patched_get(url, *args, **kwargs):
    try:
        resp = _original_get(url, *args, **kwargs)
        # Always save debug file for each URL
        save_debug_html(resp.text, tag=url)
        return resp
    except Exception as e:
        # Also save even when error
        try:
            if hasattr(e, "response") and getattr(e.response, "text", None):
                save_debug_html(e.response.text, tag=url)
        except:
            pass
        raise e

requests.get = patched_get


# ========== PATCH pandas.read_html ==========

_original_read_html = pd.read_html

def patched_read_html(*args, **kwargs):
    try:
        result = _original_read_html(*args, **kwargs)
        return result
    except Exception as e:
        # Save the HTML that caused the failure if available
        if args:
            html = args[0]
            if isinstance(html, str):
                save_debug_html(html, tag="read_html_error")
        raise e

pd.read_html = patched_read_html

print("🔧 Debug wrapper active: requests.get and read_html patched.")
