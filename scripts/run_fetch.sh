#!/usr/bin/env bash
set -e
python -c "from utils.fetch_data import fetch_all_data; import sys; fetch_all_data(limit=120, save_dir='data')"
python -c "from utils.fetch_checks import load_saved, quick_validate, print_head; m,p = load_saved('data'); print_head(m); print_head(p); quick_validate(m,'mega'); quick_validate(p,'power')"
