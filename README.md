# MegaPowerReal — Pipeline for Mega/Power (Fetch, Stats, ML, Report, API)

This repository contains a full pipeline:
- fetch lottery history (Mega 6/45 & Power 6/55)
- preprocess and create `n1..n6`
- compute statistics (frequency, pair frequency, repeats)
- train models (LightGBM, CatBoost, MLP) and optional TensorFlow model
- produce Excel report with sheets & predictions
- optional email notification
- GitHub Actions workflow trains models and uploads artifacts

---

## Quick start (local)

1. Create virtualenv and install core deps:

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
