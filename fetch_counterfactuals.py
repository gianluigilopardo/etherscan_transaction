import os
import sys
from pathlib import Path
import yfinance as yf
from ecb import get_proxies

os.environ.setdefault('YF_USE_CURL', '0')  # prefer requests backend

TICKER = "^GSPC"
START_DATE = "2019-01-01"
END_DATE = "2025-08-31"
OUT_PATH = Path('data/sp500_historical_data.csv')
OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

proxies = get_proxies()
proxy_url = proxies.get('https') or proxies.get('http')
print(f"[INFO] Proxy: {proxy_url}")

try:
    df = yf.download(TICKER, start=START_DATE, end=END_DATE, progress=False, proxy=proxy_url)
    if df.empty:
        print('[WARN] Empty dataframe.')
    df.to_csv(OUT_PATH)
    print(f"[INFO] Saved {len(df)} rows -> {OUT_PATH}")
except Exception as e:
    print(f"[ERROR] Download failed: {e}")
    sys.exit(1)
