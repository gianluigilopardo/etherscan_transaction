import requests
import polars as pl
from datetime import datetime, timezone
import time
import os
from pathlib import Path
from dotenv import load_dotenv
from utils.utils import (
    print_overall_transaction_dates,
    load_index, save_index, update_index_with_file, is_block_in_index,
    make_chunk_filename
)
from ecb import get_proxies

load_dotenv()
api_key = os.getenv('ETHERSCAN_KEY')

TRANSACTIONS_DIR = 'transactions_usdt_eth_2025'
INDEX_PATH = os.path.join(TRANSACTIONS_DIR, 'index.json')
API_SLEEP_SECONDS = 0.25
MAX_EMPTY_BATCHES = 3
BASE_URL_V2 = 'https://api.etherscan.io/v2/api'


def _etherscan_get(params: dict, max_retries: int = 3):
    """Generic Etherscan V2 GET with minimal backoff."""
    attempt = 0
    while True:
        r = requests.get(BASE_URL_V2, params=params, proxies=get_proxies(), timeout=30)
        try:
            data = r.json()
        except Exception:
            data = {'status': '0', 'result': f'Non-JSON response: {r.text[:120]}'}
        # Rate limit or transient signals
        msg = (data.get('message') or '') + ' ' + str(data.get('result'))
        if ('rate limit' in msg.lower() or 'too many' in msg.lower()) and attempt < max_retries:
            sleep_for = API_SLEEP_SECONDS * (2 ** attempt)
            print(f'[WARN] Rate limit, backing off {sleep_for:.2f}s (attempt {attempt+1}/{max_retries})')
            time.sleep(sleep_for)
            attempt += 1
            continue
        return data

def _fetch_latest_batch(token_address: str):
    # Use shared helper (still descending)
    params = {
        'chainid': 1,
        'module': 'account',
        'action': 'tokentx',
        'contractaddress': token_address,
        'sort': 'desc',
        'apikey': api_key
    }
    data = _etherscan_get(params)
    if data.get('status') != '1':
        raise RuntimeError(f"Initial fetch failed: {data.get('result')}")
    result = data.get('result', [])
    if not result:
        raise RuntimeError('No transactions returned in initial batch.')
    return pl.DataFrame(result)

def _fetch_batch(token_address: str, end_block: int):
    """Fetch a descending batch ending at end_block (inclusive) via V2 API."""
    params = {
        'chainid': 1,
        'module': 'account',
        'action': 'tokentx',
        'contractaddress': token_address,
        'endblock': end_block,
        'sort': 'desc',
        'apikey': api_key
    }
    data = _etherscan_get(params)
    status = data.get('status')
    if status == '1':
        result = data.get('result', [])
        return (pl.DataFrame(result) if result else pl.DataFrame(), data)
    # status == '0' could be genuine "No transactions found"
    result_field = str(data.get('result', '')).lower()
    if 'no transactions found' in result_field:
        return pl.DataFrame(), data
    # Return None to signal a hard API problem (handled upstream)
    return None, data

def _parse_range_from_filename(fname: str):
    """Parse HIGH_LOW from a filename like '23524414_23524246.csv'."""
    if not fname:
        return None
    base = os.path.basename(fname)
    stem = base.split('.')[0]
    parts = stem.split('_')
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        a, b = int(parts[0]), int(parts[1])
        return max(a, b), min(a, b)
    return None

def _extract_range(entry):
    """Return (high, low) tuple from a variety of possible index entry formats.

    Supported:
      - plain filename string "HIGH_LOW.csv"
      - dict with keys: ('high','low') or ('max_block','min_block') etc.
      - dict that only has 'file' / 'filename' (then parse)
    """
    # String form
    if isinstance(entry, str):
        return _parse_range_from_filename(entry)

    # Dict form
    if isinstance(entry, dict):
        high = low = None
        for h_key in ('high', 'max_block', 'max', 'hi'):
            if h_key in entry:
                try:
                    high = int(entry[h_key])
                    break
                except Exception:
                    pass
        for l_key in ('low', 'min_block', 'min', 'lo'):
            if l_key in entry:
                try:
                    low = int(entry[l_key])
                    break
                except Exception:
                    pass
        if high is not None and low is not None:
            return high, low
        # Try filename fields
        fname = entry.get('file') or entry.get('filename')
        rng = _parse_range_from_filename(fname) if fname else None
        if rng:
            return rng
    return None

def _iter_index_entries(index_obj):
    """Yield raw entries from various index container shapes."""
    if not index_obj:
        return
    if isinstance(index_obj, list):
        for e in index_obj:
            yield e
    elif isinstance(index_obj, dict):
        # Common pattern: {'files': [...]} or some dict of file->meta
        if 'files' in index_obj and isinstance(index_obj['files'], list):
            for e in index_obj['files']:
                yield e
        else:
            # Fall back to values
            for e in index_obj.values():
                yield e

def _covering_range(block: int, index_obj) -> tuple[int, int] | None:
    """Return (high, low) of a range covering block, else None."""
    for entry in _iter_index_entries(index_obj):
        rng = _extract_range(entry)
        if not rng:
            continue
        hi, lo = rng
        if lo <= block <= hi:
            return hi, lo
    return None

def fetch_and_save_transactions(token_address: str, start_date: str):
    """Backward fetch from latest block down to start_date (inclusive).

    start_date: YYYY-MM-DD (oldest date boundary). We stop once all older txs excluded.
    Chunk files named {high}_{low}.csv with descending coverage.
    """
    os.makedirs(TRANSACTIONS_DIR, exist_ok=True)
    index = load_index(INDEX_PATH)
    start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())

    print('[INFO] Starting fetch')
    print(f'[INFO] Boundary start_date={start_date} (timestamp {start_ts})')

    try:
        latest_df = _fetch_latest_batch(token_address)
    except Exception as e:
        print(f'[ERROR] {e}')
        return

    newest_block = int(latest_df['blockNumber'].max())
    newest_ts = int(latest_df['timeStamp'].max())
    print(f'[DEBUG] Newest block {newest_block} newest timestamp {newest_ts}')

    print('Coverage before:')
    print_overall_transaction_dates(TRANSACTIONS_DIR)

    current_block = newest_block
    empty_batches = 0
    written = 0
    oldest_ts_seen = newest_ts  # track oldest timestamp reached in this run

    while True:
        covering = _covering_range(current_block, index)
        if covering:
            hi, lo = covering
            next_block = lo - 1
            if next_block < 0:
                print('[INFO] Reached block < 0, stopping.')
                break
            print(f'[INFO] Block {current_block} lies in existing range {hi}->{lo}; skipping to {next_block}')
            current_block = next_block
            # Check if we still need more (timestamp boundary handled later)
            continue

        df, meta = _fetch_batch(token_address, current_block)
        if df is None:  # hard error
            print(f"[WARN] API error at block {current_block}: {meta.get('result')} Retrying once after sleep.")
            time.sleep(API_SLEEP_SECONDS * 5)
            df, meta = _fetch_batch(token_address, current_block)
            if df is None:
                print('[ERROR] Persistent API failure, aborting.')
                break
        if df.is_empty():
            empty_batches += 1
            if empty_batches >= MAX_EMPTY_BATCHES:
                print('[INFO] Max empty batches -> stopping.')
                break
            current_block -= 1
            continue

        lowest_block = int(df['blockNumber'].min())
        oldest_ts_batch = int(df['timeStamp'].min())

        # Date trimming
        if oldest_ts_batch < start_ts:
            df = df.filter(pl.col('timeStamp') >= start_ts)
            if df.is_empty():
                print('[INFO] Batch entirely below boundary; stopping.')
                break
            lowest_block = int(df['blockNumber'].min())

        # Add datetime at end (avoid duplicate computation for trimmed set)
        df = df.with_columns(pl.from_epoch('timeStamp', time_unit='s').alias('datetime'))

        filename = make_chunk_filename(current_block, lowest_block)
        out_path = Path(TRANSACTIONS_DIR) / filename
        try:
            df.write_csv(out_path)
            index = update_index_with_file(index, filename)
            # No need to recompute entire structure; skip as extraction is resilient.
            save_index(INDEX_PATH, index)
            written += 1
            print(f'[DEBUG] Wrote {filename} rows={df.height} blocks {current_block}->{lowest_block}')
            # Progress every 10 chunks
            oldest_ts_file = int(df['timeStamp'].min())
            if oldest_ts_file < oldest_ts_seen:
                oldest_ts_seen = oldest_ts_file
            if written % 10 == 0:
                progress_dt = datetime.fromtimestamp(oldest_ts_seen, tz=timezone.utc)
                print(f'[PROGRESS] After {written} chunks oldest timestamp so far: '
                      f'{progress_dt.isoformat()} (block {lowest_block})')
        except Exception as e:
            print(f'[ERROR] Could not write {filename}: {e}')
            break

        current_block = lowest_block - 1
        if oldest_ts_batch < start_ts:
            print('[INFO] Reached boundary timestamp; stopping.')
            break
        time.sleep(API_SLEEP_SECONDS)

    print(f'[INFO] Fetch finished. Chunks written: {written}')
    print('Coverage after:')
    print_overall_transaction_dates(TRANSACTIONS_DIR)

if __name__ == '__main__':
    token_address = '0xdac17f958d2ee523a2206206994597c13d831ec7'
    start_date = '2025-01-01'
    fetch_and_save_transactions(token_address, start_date)
