import os
import time
import json
import requests
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed

# New: support multiple base endpoints & proxies
TRONSCAN_BASE = os.getenv('TRONSCAN_BASE', 'https://apilist.tronscanapi.com')
TRONSCAN_BASES = [b.strip() for b in os.getenv('TRONSCAN_BASES', TRONSCAN_BASE).split(',') if b.strip()]
API_KEY = os.getenv('TRON_KEY')
DEFAULT_LIMIT = int(os.getenv('TRON_LIMIT', '50'))
# Proxy env (corporate networks / DNS issues)
PROXY_ALL = os.getenv('TRON_PROXY')
HTTP_PROXY = os.getenv('TRON_HTTP_PROXY') or PROXY_ALL or os.getenv('HTTP_PROXY')
HTTPS_PROXY = os.getenv('TRON_HTTPS_PROXY') or PROXY_ALL or os.getenv('HTTPS_PROXY')
PROXIES = {'http': HTTP_PROXY, 'https': HTTPS_PROXY} if (HTTP_PROXY or HTTPS_PROXY) else None
HEADERS = {'Accept': 'application/json'}
if API_KEY:
    HEADERS['TRON-PRO-API-KEY'] = API_KEY

# ---------------- HTTP helpers -----------------

def _get_single(base: str, path: str, params: Dict[str, Any], *, timeout: int, retries: int, backoff: float, debug: bool) -> Dict[str, Any]:
    url = base.rstrip('/') + path
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout, proxies=PROXIES)
            if r.status_code == 200:
                try:
                    return r.json() or {}
                except Exception:
                    return {}
            if r.status_code == 429:
                if debug:
                    print(f"[DEBUG][TRONSCAN] 429 {url} attempt={attempt}")
                time.sleep(backoff * (2 ** attempt) + 0.25)
            else:
                if debug:
                    print(f"[DEBUG][TRONSCAN] HTTP {r.status_code} {url} attempt={attempt}")
                time.sleep(backoff * (attempt + 1))
        except requests.RequestException as e:
            if debug:
                print(f"[DEBUG][TRONSCAN] net err {e} {url} attempt={attempt}")
            time.sleep(backoff * (attempt + 1))
    return {}

def _get(path: str, params: Dict[str, Any], *, timeout: int = 40, retries: int = 4, backoff: float = 0.6, debug: bool = False) -> Dict[str, Any]:
    for idx, base in enumerate(TRONSCAN_BASES):
        data = _get_single(base, path, params, timeout=timeout, retries=retries, backoff=backoff, debug=debug)
        if data:
            if debug:
                print(f"[DEBUG][TRONSCAN] success base={base}")
            return data
        if debug and idx < len(TRONSCAN_BASES)-1:
            print(f"[DEBUG][TRONSCAN] switching base ({idx+1}/{len(TRONSCAN_BASES)})")
    return {}

# ---------------- Core fetch (time window + paging) -----------------

def fetch_transactions_interval(contract: str, start_ts_ms: int, end_ts_ms: int, *, limit: int, debug: bool) -> List[Dict[str, Any]]:
    """Fetch all transactions where toAddress=contract & contractType=31 in [start_ts_ms,end_ts_ms] (ms).
    Uses offset paging with start parameter. Returns raw list (no normalization)."""
    out: List[Dict[str, Any]] = []
    offset = 0
    page = 0
    while True:
        params = {
            'sort': '-timestamp',
            'count': 'true',
            'limit': limit,
            'start': offset,
            'start_timestamp': start_ts_ms,
            'end_timestamp': end_ts_ms,
            'toAddress': contract,
            'contractType': 31
        }
        data = _get('/api/transaction', params, debug=debug)
        txs = data.get('data') if isinstance(data, dict) else None
        if not isinstance(txs, list):
            txs = []
        if debug:
            print(f"[DEBUG][TRONSCAN] interval page={page} off={offset} size={len(txs)} start_ts={start_ts_ms} end_ts={end_ts_ms}")
        if not txs:
            break
        out.extend(txs)
        if len(txs) < limit:
            break
        offset += limit
        page += 1
        time.sleep(0.11)
    return out

def fetch_trc20_transfers_interval(contract: str, start_ts_ms: int, end_ts_ms: int, *, limit: int, debug: bool) -> List[Dict[str, Any]]:
    """Fetch all TRC20 transfers for a contract in [start_ts_ms,end_ts_ms] newest->older via offset paging."""
    out: List[Dict[str, Any]] = []
    offset = 0
    page = 0
    while True:
        params = {
            'contract_address': contract,
            'start_timestamp': start_ts_ms,
            'end_timestamp': end_ts_ms,
            'sort': '-timestamp',
            'limit': limit,
            'start': offset,
            'count': 'true'
        }
        data = _get('/api/token_trc20/transfers', params, debug=debug)
        transfers = []
        if isinstance(data, dict):
            for key in ('token_transfers','trc20_transfers','trc20Transfers','data'):
                if isinstance(data.get(key), list):
                    transfers = data.get(key)  # type: ignore
                    break
        if debug:
            print(f"[DEBUG][TRONSCAN] trc20 page={page} off={offset} size={len(transfers)} start={start_ts_ms} end={end_ts_ms}")
        if not transfers:
            break
        out.extend(transfers)
        if len(transfers) < limit:
            break
        offset += limit
        page += 1
        time.sleep(0.11)
    return out

def fetch_trc20_transfers_page(contract: str, start_ts_ms: int, end_ts_ms: int, *, offset: int, limit: int, debug: bool) -> List[Dict[str, Any]]:
    """Fetch a single TRC20 transfers page for contract within time window using offset."""
    params = {
        'contract_address': contract,
        'start_timestamp': start_ts_ms,
        'end_timestamp': end_ts_ms,
        'sort': '-timestamp',
        'limit': limit,
        'start': offset,
        'count': 'true'
    }
    data = _get('/api/token_trc20/transfers', params, debug=debug)
    transfers = []
    if isinstance(data, dict):
        for key in ('token_transfers','trc20_transfers','trc20Transfers','data'):
            if isinstance(data.get(key), list):
                transfers = data.get(key)  # type: ignore
                break
    if debug:
        print(f"[DEBUG][TRONSCAN] trc20 single page off={offset} size={len(transfers)} start={start_ts_ms} end={end_ts_ms}")
    return transfers

def fetch_trc20_transfers_page_simple(contract: str, offset: int, *, limit: int, debug: bool) -> List[Dict[str, Any]]:
    """Fetch newest-first TRC20 transfers page for contract using plain offset (no time filters)."""
    params = {
        'contract_address': contract,
        'sort': '-timestamp',
        'limit': limit,
        'start': offset,
        'count': 'true'
    }
    data = _get('/api/token_trc20/transfers', params, debug=debug)
    transfers: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        for key in ('token_transfers','trc20_transfers','trc20Transfers','data'):
            if isinstance(data.get(key), list):
                transfers = data.get(key)  # type: ignore
                break
    if debug:
        print(f"[DEBUG][TRONSCAN] simple page off={offset} size={len(transfers)}")
    return transfers

def fetch_transactions_page_simple(contract: str, offset: int, *, limit: int, debug: bool) -> List[Dict[str, Any]]:
    """Fetch newest-first contract transactions via /api/transaction using contract_address (plain offset)."""
    params = {
        'contract_address': contract,
        'sort': '-timestamp',
        'limit': limit,
        'start': offset,
        'count': 'true'
    }
    data = _get('/api/transaction', params, debug=debug)
    txs: List[Dict[str, Any]] = []
    if isinstance(data, dict):
        if isinstance(data.get('data'), list):
            txs = data.get('data')  # type: ignore
        else:
            # Fallback: find first list value
            for v in data.values():
                if isinstance(v, list):
                    txs = v  # type: ignore
                    break
    if debug:
        print(f"[DEBUG][TRONSCAN] tx simple page off={offset} size={len(txs)}")
    return txs

def fetch_transactions_page_window(contract: str, start_ts_ms: int, end_ts_ms: int, *, offset: int, limit: int, debug: bool) -> List[Dict[str, Any]]:
    """Fetch a single page of contract transactions within a time window using /api/transaction."""
    params = {
        'contract_address': contract,
        'start_timestamp': start_ts_ms,
        'end_timestamp': end_ts_ms,
        'sort': '-timestamp',
        'limit': limit,
        'start': offset,
        'count': 'true'
    }
    data = _get('/api/transaction', params, debug=debug)
    txs: List[Dict[str, Any]] = []
    if isinstance(data, dict) and isinstance(data.get('data'), list):
        txs = data.get('data')  # type: ignore
    elif isinstance(data, dict):
        for v in data.values():
            if isinstance(v, list):
                txs = v  # type: ignore
                break
    if debug:
        print(f"[DEBUG][TRONSCAN] window tx page off={offset} size={len(txs)} start={start_ts_ms} end={end_ts_ms}")
    return txs

# ---------------- Normalization -----------------

def normalize_transactions(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    norm: List[Dict[str, Any]] = []
    for r in raw:
        if not isinstance(r, dict):
            continue
        ts = r.get('timestamp') or r.get('time') or r.get('block_timestamp') or r.get('block_ts')
        try:
            ts = int(ts)
        except Exception:
            continue
        ts_ms = ts * 1000 if ts < 10**12 else ts
        ts_sec = ts_ms // 1000
        rec = dict(r)
        if 'hash' not in rec:
            for k in ('transaction_id','txID'):
                if k in rec:
                    rec['hash'] = rec.get(k)
                    break
        rec['timestamp_ms'] = ts_ms
        rec['timeStamp'] = ts_sec
        rec['datetime'] = datetime.fromtimestamp(ts_sec, tz=timezone.utc).isoformat()
        norm.append(rec)
    norm.sort(key=lambda x: x['timeStamp'], reverse=True)
    return norm

def normalize_trc20_transfers(raw: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    norm: List[Dict[str, Any]] = []
    for t in raw:
        if not isinstance(t, dict):
            continue
        ts = t.get('block_ts') or t.get('timestamp') or t.get('time')
        try:
            ts = int(ts)
        except Exception:
            continue
        if ts < 10**12:
            ts_ms = ts * 1000
        else:
            ts_ms = ts
        ts_sec = ts_ms // 1000
        rec = dict(t)
        txid = rec.get('transaction_id') or rec.get('hash') or rec.get('txID')
        if txid:
            rec['hash'] = txid
        rec['timestamp_ms'] = ts_ms
        rec['timeStamp'] = ts_sec
        rec['datetime'] = datetime.fromtimestamp(ts_sec, tz=timezone.utc).isoformat()
        # Amount from quant (USDT has 6 decimals)
        quant = rec.get('quant') or rec.get('value')
        try:
            rec['amount_usdt'] = int(quant) / 1_000_000 if quant is not None else None
        except Exception:
            rec['amount_usdt'] = None
        norm.append(rec)
    norm.sort(key=lambda x: x['timeStamp'], reverse=True)
    return norm

# ---------------- Cost retrieval -----------------
COST_FIELDS = ['fee','energy_fee','net_fee','energy_usage_total','energy_usage','net_usage','energy_penalty_total','energy_penalty_usage','energy_penalty_fee']

def fetch_tx_cost(hash_: str, *, debug: bool = False) -> Dict[str, Any]:
    params = {'hash': hash_}
    data = _get('/api/transaction-info', params, debug=debug)
    if not data:
        return {'hash': hash_, 'cost_pending': 1}
    cost = data.get('cost') or {}
    receipt = data.get('receipt') or {}
    out = {'hash': hash_}
    for f in COST_FIELDS:
        out[f] = cost.get(f, receipt.get(f))
    for k in ('fee','energy_fee','net_fee'):
        v = out.get(k)
        if isinstance(v, int):
            out[f'{k}_trx'] = v / 1_000_000
    out['contract_ret'] = data.get('contractRet') or cost.get('result')
    return out

def fetch_costs_parallel(hashes: List[str], *, max_workers: int, debug: bool, throttle: float = 0.25) -> Dict[str, Dict[str, Any]]:
    hashes = [h for h in dict.fromkeys(hashes)]
    results: Dict[str, Dict[str, Any]] = {}
    if not hashes:
        return results
    if len(hashes) <= 3:
        for h in hashes:
            results[h] = fetch_tx_cost(h, debug=debug)
            time.sleep(throttle)
        return results
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(fetch_tx_cost, h, debug=debug): h for h in hashes}
        for fut in as_completed(fut_map):
            h = fut_map[fut]
            try:
                res = fut.result()
            except Exception as e:
                if debug:
                    print(f"[DEBUG][TRONSCAN] cost error {h} {e}")
                res = {'hash': h, 'cost_pending': 1}
            results[h] = res
            time.sleep(throttle)
    return results

# ---------------- Enrichment -----------------

def enrich_with_cost(txs: List[Dict[str, Any]], cost_map: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for t in txs:
        h = t.get('hash')
        c = cost_map.get(h, {}) if h else {}
        merged = dict(t)
        for k, v in c.items():
            if k == 'hash':
                continue
            merged[k] = v
        out.append(merged)
    return out

# ---------------- Window iterator -----------------

def iterate_time_windows(start_ms: int, end_ms: int, *, window_ms: int) -> List[Tuple[int,int]]:
    windows: List[Tuple[int,int]] = []
    cur_end = end_ms
    while cur_end >= start_ms:
        ws = max(start_ms, cur_end - window_ms + 1)
        windows.append((ws, cur_end))
        cur_end = ws - 1
    return windows

# ---------------- Connectivity test -----------------

def test_connectivity(contract: str, debug: bool = False) -> Tuple[bool, str]:
    """Connectivity test targeting TRC20 transfers endpoint (less restrictive).
    Returns (ok, message)."""
    now_ms = int(time.time()*1000)
    params = {
        'contract_address': contract,
        'limit': 1,
        'sort': '-timestamp',
        'start': 0,
        'start_timestamp': now_ms-3600000,
        'end_timestamp': now_ms
    }
    data = _get('/api/token_trc20/transfers', params, debug=debug)
    if data.get('data') or any(isinstance(v, list) and v for v in data.values()):
        return True, 'ok'
    # Empty could be rate limit / auth / window no transfers; treat as soft fail
    return False, 'no_data'

# Proxy setter for runtime injection
def set_external_proxies(proxies: Dict[str, str]):
    global PROXIES
    PROXIES = proxies
