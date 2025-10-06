# Fetch all USDT (TRC20) transfer events on Tron via TronScan.
# Simple backward collection via offset until reaching start_date boundary.

import os
import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Set
from dotenv import load_dotenv
from datetime import timedelta  # add after imports
from pathlib import Path

from ecb import get_proxies
from utils.tronscan import (
    normalize_transactions,
    fetch_transactions_page_window,
    set_external_proxies,
    fetch_trc20_transfers_page,
    normalize_trc20_transfers
)

load_dotenv()
USDT_CONTRACT = 'TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t'
OUT_DIR = 'transactions_tron'
BLOCK_DIR = os.path.join(OUT_DIR, 'blocks')
RESUME_FILE = os.path.join(OUT_DIR, 'resume.json')
PAGE_LIMIT = int(os.getenv('TRON_PAGE_LIMIT', '50'))
WINDOW_MODE = os.getenv('TRON_WINDOW_MODE', 'month')  # 'month' or hours
START_DATE = os.getenv('TRON_START_DATE', '2024-01-01')
DEBUG = os.getenv('TRON_DEBUG', '1') == '1'
SLEEP_PAGE = float(os.getenv('TRON_SLEEP_PAGE', '0.10'))
DUP_PAGES_BREAK = int(os.getenv('TRON_DUP_PAGES_BREAK', '3'))  # consecutive duplicate pages to break window early
DATA_SOURCE = os.getenv('DATA_SOURCE', 'transfers')  # 'transfers' or 'transactions'
MAX_PAGES_PER_WINDOW = int(os.getenv('TRON_MAX_PAGES_PER_WINDOW', '4000'))  # hard cap to prevent extremely long windows
SUBWINDOW_DAYS = int(os.getenv('TRON_SUBWINDOW_DAYS', '0'))  # if >0 split each month window into day-sized backward slices


def _ensure():
    os.makedirs(OUT_DIR, exist_ok=True)
    os.makedirs(BLOCK_DIR, exist_ok=True)


def _load_resume() -> Dict[str, Any]:
    if os.path.exists(RESUME_FILE):
        try:
            with open(RESUME_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_resume(state: Dict[str, Any]):
    state['updated'] = datetime.utcnow().isoformat() + 'Z'
    with open(RESUME_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _write_block_records(block: int, records: List[Dict[str, Any]]):
    """Store ALL raw transfer objects for a block (append + dedupe by transaction_id)."""
    if block is None or not records:
        return
    path = os.path.join(BLOCK_DIR, f"{block}.json")
    existing: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                existing = json.load(f) or []
            for r in existing:
                tid = r.get('transaction_id') or r.get('hash')
                if tid:
                    seen.add(tid)
        except Exception:
            existing = []
            seen = set()
    appended = 0
    for r in records:
        tid = r.get('transaction_id') or r.get('hash')
        if tid and tid in seen:
            continue
        existing.append(r)
        if tid:
            seen.add(tid)
        appended += 1
    if appended:
        # keep newest first
        existing.sort(key=lambda x: x.get('block_ts', x.get('timestamp', x.get('time', 0))), reverse=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(existing, f, ensure_ascii=False, separators=(',', ':'))


def _month_windows(start_dt: datetime, end_dt: datetime) -> List[tuple[int,int]]:
    """Return list of (start_ms,end_ms) month windows newest->oldest covering [start_dt,end_dt]."""
    windows: List[tuple[int,int]] = []
    # move to first day next month after end_dt
    cur = datetime(end_dt.year, end_dt.month, 1, tzinfo=timezone.utc)
    # ensure cur covers end_dt
    if cur <= end_dt:
        # advance one month
        if cur.month == 12:
            cur = datetime(cur.year+1, 1, 1, tzinfo=timezone.utc)
        else:
            cur = datetime(cur.year, cur.month+1, 1, tzinfo=timezone.utc)
    while True:
        next_month = cur
        # previous month start
        if cur.month == 1:
            prev_start = datetime(cur.year-1, 12, 1, tzinfo=timezone.utc)
        else:
            prev_start = datetime(cur.year, cur.month-1, 1, tzinfo=timezone.utc)
        window_start = prev_start
        window_end = next_month - timedelta(milliseconds=1)
        if window_end < start_dt:
            break
        ws_ms = int(max(window_start, start_dt).timestamp()*1000)
        we_ms = int(min(window_end, end_dt).timestamp()*1000)
        windows.append((ws_ms, we_ms))
        cur = prev_start
        if window_start <= start_dt:
            break
    return windows


def _scan_existing_blocks() -> Set[int]:
    blocks: Set[int] = set()
    p = Path(BLOCK_DIR)
    if not p.exists():
        return blocks
    for f in p.glob('*.json'):
        try:
            b = int(f.stem)
            blocks.add(b)
        except Exception:
            continue
    return blocks


def harvest_simple(start_date: str = START_DATE):
    _ensure()
    boundary_dt = datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    now_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
    print(f'[INFO][TRONSCAN] Windowed contract harvest start boundary={boundary_dt.isoformat()} end={now_dt.isoformat()}')
    try:
        set_external_proxies(get_proxies())
    except Exception:
        pass
    resume = _load_resume()
    # Resume state per window
    windows = _month_windows(boundary_dt, now_dt)
    # windows are newest->older
    win_index = resume.get('win_index', 0)
    page_offset = resume.get('page_offset', 0)
    total_saved = resume.get('total_saved', 0)  # counts newly appended tx
    existing_blocks: Set[int] = _scan_existing_blocks()  # still used for stats but not for skipping
    if DEBUG:
        print(f"[DEBUG][TRONSCAN] existing_blocks_loaded={len(existing_blocks)}")

    while win_index < len(windows):
        ws_ms, we_ms = windows[win_index]
        if SUBWINDOW_DAYS > 0:
            # produce subwindows newest->older within the month window
            day_ms = 86400000 * SUBWINDOW_DAYS
            sub_end = we_ms
            subwindows: List[tuple[int,int]] = []
            while sub_end >= ws_ms:
                sub_start = max(ws_ms, sub_end - day_ms + 1)
                subwindows.append((sub_start, sub_end))
                sub_end = sub_start - 1
        else:
            subwindows = [(ws_ms, we_ms)]
        if DEBUG:
            print(f'[DEBUG][TRONSCAN] Window idx={win_index} slices={len(subwindows)} start={datetime.fromtimestamp(ws_ms/1000, tz=timezone.utc).isoformat()} end={datetime.fromtimestamp(we_ms/1000, tz=timezone.utc).isoformat()} start_offset={page_offset}')
        for slice_idx, (s_start, s_end) in enumerate(subwindows):
            pages_in_slice = 0
            if DEBUG and len(subwindows) > 1:
                print(f'[DEBUG][TRONSCAN]  Slice {slice_idx+1}/{len(subwindows)} {datetime.fromtimestamp(s_start/1000, tz=timezone.utc).isoformat()} -> {datetime.fromtimestamp(s_end/1000, tz=timezone.utc).isoformat()} offset_start={page_offset}')
            while True:
                if DATA_SOURCE == 'transfers':
                    page_raw = fetch_trc20_transfers_page(USDT_CONTRACT, s_start, s_end, offset=page_offset, limit=PAGE_LIMIT, debug=DEBUG)
                else:
                    page_raw = fetch_transactions_page_window(USDT_CONTRACT, s_start, s_end, offset=page_offset, limit=PAGE_LIMIT, debug=DEBUG)
                if not page_raw:
                    page_offset = 0
                    break
                pages_in_slice += 1
                retrieval_ts = int(time.time())
                for rr in page_raw:
                    rr['retrieved_at'] = retrieval_ts
                norm = normalize_trc20_transfers(page_raw) if DATA_SOURCE == 'transfers' else normalize_transactions(page_raw)
                if not norm:
                    page_offset += PAGE_LIMIT
                    time.sleep(SLEEP_PAGE)
                    continue
                per_block: Dict[int, List[Dict[str, Any]]] = {}
                for raw_r in page_raw:
                    blk = raw_r.get('block') or raw_r.get('blockNumber')
                    try:
                        blk_i = int(blk)
                    except Exception:
                        continue
                    per_block.setdefault(blk_i, []).append(raw_r)
                new_block_count = 0
                new_tx_count = 0
                for blk_num, recs in per_block.items():
                    before_len = 0
                    path_blk = os.path.join(BLOCK_DIR, f"{blk_num}.json")
                    if os.path.exists(path_blk):
                        try:
                            with open(path_blk, 'r', encoding='utf-8') as f:
                                before_len = len(json.load(f) or [])
                        except Exception:
                            before_len = 0
                    _write_block_records(blk_num, recs)
                    existing_blocks.add(blk_num)
                    try:
                        with open(path_blk, 'r', encoding='utf-8') as f:
                            after_len = len(json.load(f) or [])
                        appended_here = max(after_len - before_len, 0)
                        new_tx_count += appended_here
                        if appended_here > 0:
                            new_block_count += 1
                    except Exception:
                        pass
                total_saved += new_tx_count
                if DEBUG:
                    print(f"[DEBUG][TRONSCAN] win={win_index} slice={slice_idx} off={page_offset} new_blocks_with_additions={new_block_count} new_tx={new_tx_count} total_tx_added={total_saved} pages_in_slice={pages_in_slice}")
                # Duplicate page detection (no new tx appended)
                if new_tx_count == 0:
                    dup_counter = resume.get('dup_counter', 0) + 1
                else:
                    dup_counter = 0
                resume['dup_counter'] = dup_counter
                if dup_counter >= DUP_PAGES_BREAK:
                    if DEBUG:
                        print(f"[DEBUG][TRONSCAN] breaking slice early after {dup_counter} duplicate pages (win={win_index} slice={slice_idx})")
                    page_offset = 0
                    break
                if pages_in_slice >= MAX_PAGES_PER_WINDOW:
                    if DEBUG:
                        print(f"[WARN][TRONSCAN] MAX_PAGES_PER_WINDOW reached ({MAX_PAGES_PER_WINDOW}) win={win_index} slice={slice_idx}; moving on.")
                    page_offset = 0
                    break
                if len(page_raw) < PAGE_LIMIT:
                    page_offset = 0
                    break
                page_offset += PAGE_LIMIT
                resume.update({
                    'win_index': win_index,
                    'page_offset': page_offset,
                    'total_saved': total_saved,
                    'dup_counter': dup_counter,
                    'existing_blocks': len(existing_blocks),
                    'data_source': DATA_SOURCE,
                    'pages_in_slice': pages_in_slice
                })
                _save_resume(resume)
                time.sleep(SLEEP_PAGE)
            page_offset = 0  # reset for next slice
        win_index += 1
        resume.update({
            'win_index': win_index,
            'page_offset': page_offset,
            'total_saved': total_saved,
            'dup_counter': resume.get('dup_counter', 0),
            'existing_blocks': len(existing_blocks),
            'data_source': DATA_SOURCE
        })
        _save_resume(resume)
    print(f'[INFO][TRONSCAN] Harvest complete total_new_tx_appended={total_saved} blocks_dir={BLOCK_DIR}')


if __name__ == '__main__':
    harvest_simple()
