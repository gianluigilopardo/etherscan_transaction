# Consolidated utilities for Etherscan transaction fetch/merge
"""Utility functions for managing transaction chunk files, indexes, and
robust I/O for large-scale token transaction harvesting.

Simplified & hardened:
- Unified index schema (files/global/merged)
- Robust CSV append that auto-removes header (works for polars returning str or bytes)
- Convenience helpers for chunk naming & coverage checks
"""
from __future__ import annotations
import os
import json
import tempfile
from pathlib import Path
from typing import Dict, Tuple, Optional, Iterable
import polars as pl
from datetime import datetime, timezone

# --------------------------------------------------------------------------------------
# Index handling
# --------------------------------------------------------------------------------------

def _empty_index() -> Dict:
    return {"files": {}, "global": {"min": None, "max": None}, "merged": {"min": None, "max": None, "files": []}}

def load_index(index_path: str | Path) -> Dict:
    index_path = Path(index_path)
    if not index_path.exists():
        return _empty_index()
    try:
        with index_path.open('r') as f:
            data = json.load(f)
        if 'files' not in data:  # legacy flat form
            files = data
            lows = [v['low'] for v in files.values()]
            highs = [v['high'] for v in files.values()]
            data = {
                'files': files,
                'global': {'min': min(lows) if lows else None, 'max': max(highs) if highs else None},
                'merged': {'min': None, 'max': None, 'files': []}
            }
        data.setdefault('files', {})
        data.setdefault('global', {'min': None, 'max': None})
        data.setdefault('merged', {'min': None, 'max': None, 'files': []})
        data['merged'].setdefault('files', [])
        return data
    except Exception:
        return _empty_index()

def save_index(index_path: str | Path, index: Dict) -> None:
    index_path = Path(index_path)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_fd, tmp_name = tempfile.mkstemp(prefix='idx_', suffix='.json', dir=str(index_path.parent))
    try:
        with os.fdopen(tmp_fd, 'w') as f:
            json.dump(index, f, indent=2)
        os.replace(tmp_name, index_path)
    except Exception as e:
        print(f"Warning: Could not save index: {e}")
        try: os.unlink(tmp_name)
        except OSError: pass

def parse_block_range_from_filename(filename: str) -> Tuple[Optional[int], Optional[int]]:
    base = Path(filename).name.replace('.csv', '')
    parts = base.split('_')
    if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
        return int(parts[0]), int(parts[1])
    return None, None

def make_chunk_filename(high: int, low: int) -> str:
    return f"{high}_{low}.csv"

def update_index_with_file(index: Dict, filename: str) -> Dict:
    high, low = parse_block_range_from_filename(filename)
    if high is None: return index
    index['files'][filename] = {'high': high, 'low': low}
    gmin = index['global']['min']; gmax = index['global']['max']
    index['global']['min'] = low if gmin is None else min(gmin, low)
    index['global']['max'] = high if gmax is None else max(gmax, high)
    return index

def mark_files_merged(index: Dict, filenames: Iterable[str]) -> Dict:
    names = list(filenames)
    if not names: return index
    lows = [index['files'][n]['low'] for n in names if n in index['files']]
    highs = [index['files'][n]['high'] for n in names if n in index['files']]
    if lows and highs:
        cur_min = index['merged']['min']; cur_max = index['merged']['max']
        index['merged']['min'] = min(lows) if cur_min is None else min(cur_min, min(lows))
        index['merged']['max'] = max(highs) if cur_max is None else max(cur_max, max(highs))
    merged_set = set(index['merged']['files'])
    merged_set.update(names)
    index['merged']['files'] = sorted(merged_set)
    return index

def is_block_in_index(block: int, index: Dict) -> bool:
    gmin = index['global']['min']; gmax = index['global']['max']
    if gmin is None or gmax is None: return False
    if block < gmin or block > gmax: return False
    for meta in index['files'].values():
        if meta['low'] <= block <= meta['high']: return True
    return False

def get_index_extremes(index: Dict) -> Tuple[Optional[int], Optional[int]]:
    return index['global']['min'], index['global']['max']

# --------------------------------------------------------------------------------------
# Directory scan (metadata only)
# --------------------------------------------------------------------------------------

def quick_scan_transaction_dir(dir_path: str | Path) -> Tuple[Optional[int], Optional[int], int]:
    dir_path = Path(dir_path)
    if not dir_path.exists(): return None, None, 0
    lows, highs = [], []
    for f in dir_path.glob('*.csv'):
        h,l = parse_block_range_from_filename(f.name)
        if h is not None:
            highs.append(h); lows.append(l)
    if not lows: return None, None, 0
    return min(lows), max(highs), len(lows)

# --------------------------------------------------------------------------------------
# Robust CSV write / append
# --------------------------------------------------------------------------------------

def append_dataframe(df: pl.DataFrame, path: str | Path, column_order: list[str] | None = None):
    """Append DataFrame to CSV ensuring header only once (handles str/bytes return)."""
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    if column_order:
        cols = [c for c in column_order if c in df.columns]
        df = df.select(cols)
    data = df.write_csv(None)  # may be str or bytes depending on polars version
    if isinstance(data, str):
        first_newline = data.find('\n')
        if first_newline == -1: return
        if not path.exists():
            with path.open('w', encoding='utf-8', newline='') as f: f.write(data)
        else:
            with path.open('a', encoding='utf-8', newline='') as f: f.write(data[first_newline+1:])
    else:  # bytes
        first_newline = data.find(b'\n')
        if first_newline == -1: return
        if not path.exists():
            with path.open('wb') as f: f.write(data)
        else:
            with path.open('ab') as f: f.write(data[first_newline+1:])

# --------------------------------------------------------------------------------------
# Stats / reporting
# --------------------------------------------------------------------------------------

def print_overall_transaction_dates(transactions_dir: str = 'transactions') -> None:
    try:
        dir_path = Path(transactions_dir)
        if not dir_path.exists():
            print("No transactions directory found."); return
        candidates = []
        for f in dir_path.glob('*.csv'):
            h,l = parse_block_range_from_filename(f.name)
            if h is not None: candidates.append((l,h,f))
        if not candidates:
            print("No transaction chunk files found."); return
        oldest_file = min(candidates, key=lambda x: x[0])[2]
        newest_file = max(candidates, key=lambda x: x[1])[2]
        oldest_df = pl.read_csv(oldest_file, columns=['timeStamp'])
        newest_df = pl.read_csv(newest_file, columns=['timeStamp'])
        oldest_ts = int(oldest_df['timeStamp'].min())
        newest_ts = int(newest_df['timeStamp'].max())
        print(f"Oldest transaction date: {datetime.fromtimestamp(oldest_ts, tz=timezone.utc)}")
        print(f"Newest transaction date: {datetime.fromtimestamp(newest_ts, tz=timezone.utc)}")
    except Exception as e:
        print(f"Error determining overall transaction dates: {e}")
