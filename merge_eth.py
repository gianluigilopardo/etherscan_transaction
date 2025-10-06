from pathlib import Path
import polars as pl
from utils.utils import (
    parse_block_range_from_filename,
    load_index, save_index, update_index_with_file, mark_files_merged,
    append_dataframe
)
import os

TRANSACTIONS_DIR = 'transactions_eth'
INDEX_PATH = os.path.join(TRANSACTIONS_DIR, 'index.json')
MERGED_FILE = 'transactions_eth.csv'
BATCH_SAVE_INTERVAL = 100  # save after this many source chunks processed

STANDARD_COLUMNS = [
    'blockNumber', 'timeStamp', 'hash', 'nonce', 'blockHash',
    'from', 'to', 'contractAddress', 'value', 'tokenName',
    'tokenSymbol', 'tokenDecimal', 'transactionIndex', 'gas',
    'gasPrice', 'gasUsed', 'cumulativeGasUsed', 'input',
    'confirmations', 'datetime'
]

def _iter_new_chunk_files(directory: Path, index: dict) -> list:
    """Return list of (high, low, Path) for chunk CSVs not yet marked merged."""
    merged_files = set(index.get('merged', {}).get('files', []))
    candidates = []
    for f in directory.glob('*.csv'):
        if f.name == MERGED_FILE:
            continue
        if f.name in merged_files:
            continue
        high, low = parse_block_range_from_filename(f.name)
        if high is None:
            continue
        candidates.append((high, low, f))
    # Sort descending by high so newest first (optional)
    candidates.sort(reverse=True)
    return candidates

def merge_csv_files(directory_path, output_file):
    directory = Path(directory_path)
    output = Path(output_file)

    print(f"[DEBUG] Directory: {directory}")
    if not directory.exists():
        raise FileNotFoundError(f"Directory {directory_path} does not exist")

    index = load_index(INDEX_PATH)
    print(f"[DEBUG] Loaded index (merged files count={len(index.get('merged', {}).get('files', []))})")

    new_chunks = _iter_new_chunk_files(directory, index)
    if not new_chunks:
        print("No new chunk files to merge.")
        return
    print(f"[DEBUG] {len(new_chunks)} new chunk files pending merge.")

    processed_files = []
    total_rows_appended = 0

    for i, (high, low, file) in enumerate(new_chunks, start=1):
        try:
            print(f"[DEBUG] ({i}/{len(new_chunks)}) Reading {file.name} blocks {high}->{low}")
            df = pl.read_csv(file)
            missing = set(STANDARD_COLUMNS) - set(df.columns)
            if missing:
                print(f"[WARN] Skipping {file.name}, missing columns: {missing}")
                continue
            df = df.select(STANDARD_COLUMNS)
            append_dataframe(df, output, column_order=STANDARD_COLUMNS)
            total_rows_appended += df.height
            processed_files.append(file.name)
            # Update per-file coverage index
            index = update_index_with_file(index, file.name)
        except Exception as e:
            print(f"[ERROR] Failed processing {file.name}: {e}")
            continue

        if i % BATCH_SAVE_INTERVAL == 0:
            index = mark_files_merged(index, processed_files)
            save_index(INDEX_PATH, index)
            processed_files.clear()
            print(f"[DEBUG] Intermediate save after {i} files, total rows appended so far {total_rows_appended}")

    # Final save of any remaining processed files
    if processed_files:
        index = mark_files_merged(index, processed_files)
        save_index(INDEX_PATH, index)

    print(f"[INFO] Merge complete. Total new rows appended: {total_rows_appended}")

if __name__ == "__main__":
    try:
        script_dir = Path(__file__).parent
        print(f'[DEBUG] Script directory: {script_dir}')
        merge_csv_files(
            script_dir / TRANSACTIONS_DIR,
            script_dir / MERGED_FILE
        )
    except Exception as e:
        print(f"Error: {e}")
