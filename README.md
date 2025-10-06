# Etherscan Token Transaction Fetcher

This project fetches and merges blockchain token transaction data from Etherscan efficiently, even for very large datasets.

## Quick Start

### 1. Install Requirements

```bash
pip install -r requirements.txt
```

### 2. Setup API Key

- Get an Etherscan API key: https://etherscan.io/apis
- Create a `.env` file in the project root directory:
  ```
  ETHERSCAN_KEY=your_etherscan_api_key_here
  ```

### 3. Fetch Data

Edit `fetch.py` to set your token address and end date. Then run:

```bash
python fetch.py
```

- Data is saved in `transactions/{startblock}_{endblock}.csv` files.
- Progress and coverage are tracked in `transactions/index.json` (no need to scan all files each time).

### 4. Merge Data

Combine all block files into a single large CSV:

```bash
python merge.py
```

- Merges only new/unmerged files (using `index.json`).
- Intermediate results are saved every ≤100 files for robustness.

## How It Works

- **index.json**: Tracks which block ranges are already saved/merged. This avoids redundant file scans and speeds up fetch/merge.
- **Debug Output**: Both scripts print progress, block ranges, and skipped files for transparency.
- **Robustness**: Merging saves progress regularly, so you can safely interrupt and resume long jobs.

## Tips for Large Jobs

- You can stop and restart either script at any time; progress is tracked in `index.json`.
- For very large merges, monitor disk space and memory usage.
- Use the debug output to verify which blocks are covered or missing.

## Directory Structure

- `transactions/` — Contains all per-block CSVs and `index.json`.
- `transactions.csv` — The merged, deduplicated dataset.
- `utils/utils.py` — All helper functions (block range parsing, index management, etc).

---

# README for etherscan_transaction project

Project purpose:
Efficiently fetch, store, and merge large volumes of ERC‑20 token transfer transactions into a single consolidated CSV (potentially tens of GB) with resilience and minimal redundant I/O.

Directory structure (key parts):
code/etherscan_transaction/
  fetch.py               Fetch new historical transaction chunks.
  merge.py               Merge chunk CSV files into transactions.csv.
  transactions/          Folder containing chunk CSVs + index.json.
  transactions/index.json Index metadata about chunk coverage & merge status.
  utils/utils.py         Shared helper utilities.

Index JSON (transactions/index.json):
files: per-chunk metadata {filename: {high, low}}
global: overall covered min/max block across all chunk files
merged: {min, max, files[]} list of chunk filenames already merged

Workflow:
1. Set environment variable ETHERSCAN_KEY in a .env file.
2. Run fetch.py providing token contract address and start_date (oldest date you care about). It:
   - Queries newest transactions first, walks backward in block height.
   - Writes chunk CSVs named {high}_{low}.csv (descending batches).
   - Updates/creates index.json incrementally (only metadata, fast).
3. Run merge.py to append any unmerged chunk CSVs into transactions.csv.
   - Processes in batches, periodically records progress into index.json.
   - Skips already merged chunk files via index metadata.

Running examples:
python fetch.py                  # uses default token & start_date inside script
python merge.py                  # merges newly fetched chunks

Long‑running job tips:
- Adjust API_SLEEP_SECONDS in fetch.py if you hit rate limits.
- Increase/decrease BATCH_SAVE_INTERVAL in merge.py based on desired checkpoint frequency (tradeoff: more frequent saves = more I/O, safer restarts).
- Use screen/tmux or a job scheduler for multi‑hour merges.
- Keep an eye on disk space; chunk files + merged file can be large simultaneously.

Resuming after interruption:
- fetch.py consults index.json and stops when it hits already covered blocks.
- merge.py consults merged.files list and only appends new chunks.

Data integrity:
- Chunk files are immutable once written.
- Index updates are atomic (temp file then replace) to reduce corruption risk.
- Merging appends CSV sections without reloading the large transactions.csv.

Debug output:
- Fetch prints batch ranges, file writes, and early termination reasons.
- Merge prints per-file progress, skipped files (missing columns), and periodic checkpoint saves.

Customization:
- Change token address & start date in fetch.py main block or expose CLI arguments if needed.
- Add filtering logic (e.g., addresses) by modifying fetch loop before writing CSV.

Limitations:
- Etherscan API pagination limits may require enhancements for very deep history (add startblock/endblock pagination logic if needed).
- No deduplication check beyond file segmentation; ensure chunk naming uniqueness.

License: Internal use.

---

For more details, see comments in the code.
