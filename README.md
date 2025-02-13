# Etherscan Token Transaction Fetcher

This script fetches token transaction data from the Etherscan API and saves it to a single CSV file. It retrieves transactions for a specified token from the latest block until a specified end date.

## Requirements

- Python 3.x
- `requests` library for making HTTP requests.
- `polars` library for data manipulation.
- `python-dotenv` library for managing environment variables.

Install the required libraries using pip:

```bash
pip install -r requirements.txt
```

## Setup

1. **API Key**: Obtain an API key from [Etherscan APIs](https://etherscan.io/apis) by signing up for an account.
2. **Environment Variable**: Store your Etherscan API key in a `.env` file in the same directory as the script:

   ```plaintext
   ETHERSCAN_KEY=your_etherscan_api_key_here
   ```

## Usage

1. **Configure Parameters**:
   - Set the `token_address` variable to the contract address of the token you want to fetch transactions for. Default is set to USDT ([Tether](https://tether.to/)).
   - Set the `end_date` variable to the date until which you want to fetch transactions. Format: `YYYY-MM-DD`.

2. **Run the Script**: 
- Fetch and collect transaction data:
   ```bash
   python fetch.py 
   ```
- Merge into single CSV file: 
   ```bash
   python merge.py 
   ```

3. **Output**: The script saves transaction data to CSV files in the `transactions` directory, named based on the block range.

## Example

To fetch transactions for the USDT (Tether) token from the latest block until January 1, 2025:

```python
token_address = '0xdac17f958d2ee523a2206206994597c13d831ec7'  # USDT (Tether)
end_date = '2025-01-01'
```
