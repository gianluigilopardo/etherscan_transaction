import requests
import polars as pl
from datetime import datetime
import time
import os
from dotenv import load_dotenv


load_dotenv()
api_key = os.getenv('ETHERSCAN_KEY')


def fetch_and_save_transactions(token_address, end_date):
    """
    Fetch transactions for a given token from the latest block until a specified end date.
    Saves the transactions to CSV files.
    """

    date_obj = datetime.strptime(end_date, "%Y-%m-%d")
    # Calculate the Unix timestamp
    end_timestamp = int(date_obj.timestamp())

    # Fetch the latest transactions to determine the starting block
    url = f'https://api.etherscan.io/api?module=account&action=tokentx&contractaddress={token_address}&sort=desc&apikey={api_key}'
    response = requests.get(url)
    data = response.json()

    if data['status'] == '1':
        # Extract the transaction data
        transactions = data['result']

        if not transactions:
            print("No transactions found.")
            return

        # Convert the transaction data to a Polars DataFrame
        df = pl.DataFrame(transactions)

        # Determine the starting block as the minimum block number from the latest transactions
        start_block = df['blockNumber'].max()
    else:
        print("Error fetching initial data:", data['result'])
        return

    current_block = int(start_block)
    current_ts = int(df['timeStamp'].max())

    # Create a directory to save transactions, if it doesn't exist
    os.makedirs('transactions', exist_ok=True)

    while current_ts >= end_timestamp:
        # Define the API request URL for fetching token transactions up to the current block
        url = f'https://api.etherscan.io/api?module=account&action=tokentx&contractaddress={token_address}&endblock={current_block}&sort=desc&apikey={api_key}'

        # Make the API request
        response = requests.get(url)
        data = response.json()

        # Check if the request was successful
        if data['status'] == '1':
            # Extract the transaction data
            transactions = data['result']

            if not transactions:
                # No more transactions to fetch in this batch
                break

            # Convert the transaction data to a Polars DataFrame
            df = pl.DataFrame(transactions)

            # Convert the 'timeStamp' column to datetime
            df = df.with_columns(
                pl.from_epoch("timeStamp", time_unit="s").alias("datetime")
            )

            # Update current_block to the last block number in the DataFrame
            last_block = int(df['blockNumber'].min())  # - 1
            
            # Define the CSV file name based on the block range
            csv_file = f'transactions/{current_block}_{last_block}.csv'

            # Save the DataFrame to a CSV file
            df.write_csv(csv_file)
            print(f"Data successfully written to {csv_file}")

            current_block = last_block
            current_ts = int(df['timeStamp'].max())

        else:
            print("Error fetching data:", data['result'])
            break

        # Be respectful and avoid hitting rate limits
        time.sleep(1)


# Define the parameters
token_address = '0xdac17f958d2ee523a2206206994597c13d831ec7'  # USDT (Tether)
end_date = '2025-01-11'  # Replace with the actual end block number if needed

fetch_and_save_transactions(token_address, end_date)
