import polars as pl
import os


def merge_csv_files(directory_path, output_file):
    """
    Merge CSV files in transaction directory into a single CSV file.
    
    Args:
        directory_path (str): Path to directory containing CSV files
        output_file (str): Path where the merged CSV file will be saved
    """
    # List all CSV files in the directory by filtering for .csv extension
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

    # Initialize an empty list to store individual DataFrames
    dataframes = []

    # Define the standard column order to ensure consistency across all files
    standard_columns = [
        'blockNumber',          # Block number where the transaction was recorded
        'timeStamp',            # Unix timestamp of the transaction
        'hash',                 # Transaction hash
        'nonce',                # Number of transactions sent from this address
        'blockHash',            # Hash of the block containing this transaction
        'from',                 # Address of the sender
        'to',                   # Address of the receiver
        'contractAddress',      # Contract address for contract creation transactions
        'value',                # Amount of tokens transferred
        'tokenName',            # Name of the token
        'tokenSymbol',          # Symbol of the token
        'tokenDecimal',         # Number of decimals the token uses
        'transactionIndex',     # Index of the transaction in the block
        'gas',                  # Gas provided for the transaction
        'gasPrice',             # Price per unit of gas
        'gasUsed',              # Amount of gas used by the transaction
        'cumulativeGasUsed',    # Total gas used in the block up to this transaction
        'input',                # Input data for the transaction
        'confirmations',        # Number of confirmations
        'datetime'              # Human-readable datetime
    ]

    # Process each CSV file and add it to the dataframes list
    for file in csv_files:
        print(f'Processing {file}')
        file_path = os.path.join(directory_path, file)
        # Read the CSV file into a Polars DataFrame
        df = pl.read_csv(file_path)
        
        # Ensure all columns are in the standard order
        df = df.select(standard_columns)
        dataframes.append(df)

    # Merge all DataFrames if any exist
    if dataframes:
        # Concatenate all DataFrames vertically
        merged_df = pl.concat(dataframes)

        # Remove any duplicate transactions that might exist across files
        merged_df = merged_df.unique()

        # Save the final merged DataFrame to CSV
        merged_df.write_csv(output_file)
        print(f"Merged data successfully written to {output_file}")
    else:
        print("No CSV files found to merge.")


# Execute the merge function with default paths
merge_csv_files('transactions', 'transactions.csv')
