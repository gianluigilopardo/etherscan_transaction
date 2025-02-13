import polars as pl
import os


def merge_csv_files(directory_path, output_file):
    """
    Merge CSV files in transaction directory into a single CSV file.
    """
    # List all CSV files in the directory
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]

    # Initialize an empty list to store DataFrames
    dataframes = []

    # Loop through each CSV file and read it into a DataFrame
    for file in csv_files:
        file_path = os.path.join(directory_path, file)
        df = pl.read_csv(file_path)
        dataframes.append(df)

    # Concatenate all DataFrames into a single DataFrame
    if dataframes:
        merged_df = pl.concat(dataframes)

        # Remove duplicate rows
        merged_df = merged_df.unique()

        # Write the merged DataFrame to a CSV file
        merged_df.write_csv(output_file)
        print(f"Merged data successfully written to {output_file}")
    else:
        print("No CSV files found to merge.")


merge_csv_files('transactions', 'transactions.csv')
