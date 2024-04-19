import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import sys
import os

def csv_to_parquet(input_file):
    data_types = {
        'Client ID': 'int64',
        'Transaction ID': 'str',
        'Item ID': 'str',
        'Item Description': 'str',
        'Category': 'str',
        'Quantity': 'int32',
        'Total Amount': 'float64',
        'Credit Card Number': 'str'
    }

    date_columns = ['Transaction Date']
    df = pd.read_csv(input_file, dtype=data_types, parse_dates=date_columns)

    # Debugging output to check data types
    print(df.dtypes)

    # Replace NaN values in 'Total Amount' with zeros
    df['Total Amount'] = df['Total Amount'].fillna(0)

    output_file = os.path.splitext(input_file)[0] + '.parquet'

    # Convert DataFrame to Parquet without explicitly defining the schema
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, output_file, compression='snappy')

    print(f"Converted '{input_file}' to '{output_file}' successfully.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <input_file.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    csv_to_parquet(input_file)
