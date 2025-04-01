import pandas as pd
import struct
import os

class ColumnStore:
    def __init__(self, csv_file, store_dir="ColumnStore"):
        """
        Initialize Column Store object with the provided csv file

        Args:
        - csv_file: Path to the CSV file containing the data.
        - store_dir: Directory where the column data will be stored.
        """
        
        self.csv_file = csv_file
        self.store_dir = store_dir
        self.expected_data_types = {
            'month': str,
            'town': str,
            'flat_type': str,
            'block': str,
            'street_name': str,
            'storey_range': str,
            'floor_area_sqm': float,
            'flat_model': str,
            'lease_commence_date': int,
            'resale_price': float
        }

        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir)

    def extract_and_store(self):
        """
        Reads CSV and stores data in separate binary files per column.
        
        Args:
        - None
        """
        df = pd.read_csv(self.csv_file)

        for column_name in df.columns:
            column_data = df[column_name].fillna("@#*NULL@#*")
            expected_type = self.expected_data_types.get(column_name, str) 
            file_path = os.path.join(self.store_dir, f"{column_name}.store")

            with open(file_path, 'wb') as f:
                for value in column_data:
                    if expected_type == str:
                        f.write(value.encode('utf-8')[:50].ljust(50, b'\x00'))  # Fixed 50-byte strings
                    elif expected_type == int:
                        f.write(struct.pack('i', int(value))) # Store as 4-byte integer
                    elif expected_type == float:
                        f.write(struct.pack('d', float(value)))  # Store as 8-byte double

    def load_column(self, column_name, positions=None):
        """
        Loads a column from its binary file.
        
        Args:
        - column_name: The column name to be loaded
        - positions: Optional list of integer indices to filter data

        Returns:
        - A list of values (all or filtered by positions)
        """

        file_path = os.path.join(self.store_dir, f"{column_name}.store")
        data = []

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Column '{column_name}' not found in storage.")

        expected_type = self.expected_data_types.get(column_name, str)  # Default to string

        if expected_type == str:
            record_size = 50
            unpack_fn = lambda b: b.decode('utf-8').strip('\x00')
        elif expected_type == int:
            record_size = 4
            unpack_fn = lambda b: struct.unpack('i', b)[0]
        elif expected_type == float:
            record_size = 8
            unpack_fn = lambda b: struct.unpack('d', b)[0]

        with open(file_path, 'rb') as f:
            # Read Whole Column
            if positions is None:
                while True:
                    chunk = f.read(record_size)
                    if not chunk:
                        break
                    data.append(unpack_fn(chunk))

            # Read only specific positions
            else:
                for pos in positions:
                    f.seek(pos * record_size)
                    chunk = f.read(record_size)
                    if not chunk:
                        break
                    data.append(unpack_fn(chunk))

        return data


    def query(self, column_name, condition=lambda x: True):
        """Queries a specific column with a condition."""
        data = self.load_column(column_name)
        return [val for val in data if condition(val)]



# # Example Usage
# csv_file = "data/ResalePricesSingapore.csv"
# store = ColumnStore(csv_file)

# # Extract and store data as binary
# store.extract_and_store()

# # Load and print first 10 towns
# towns = store.load_column("town")
# print("First 10 towns:", towns[:10])

# towns_filtered = store.load_column("town", [0, 100, 1000, 10000])
# print("Fetch specific positions", towns_filtered)

# # Query resale prices greater than 300,000
# high_prices = store.query("resale_price", lambda x: x > 300000)
# print("High resale prices:", high_prices[:10])
