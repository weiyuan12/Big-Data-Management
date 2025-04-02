import pandas as pd
import struct
import os

class ColumnStore:
    def __init__(self, csv_file, store_dir="ColumnStore", map_dir = "ZoneMap"):
        """
        Initialize Column Store object with the provided csv file

        Args:
        - csv_file: Path to the CSV file containing the data.
        - store_dir: Directory where the column data will be stored.
        """
        self.ZONE_SIZE = 1000
        self.csv_file = csv_file
        self.store_dir = store_dir
        self.map_dir = map_dir
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
        if not os.path.exists(self.map_dir):
            os.makedirs(self.map_dir)

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
            
            zone_map = []
            idx = 0
            with open(file_path, 'wb') as f:
                temp_block = []
                for value in column_data:
                    if expected_type == str:
                        f.write(value.encode('utf-8')[:50].ljust(50, b'\x00'))  # Fixed 50-byte strings
                    elif expected_type == int:
                        f.write(struct.pack('i', int(value))) # Store as 4-byte integer
                    elif expected_type == float:
                        f.write(struct.pack('d', float(value)))  # Store as 8-byte double
                    
                    if column_name == "month" or column_name =="floor_area_sqm" or column_name =="resale_price":
                        temp_block.append(value)
                        idx+=1
                        if idx % self.ZONE_SIZE ==0:
                            zone_map.append((min(temp_block), max(temp_block)))
                            temp_block = []
                if len(temp_block) > 0:
                    zone_map.append((min(temp_block), max(temp_block)))
            if column_name == "month" or column_name =="floor_area_sqm" or column_name =="resale_price":
                map_path = os.path.join(self.map_dir, f"{column_name}.zonemap")
                with open(map_path, 'wb') as zm:
                    if expected_type == int:
                        for min_val, max_val in zone_map:
                            zm.write(struct.pack('ii', min_val, max_val))
                    elif expected_type == float:
                        for min_val, max_val in zone_map:
                            zm.write(struct.pack('dd', min_val, max_val))
                    elif expected_type == str:
                        for min_val, max_val in zone_map:
                            zm.write(min_val.encode('utf-8')[:50].ljust(50, b'\x00'))
                            zm.write(max_val.encode('utf-8')[:50].ljust(50, b'\x00'))
                    
    def load_zone_map(self, column_name):
        map_path = os.path.join(self.map_dir, f"{column_name}.zonemap")
        if not os.path.exists(map_path):
            return None
        expected_type = self.expected_data_types.get(column_name, str)
        zone_map = []

        with open(map_path, 'rb') as zm:
            if expected_type == str:
                record_size = 100  # 2 x 50 bytes
                while True:
                    chunk = zm.read(record_size)
                    if not chunk:
                        break
                    min_val = chunk[:50].decode('utf-8').rstrip('\x00')
                    max_val = chunk[50:].decode('utf-8').rstrip('\x00')
                    zone_map.append((min_val, max_val))
            
            elif expected_type == int:
                record_size = 8
                while True:
                    chunk = zm.read(record_size)
                    if not chunk:
                        break
                    min_val, max_val = struct.unpack('ii', chunk)
                    zone_map.append((min_val, max_val))
            
            elif expected_type == float:
                record_size = 16
                while True:
                    chunk = zm.read(record_size)
                    if not chunk:
                        break
                    min_val, max_val = struct.unpack('dd', chunk)
                    zone_map.append((min_val, max_val))
        return zone_map
    def load_column(self, column_name, positions=None, filters=None):
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
        zone_map = None
        if positions == None:
            zone_map = self.load_zone_map(column_name)
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

        zones_read = 0
        with open(file_path, 'rb') as f:
            # Read Whole Column
            if positions is None:
                for zone_index, (min_val, max_val) in enumerate(zone_map):
                    # Zone-level filter
                    if filters and not self._might_match(filters, min_val, max_val):
                        continue
                    f.seek(zone_index * self.ZONE_SIZE * record_size)
                    zones_read +=1
                    for i in range(self.ZONE_SIZE):
                        chunk = f.read(record_size)
                        if not chunk:
                            break
                        value = unpack_fn(chunk)
                        data.append((zone_index * self.ZONE_SIZE + i, value))
                print(f"Column: {column_name}, Zones read: {zones_read}, Data Accessed: {zones_read * self.ZONE_SIZE}")
            # Read only specific positions
            else:
                for pos in positions:
                    f.seek(pos * record_size)
                    chunk = f.read(record_size)
                    if not chunk:
                        break
                    data.append((pos, unpack_fn(chunk)))

        return data


    def query(self, column_name, condition=lambda x: True):
        """Queries a specific column with a condition."""
        data = self.load_column(column_name)
        return [val for val in data if condition(val)]
    
    def _might_match(self, filters, min_val, max_val):
        try:
            for cond in filters:
                op = cond["equality"]
                val = cond["value"]

                # Cast val to the same type as min_val/max_val
                if isinstance(min_val, (int, float)):
                    val = type(min_val)(val)  # safe cast
                elif isinstance(min_val, str):
                    val = str(val)

                if op == "==":
                    if min_val <= val <= max_val:
                        return True
                elif op == ">":
                    if max_val > val:
                        return True
                elif op == ">=":
                    if max_val >= val:
                        return True
                elif op == "<":
                    if min_val < val:
                        return True
                elif op == "<=":
                    if min_val <= val:
                        return True
                else:
                    raise ValueError(f"Unsupported operator: {op}")

            return False  # No match
        except Exception:
            return True


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
