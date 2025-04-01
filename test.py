from Query import Query
from QueryProcessing import QueryProcessing
from ColumnStore import ColumnStore
from Utilities import (
    reconstruct_tuples,
    compute_avg_price,
    compute_min_price,
    compute_std_dev_price,
    compute_min_price_per_sqm
)

choice = 0
matric = "841G"
query = Query(matric)

# Initialise Column Store
csv_file = "data/ResalePricesSingapore.csv"
store = ColumnStore(csv_file)
store.extract_and_store()

class QueryProcessingTest:
    def __init__(self, query: Query, store: ColumnStore):
        self.query = query
        self.store = store
        self.filtered_positions = list(range(len(store.load_column("resale_price"))))

    def filterByPeriod(self):
        start_month = self.query.START_MONTH
        end_month = self.query.END_MONTH
        months = self.store.load_column("month", self.filtered_positions)

        self.filtered_positions = [
            pos for pos, m in zip(self.filtered_positions, months)
            if (m == start_month or m == end_month)
        ]
        print("Number of positions :",len(self.filtered_positions))
        return self
    
    def filterByTown(self):
        town = self.query.TOWN
        towns = self.store.load_column("town", self.filtered_positions)

        self.filtered_positions = [
            pos for pos, t in zip(self.filtered_positions, towns)
            if t.lower() == town.lower()
        ]
        print("Number of positions :",len(self.filtered_positions))
        return self
    
    def filterByArea(self, min_area=80):
        areas = self.store.load_column("floor_area_sqm", self.filtered_positions)
        self.filtered_positions = [
            pos for pos, area in zip(self.filtered_positions, areas)
            if area >= min_area
        ]
        print("Number of positions :",len(self.filtered_positions))
        return self
    
    def reconstructTuple(self):
        columns = ["resale_price", "floor_area_sqm"]
        column_data = [self.store.load_column(col, self.filtered_positions) for col in columns]

        return list(zip(*column_data))  # Transpose the list of lists

# Query Processor
queryProcessor = QueryProcessingTest(query, store)
queryProcessor.filterByPeriod()
queryProcessor.filterByTown()
queryProcessor.filterByArea()

result = queryProcessor.reconstructTuple()

# Metrics Utilities
minPrice = compute_min_price(result)
avgPrice = compute_avg_price(result)
stdDevPrice = compute_std_dev_price(result)
minPricePerSqm = compute_min_price_per_sqm(result)

print(minPrice, avgPrice, stdDevPrice, minPricePerSqm)