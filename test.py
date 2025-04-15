import time
from Query import Query
from QueryProcessing import QueryProcessing
from ColumnStore import ColumnStore
from Utilities import (
    compute_avg_price,
    compute_min_price,
    compute_std_dev_price,
    compute_min_price_per_sqm
)

choice = 0
matric = "392J"
## MATRICULATION NUMBER ##
## WEIYUAN = 841G
## JIAYUN = 392J
query = Query(matric)

# Initialise Column Store
csv_file = "data/ResalePricesSingapore.csv"
store = ColumnStore(csv_file)
store.extract_and_store()


##### BASE PIPELINE #####
print("Base Query Pipeline")
start_time = time.time()

# Query Processor
queryProcessor = QueryProcessing(query, store)
queryProcessor.filterByPeriod()
queryProcessor.filterByTown()
queryProcessor.filterByArea()
result = queryProcessor.reconstructTuple()


# Metrics Utilities
minPrice = compute_min_price(result)
avgPrice = compute_avg_price(result)
stdDevPrice = compute_std_dev_price(result)
minPricePerSqm = compute_min_price_per_sqm(result)

end_time = time.time()
elapsed_time = end_time - start_time

print("Query Result (Base): ", minPrice, avgPrice, stdDevPrice, minPricePerSqm)
print(f"Time Taken: {elapsed_time:.4f} seconds\n\n")



##### SharedScan PIPELINE #####
print("SharedScan Query Pipeline")
start_time = time.time()

# Query Processor
queryProcessor = QueryProcessing(query, store)
queryProcessor.filterBySharedScan()
result = queryProcessor.reconstructTuple()

# Metrics Utilities
minPrice = compute_min_price(result)
avgPrice = compute_avg_price(result)
stdDevPrice = compute_std_dev_price(result)
minPricePerSqm = compute_min_price_per_sqm(result)

end_time = time.time()
elapsed_time = end_time - start_time

print("Query Result (ZoneMap): ", minPrice, avgPrice, stdDevPrice, minPricePerSqm)
print(f"Time Taken: {elapsed_time:.4f} seconds\n\n")




##### ZoneMap PIPELINE #####
print("ZoneMap Query Pipeline")
start_time = time.time()

# Query Processor
queryProcessor = QueryProcessing(query, store)
queryProcessor.filterByPeriod(useZoneMap=True)
queryProcessor.filterByTown()
queryProcessor.filterByArea()
result = queryProcessor.reconstructTuple()

# Metrics Utilities
minPrice = compute_min_price(result)
avgPrice = compute_avg_price(result)
stdDevPrice = compute_std_dev_price(result)
minPricePerSqm = compute_min_price_per_sqm(result)

end_time = time.time()
elapsed_time = end_time - start_time

print("Query Result (ZoneMap): ", minPrice, avgPrice, stdDevPrice, minPricePerSqm)
print(f"Time Taken: {elapsed_time:.4f} seconds\n\n")

##### Bitmap PIPELINE #####
print("BitMap Query Pipeline")
start_time = time.time()

# Query Processor
queryProcessor = QueryProcessing(query, store)
queryProcessor.filterByPeriod()
queryProcessor.filterByTownWithBitMap()
queryProcessor.filterByArea()
result = queryProcessor.reconstructTuple()

# Metrics Utilities
minPrice = compute_min_price(result)
avgPrice = compute_avg_price(result)
stdDevPrice = compute_std_dev_price(result)
minPricePerSqm = compute_min_price_per_sqm(result)

end_time = time.time()
elapsed_time = end_time - start_time

print("Query Result (ZoneMap): ", minPrice, avgPrice, stdDevPrice, minPricePerSqm)
print(f"Time Taken: {elapsed_time:.4f} seconds\n\n")



##### ZoneMap + SharedScan PIPELINE #####
print("ZoneMap + SharedScan Query Pipeline")
start_time = time.time()

# Query Processor
queryProcessor = QueryProcessing(query, store)
queryProcessor.filterByPeriod(useZoneMap=True)
queryProcessor.filterBySharedScanWithZoneMap()
result = queryProcessor.reconstructTuple()

# Metrics Utilities
minPrice = compute_min_price(result)
avgPrice = compute_avg_price(result)
stdDevPrice = compute_std_dev_price(result)
minPricePerSqm = compute_min_price_per_sqm(result)

end_time = time.time()
elapsed_time = end_time - start_time

print("Query Result (ZoneMap): ", minPrice, avgPrice, stdDevPrice, minPricePerSqm)
print(f"Time Taken: {elapsed_time:.4f} seconds\n\n")