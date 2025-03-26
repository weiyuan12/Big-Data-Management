from InMemoryColumnStore import InMemoryColumnStore
from Query import Query
from QueryProcessing import QueryProcessing
import pandas as pd


storage = InMemoryColumnStore("./data/ResalePricesSingapore.csv")
query = Query("841G")
queryProcessor = QueryProcessing(query, storage)
