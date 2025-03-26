from Query import Query
from InMemoryColumnStore import InMemoryColumnStore
import numpy as np
class QueryProcessing: 

    def __init__(self, query: Query, storage : InMemoryColumnStore):
        print("-----------PROCESSING QUERY----------------")
        pos_1 = self.getFirstResultCol(storage, query.START_MONTH, query.END_MONTH)
        pos_2 = self.getSecondResultCol(pos_1, storage, query.TOWN)
        pos_3 = self.getThirdResultCol(pos_2, storage, query.AREA)
        self.operationOnFinalResult(pos_3, storage)
        pass

    def getFirstResultCol (self, storage : InMemoryColumnStore, start_month, end_month):
        print(f"[GETTING RESULTS BETWEEN {start_month} AND {end_month}]")
        result = []
        for idx in range(storage.DATA_SIZE):
            if storage.DATA["month"][idx] == start_month or storage.DATA["month"][idx] == end_month:
                result.append(idx)
        print("Positions found", len(result))
        return result

    def getSecondResultCol (self, pos_1, storage: InMemoryColumnStore, town):
        print(f"[GETTING RESULTS FOR TOWN {town}]")
        result = []
        for idx in pos_1:
            if storage.DATA['town'][idx] == town:
                result.append(idx)
        print("Positions found", len(result))
        return result

    def getThirdResultCol (self, pos_2, storage: InMemoryColumnStore, area):
        print(f"[GETTING RESULTS FOR FLOOR AREA {area}]")
        result = []
        for idx in pos_2:
            if storage.DATA['area'][idx] >= area:
                result.append(idx)
        print("Positions found", len(result))
        return result
    
    def operationOnFinalResult(self,pos_3, storage: InMemoryColumnStore):
        result = {
            "min_price": "No Result",
            "avg_price": "No Result",
            "std_price": "No Result",
            "min_price_per_sqm": "No Result"
        }
        if len(pos_3) > 0:
            price_data = np.array(storage.DATA["price"])[pos_3]
            area_data = np.array(storage.DATA["area"])[pos_3]
            price_per_area = price_data / area_data  
            result["min_price"] = price_data.min()
            result["avg_price"] = price_data.mean()
            result["std_price"] = price_data.std()
            result["min_price_per_sqm"] = price_per_area.min()
            
        print(result)
        return result
        

