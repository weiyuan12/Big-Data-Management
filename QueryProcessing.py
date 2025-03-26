from Query import Query
from InMemoryColumnStore import InMemoryColumnStore
import numpy as np
class QueryProcessing: 

    def __init__(self, query: Query, storage : InMemoryColumnStore):
        self.query = query
        self.storage = storage
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
    
    def getResult (self, choice):
        category = {
            1: "Min Price",
            2: "Average Price",
            3: "Standard Deviation",
            4: "Min Price per SqM"
        }
        print("-----------PROCESSING QUERY----------------")
        pos_1 = self.getFirstResultCol(self.storage, self.query.START_MONTH, self.query.END_MONTH)
        pos_2 = self.getSecondResultCol(pos_1, self.storage, self.query.TOWN)
        pos_3 = self.getThirdResultCol(pos_2, self.storage, self.query.AREA)
        if len(pos_3) == 0:
            return "No Result"
        if choice == 1:
            price_data = np.array(self.storage.DATA["price"])[pos_3]
            print("Result: ", price_data.min(), " Saved")
            return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[1] ,price_data.min()
        elif choice == 2:
            price_data = np.array(self.storage.DATA["price"])[pos_3]
            print("Result: ", price_data.mean(), " Saved")
            return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[2] ,price_data.mean()
        elif choice == 3:
            price_data = np.array(self.storage.DATA["price"])[pos_3]
            print("Result: ", price_data.std(), " Saved")
            return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[3] ,price_data.std()
        elif choice == 4:
            price_data = np.array(self.storage.DATA["price"])[pos_3]
            area_data = np.array(self.storage.DATA["area"])[pos_3]
            price_per_area = price_data / area_data 
            print("Result: ", price_per_area.min(), " Saved")
            return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[4] ,price_per_area.min()



        

