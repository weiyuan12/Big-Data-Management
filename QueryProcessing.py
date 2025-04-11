from Query import Query
from ColumnStore import ColumnStore


class QueryProcessing:
    def __init__(self, query: Query, store: ColumnStore):
        self.query = query
        self.store = store
        self.filtered_positions = list(range(len(store.load_column("resale_price"))))

    def filterByPeriod(self, useZoneMap=False):
        start_month = self.query.START_MONTH
        end_month = self.query.END_MONTH

        if useZoneMap:
            months = self.store.load_column_with_zone_map(
                "month", 
                filters=[{"equality": "==", "value": start_month}, {"equality": "==", "value": end_month}]
            )
        else:
            months = self.store.load_column("month", self.filtered_positions)

        self.filtered_positions = [
            pos for pos, m in months
            if m == start_month or m == end_month
        ]

        print("Number of positions :", len(self.filtered_positions))
        return self
    
    def filterByTown(self):
        town = self.query.TOWN
        towns = self.store.load_column("town", self.filtered_positions)

        self.filtered_positions = [
            pos for pos, t in towns
            if t.lower() == town.lower()
        ]

        print("Number of positions :", len(self.filtered_positions))
        return self
    
    def filterByArea(self, min_area=80, useZoneMap=False):
        if useZoneMap:
            areas = self.store.load_column_with_zone_map(
                "floor_area_sqm",
                filters=[{"equality": ">=", "value": min_area}]
            )
        else:
            areas = self.store.load_column("floor_area_sqm", self.filtered_positions)

        self.filtered_positions = [
            pos for pos, area in areas
            if area >= min_area
        ]

        print("Number of positions :",len(self.filtered_positions))
        return self
    

    def filterBySharedScan(self, min_area=80):
        start_month = self.query.START_MONTH
        end_month = self.query.END_MONTH
        town = self.query.TOWN

        # Load all required columns
        months = self.store.load_column("month", self.filtered_positions)
        towns = self.store.load_column("town", self.filtered_positions)
        areas = self.store.load_column("floor_area_sqm", self.filtered_positions)

        self.filtered_positions = [
            pos for (pos, m), (_, t), (_, a) in zip(months, towns, areas)
            if (
                (m == start_month or m == end_month) 
                and t.lower() == town.lower()
                and a >= min_area
            )
        ]

        print("Number of positions after shared scan : ", len(self.filtered_positions))
        return self
    

    def filterBySharedScanWithZoneMap(self, min_area=80):
        town = self.query.TOWN.lower()

        areas = self.store.load_column("floor_area_sqm", self.filtered_positions)
        towns = self.store.load_column("town", self.filtered_positions)

        self.filtered_positions = [
            pos for (pos, t), (_, a) in zip(towns, areas)
            if (
                t.lower() == town.lower()
                and a >= min_area
            )
        ]

        print("Number of positions after shared scan (ZoneMap) :", len(self.filtered_positions))
        return self
    
    def reconstructTuple(self):
        columns = ["resale_price", "floor_area_sqm"]
        column_data = [
            [value for _, value in self.store.load_column(col, self.filtered_positions)]
            for col in columns
        ]


        return list(zip(*column_data))  # Transpose the list of lists
    



# from InMemoryColumnStore import InMemoryColumnStore
# import numpy as np
# class QueryProcessing: 
#     def __init__(self, query: Query, storage : InMemoryColumnStore):
#         self.query = query
#         self.storage = storage
#         pass

#     def getFirstResultCol (self, storage : InMemoryColumnStore, start_month, end_month):
#         print(f"[GETTING RESULTS BETWEEN {start_month} AND {end_month}]")
#         result = []
#         for idx in range(storage.DATA_SIZE):
#             if storage.DATA["month"][idx] == start_month or storage.DATA["month"][idx] == end_month:
#                 result.append(idx)
#         print("Positions found", len(result))
#         return result

#     def getSecondResultCol (self, pos_1, storage: InMemoryColumnStore, town):
#         print(f"[GETTING RESULTS FOR TOWN {town}]")
#         result = []
#         for idx in pos_1:
#             if storage.DATA['town'][idx] == town:
#                 result.append(idx)
#         print("Positions found", len(result))
#         return result

#     def getThirdResultCol (self, pos_2, storage: InMemoryColumnStore, area):
#         print(f"[GETTING RESULTS FOR FLOOR AREA {area}]")
#         result = []
#         for idx in pos_2:
#             if storage.DATA['area'][idx] >= area:
#                 result.append(idx)
#         print("Positions found", len(result))
#         return result
    
#     def operationOnFinalResult(self,pos_3, storage: InMemoryColumnStore):
#         result = {
#             "min_price": "No Result",
#             "avg_price": "No Result",
#             "std_price": "No Result",
#             "min_price_per_sqm": "No Result"
#         }
#         if len(pos_3) > 0:
#             price_data = np.array(storage.DATA["price"])[pos_3]
#             area_data = np.array(storage.DATA["area"])[pos_3]
#             price_per_area = price_data / area_data  
#             result["min_price"] = price_data.min()
#             result["avg_price"] = price_data.mean()
#             result["std_price"] = price_data.std()
#             result["min_price_per_sqm"] = price_per_area.min()
            
#         print(result)
#         return result
    
#     def getResult (self, choice):
#         category = {
#             1: "Min Price",
#             2: "Average Price",
#             3: "Standard Deviation",
#             4: "Min Price per SqM"
#         }
#         print("-----------PROCESSING QUERY----------------")
#         pos_1 = self.getFirstResultCol(self.storage, self.query.START_MONTH, self.query.END_MONTH)
#         pos_2 = self.getSecondResultCol(pos_1, self.storage, self.query.TOWN)
#         pos_3 = self.getThirdResultCol(pos_2, self.storage, self.query.AREA)
#         if len(pos_3) == 0:
#             return "No Result"
#         if choice == 1:
#             price_data = np.array(self.storage.DATA["price"])[pos_3]
#             print("Result: ", price_data.min(), " Saved")
#             return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[1] ,price_data.min()
#         elif choice == 2:
#             price_data = np.array(self.storage.DATA["price"])[pos_3]
#             print("Result: ", price_data.mean(), " Saved")
#             return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[2] ,price_data.mean()
#         elif choice == 3:
#             price_data = np.array(self.storage.DATA["price"])[pos_3]
#             print("Result: ", price_data.std(), " Saved")
#             return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[3] ,price_data.std()
#         elif choice == 4:
#             price_data = np.array(self.storage.DATA["price"])[pos_3]
#             area_data = np.array(self.storage.DATA["area"])[pos_3]
#             price_per_area = price_data / area_data 
#             print("Result: ", price_per_area.min(), " Saved")
#             return self.query.START_MONTH[:4], self.query.START_MONTH[5:], self.query.TOWN,category[4] ,price_per_area.min()



        

