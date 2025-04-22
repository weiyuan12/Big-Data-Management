from Query import Query
from ColumnStore import ColumnStore
import numpy as np

'''
The QueryProcessing class provides methods to execute the query on different columns. 
It importantly stores the filtered positions after each filter
'''
class QueryProcessing:
    def __init__(self, query: Query, store: ColumnStore):
        self.query = query
        self.store = store
        self.filtered_positions = list(range(len(store.load_column("resale_price"))))
    
    '''
    Filters by the START and EMD date, and stores results in self.filtered positions
    Optional param useZoneMap to toggle ZoneMap optimization
    '''
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

        print("Number of positions after datetime Filter:", len(self.filtered_positions))
        return self
    
    '''
    Filters by TOWN using BitMap optimization, and stores results in self.filtered positions
    '''
    def filterByTownWithBitMap(self):
        town = self.query.TOWN
        bitmap = self.store.load_bitmap(town)
        filtered_mask = np.zeros_like(bitmap, dtype=bool)
        filtered_mask[self.filtered_positions] = True

        combined_mask = bitmap & filtered_mask
        self.filtered_positions = np.flatnonzero(combined_mask)
        print("Number of positions After town bitmap filter:", len(self.filtered_positions))
        return self
    
    '''
    Filters by TOWN without BitMap optimization, and stores results in self.filtered positions
    '''
    def filterByTown(self):
        town = self.query.TOWN
        towns = self.store.load_column("town", self.filtered_positions)

        self.filtered_positions = [
            pos for pos, t in towns
            if t.lower() == town.lower()
        ]

        print("Number of positions after town filter:", len(self.filtered_positions))
        return self
    
    '''
    Filters by Floor Area, and stores results in self.filtered positions
    '''
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

        print("Number of positions after area filter:",len(self.filtered_positions))
        return self
    
    '''
    Executes Shared Scan on all columns and stores results in self.filtered positions
    '''
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
    
    '''
    Executes Shared Scan on all columns with ZoneMap Optimization and stores results in self.filtered positions
    '''
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
    
    '''
    Reconstruct a list of tuples for results calculation.
    '''
    def reconstructTuple(self):
        columns = ["resale_price", "floor_area_sqm"]
        column_data = [
            [value for _, value in self.store.load_column(col, self.filtered_positions)]
            for col in columns
        ]

        return list(zip(*column_data))  # Transpose the list of lists
    

        

