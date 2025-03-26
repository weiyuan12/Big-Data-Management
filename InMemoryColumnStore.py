import csv
class InMemoryColumnStore:
    
    def __init__ (self, filepath):
        self.DATA_SIZE = 0
        self.DATA = {
            "month" : [],
            "town": [],
            "price": [],
            "area": []
        }
        self.analyse_data(filepath)

    def analyse_data(self,filepath):
        filename = open(filepath, 'r')
        file = csv.DictReader(filename)
        
        print("---------READING DATA TO IN MEMORY STORE -----------")
        for row in file:
            self.DATA["month"].append(row["month"])
            self.DATA["town"].append(row["town"])
            self.DATA["price"].append(float(row["resale_price"]))
            self.DATA["area"].append(float(row["floor_area_sqm"]))
            self.DATA_SIZE +=1
        
        print("length: ",  self.DATA_SIZE)



    

