class Query:
    def __init__(self, lastDigits):
        self.lastDigits = lastDigits
        self.START_MONTH = None
        self.END_MONTH = None
        self.TOWN =None
        self.AREA =80
        
        self.getQueryParams()
        self.print_params()
        pass

    def getQueryParams(self):
        towns = ["BEDOK", "BUKIT PANJANG","CLEMENTI" ,"CHOA CHU KANG", "HOUGANG", "JURONG WEST", "PASIR RIS", "TAMPINES", "WOODLANDS", "YISHUN"]
        year_val = self.lastDigits[-2]
        if int(year_val) >= 4:
            year = "201" + year_val
        else:
            year = "202" + year_val
        month_val = self.lastDigits[-3]
        if month_val == "0":
            month = "10"
            next_month = "11"
        else:
            month = int(month_val)
            next_month = month +1
            if month < 10:
                month = "0" + str(month)
            else:
                month  = str(month)
            if next_month < 10:
                next_month = "0" + str(next_month)
            else:
                next_month  = str(next_month)
        self.START_MONTH = year + "-" + month
        self.END_MONTH = year + "-" + next_month
        townIdx = int(self.lastDigits[-4])
        self.TOWN = towns[townIdx]
    
    def print_params(self):
        print("----------------Printing Params --------------")
        print("Town: ", self.TOWN)
        print("Start month", self.START_MONTH)
        print("Next month", self.END_MONTH)
        
