from Query import Query
from QueryProcessing import QueryProcessing
from ColumnStore import ColumnStore

choice = 0
matric = "841G"
query = Query(matric)

# Initialise Column Store
csv_file = "data/ResalePricesSingapore.csv"
store = ColumnStore(csv_file)
store.extract_and_store()

res = []
while True:
    print("------------------------------------------------")
    print(f"Current Matric No: {matric}")
    print("Select Option \n1. Min Value\n2.Average Value\n3.Standard Deviation\n4.Min Price per Sqm\n5.Display results\n6.Change Matric No\n7.Exit\n")
    choice = int(input("Selection: "))
    if choice == 5:
        print("Year,Month,Town,Category,Value")
        for i in res:
            print(f"{i['Year']},{i['Month']}, {i['Town']}, {i['Category']}, {i['Value']}")
        res = {}
        print("-----------------Clearing Result ---------------------------")
    elif choice in [1,2,3,4]:
        queryProcessor = QueryProcessing(query, storage)
        year, month, town, category, result = queryProcessor.getResult(choice)
        res.append({"Year" : year, "Month" : month, "Town": town, "Category": category, "Value": result})
    elif choice == 6:
        matric = input("Enter Matric :")
        query = Query(matric)
    elif choice == 7:
        break
    else:
        print("Invalid")

