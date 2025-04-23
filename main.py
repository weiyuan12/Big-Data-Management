import time
import re
import gc
import os
import pandas as pd
from Query import Query
from QueryProcessing import QueryProcessing
from ColumnStore import ColumnStore
from Utilities import (
    compute_avg_price,
    compute_min_price,
    compute_std_dev_price,
    compute_min_price_per_sqm
)


def run_query_pipeline(query: Query, store: ColumnStore):
    # Execute Query
    start_time = time.time()
    queryProcessor = QueryProcessing(query, store)
    result = queryProcessor.filterByPeriod(useZoneMap=True).filterByTown().filterByArea().reconstructTuple()
    metrics = (
        compute_min_price(result),
        compute_avg_price(result),
        compute_std_dev_price(result),
        compute_min_price_per_sqm(result)
    )
    elapsed_time = time.time() - start_time

    # Generate Output
    year, month = query.START_MONTH.split("-")
    town = query.TOWN.upper()
    matric = query.lastDigits.upper()

    result_df = pd.DataFrame(columns=["Year", "Month", "Town", "Category", "Value"])
    result_df.loc[0] = [year, month, town, "Minimum Price", f"{metrics[0]:.2f}"]
    result_df.loc[1] = [year, month, town, "Average Price", f"{metrics[1]:.2f}"]
    result_df.loc[2] = [year, month, town, "Standard Deviation of Price", f"{metrics[2]:.2f}"]
    result_df.loc[3] = [year, month, town, "Minimum Price per Square Meter", f"{metrics[3]:.2f}"]

    # Display output
    print("Statistical result:")
    print(result_df)

    os.makedirs("results", exist_ok=True)
    filename = f"ScanResult_{matric}.csv"
    result_df.to_csv(os.path.join("results", filename), index=False)
    print(f"\nResults written to result/{filename}")
    print(f"Time Taken: {elapsed_time:.4f} seconds\n\n")



def run_experiment(query: Query, store: ColumnStore):
    def run(name, processor_fn):
        print(f"{name} Query Pipeline")
        start_time = time.time()

        queryProcessor = QueryProcessing(query, store)
        result = processor_fn(queryProcessor)
        metrics = (
            compute_min_price(result),
            compute_avg_price(result),
            compute_std_dev_price(result),
            compute_min_price_per_sqm(result)
        )

        elapsed_time = time.time() - start_time
        print(f"Query Result ({name}):", *metrics)
        print(f"Time Taken: {elapsed_time:.4f} seconds\n\n")

    # Run all pipeline with different optimisation strategies
    run("Base", lambda qp: qp.filterByPeriod().filterByTown().filterByArea().reconstructTuple())
    run("SharedScan", lambda qp: qp.filterBySharedScan().reconstructTuple())
    run("ZoneMap", lambda qp: qp.filterByPeriod(useZoneMap=True).filterByTown().filterByArea().reconstructTuple())
    run("BitMap", lambda qp: qp.filterByPeriod().filterByTownWithBitMap().filterByArea().reconstructTuple())
    run("ZoneMap + SharedScan", lambda qp: qp.filterByPeriod(useZoneMap=True).filterBySharedScanWithZoneMap().reconstructTuple())


def main():
    # Initialise Column Store
    csv_file = "data/ResalePricesSingapore.csv"
    store = ColumnStore(csv_file)
    store.extract_and_store()

    while True:
        # Prompt mode selection
        print("\n======== COLUMN STORE ========")
        print("Select Mode:")
        print("1. Run Normal Query")
        print("2. Run Experiment (benchmark multiple pipelines)")
        print("Type 'exit' to quit.\n")
        mode = input("Enter your choice (1/2): ").strip().lower()

        if mode in ['exit', 'q', 'quit']:
            print("Exiting...")
            break

        if mode not in ['1', '2']:
            print("Invalid selection. Please enter '1' or '2'.")
            continue

        # Prompt for matric number
        print("\nAvailable Matric Numbers:")
        print(" - 841G")
        print(" - 392J")
        print("Or enter a custom matric number (format: 3 digits + 1 letter, e.g., 123A)")
        print("Type 'exit' to go back.\n")

        matric = input("Enter matriculation number: ").strip().upper()
        if matric in ['EXIT', 'Q', 'QUIT']:
            continue

        if not re.fullmatch(r"\d{3}[A-Z]", matric):
            print("Invalid format. Please enter in the format ###X (e.g., 123A).")
            continue

        # Create Query object and run selected function
        query = Query(matric)
        if mode == '1':
            run_query_pipeline(query, store)
        elif mode == '2':
            run_experiment(query, store)

        gc.collect()

    # while True:   
    #     # Prompt matric number
    #     print("Available Matric Numbers:")
    #     print("1. 841G")
    #     print("2. 392J")
    #     print("Or enter a custom matric number (format: 3 digits + 1 letter, e.g., 123A)")
    #     print("Type 'exit' to quit.\n")
    #     matric = input("Enter matriculation number: ").strip().upper()

    #     if matric in ['EXIT', 'Q', 'QUIT']:
    #         print("Exiting...")
    #         break

    #     if not re.fullmatch(r"\d{3}[A-Z]", matric):
    #         print("Invalid format. Please enter in the format ###X (e.g., 123A).")
    #         continue
        
    #     # Run Query
    #     query = Query(matric)
    #     run_query_pipeline(query, store)
    #     gc.collect()


if __name__ == "__main__":
    main()