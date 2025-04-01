import math

# def reconstruct_tuples(store, positions, columns):
#     """
#     Given a list of positions and column names, reconstruct a list of tuples for those columns.

#     Args:
#     - store: instance of ColumnStore
#     - positions: list of integer indices
#     - columns: list of column names to fetch (e.g., ['resale_price', 'floor_area_sqm'])

#     Returns:
#     - List of tuples: e.g., [(price1, area1), (price2, area2), ...]
#     """
#     column_data = [store.load_column(col, positions) for col in columns]
#     return list(zip(*column_data))  # Transpose the list of lists


def compute_min_price(data):
    """
    Compute the minimum resale price.
    """
    return min(price for price, _ in data)


def compute_avg_price(data):
    """
    Compute the average resale price.
    """
    prices = [price for price, _ in data]
    return sum(prices) / len(prices) if prices else 0


def compute_std_dev_price(data):
    """
    Compute the standard deviation of the resale price.
    """
    prices = [price for price, _ in data]
    if not prices:
        return 0
    avg = compute_avg_price(data)
    variance = sum((p - avg) ** 2 for p in prices) / len(prices)
    return math.sqrt(variance)


def compute_min_price_per_sqm(data):
    """
    Compute the minimum price per square meter.
    """
    return min(price / area for price, area in data if area > 0)
