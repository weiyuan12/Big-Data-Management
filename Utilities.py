import math
import statistics

def compute_min_price(data):
    """
    Compute the minimum resale price.
    """
    return round(min(price for price, _ in data), 2)


def compute_avg_price(data):
    """
    Compute the average resale price.
    """
    prices = [price for price, _ in data]
    return round(sum(prices) / len(prices) if prices else 0, 2)


def compute_std_dev_price(data):
    """
    Compute the standard deviation of the resale price.
    """
    prices = [price for price, _ in data]
    if not prices:
        return 0
    # avg = compute_avg_price(data)
    # variance = sum((p - avg) ** 2 for p in prices) / len(prices)
    # return round(math.sqrt(variance), 2)

    return round(statistics.stdev(prices), 2)


def compute_min_price_per_sqm(data):
    """
    Compute the minimum price per square meter.
    """
    return round(min(price / area for price, area in data if area > 0), 2)

