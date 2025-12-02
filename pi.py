# pi.py
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0


if __name__ == "__main__":
    """
    Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print(f"Pi is roughly {4.0 * count / n}")
    spark.stop()