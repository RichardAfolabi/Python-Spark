import os
import sys

# Path for Spark source folder
os.environ['SPARK_HOME'] = "/Users/RichardAfolabi/spark/"

# Append pySpark to Python path
sys.path.append("/Users/RichardAfolabi/spark/python/")
sys.path.append("/Users/RichardAfolabi/spark/python/lib/")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print("Successfully imported Spark modules")

except ImportError as e:
    print("Cannot import Spark modules", e)
    sys.exit(1)

