from __future__ import print_function


import sys
from pyspark import SparkContext
from pyspark.sql import SQLContext, Row



def getSqlContextInstance(sparkContext):
    if ('sqlContextSingletonInstance' not in globals()):
        globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
    return globals()['sqlContextSingletonInstance']


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: uberstats <file>", file=sys.stderr)
        exit(-1)
        
    sc = SparkContext(appName="UberStats")
    df = getSqlContextInstance(sc).read.format('com.databricks.spark.csv')\
        .options(header='true', inferschema='true')\
        .load(sys.argv[1])

    df.registerTemplate("uber")

    getSqlContextInstance(sc).sql("""SELECT DISTINCT('dispatching_base_number'),
    SUM('trips') AS cnt FROM uber GROUP BY 'dispatching_base_number'
    ORDER BY cnt DESC """).show()

    sc.stop()
