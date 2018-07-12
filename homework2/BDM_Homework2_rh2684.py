from pyspark import SparkContext
import sys
import csv

def extractRestaurants(partId, records):
    if partId==0:
        next(records)
    reader = csv.reader(records)
    for row in reader:
        (camis, desc) = (row[0], row[7])
        yield (camis, desc)

if _name=='main_':
    sc = SparkContext()
    REST = sys.argv[1]
    rests = sc.textFile(REST, use_unicode=True).cache()
    nyRests = rests.mapPartitionsWithIndex(extractRestaurants)
    nyRests.map(lambda x: (x, 1)).reduceByKey(lambda x,y: 1).keys().map(lambda x: (x[1],1)).reduceByKey(lambda x,y: x+y).top(25, key=lambda x: x[1]).saveAsTextFile(sys.argv[-1])