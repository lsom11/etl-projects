from pyspark import SparkContext, SparkConf, sql
import boto3
from pyspark.sql import Row
from helpers import s3_read

from os import environ

def nhl_spark_job(data):
    conf = SparkConf().setAppName("NHL Spark Job")
    sc = SparkContext(conf=conf)

    tsRDD = sc.parallelize(data)
    lineLengths = tsRDD.map(lambda s: len(s))

    print(lineLengths)

if __name__ == '__main__':
    data = s3_read('lsom11', 'etl-datasets-luc-somers', 'game.csv')
    nhl_spark_job(data)