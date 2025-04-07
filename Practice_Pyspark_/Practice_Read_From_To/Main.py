from connections import *
from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == '__main__':
    conf = SparkConf().setAppName("Read_From_MSSQL_Write_To_AWS_S3").setMaster("local[*]")

    SparkSession.builder.config(conf=conf) \
        .