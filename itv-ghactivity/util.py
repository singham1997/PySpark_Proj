from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_spark_conf(**kwargs) -> SparkConf:
    appname = kwargs['appname']
    environment = kwargs['environment']

    conf = SparkConf()
    conf.setAppName(appname).setMaster(environment)

    return conf

def get_spark_session(**kwargs) -> SparkSession:
    conf = get_spark_conf(**kwargs)
    spark = None

    if kwargs['environment'] == 'DEV':
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
    elif kwargs['environment'] == 'PROD':
        spark = SparkSession.builder.config(conf=conf).getOrCreate()

    return spark

