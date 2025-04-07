from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_spark_conf(app_name: str, master: str = "local[*]", **kwargs: dict) -> SparkConf:
    spark_conf = SparkConf()

    spark_conf.setAppName(app_name)
    spark_conf.setMaster(master)

    for key, value in kwargs.items():
        spark_conf.set(key, value)

    return spark_conf

def get_spark_session(app_name: str, environment: str = "DEV", **kwargs: dict) -> SparkSession:

    master = "local[*]" if environment=='DEV' else "yarn"

    spark = SparkSession \
        .builder \
        .config(get_spark_conf(app_name, master, **kwargs)) \
        .getOrCreate()

    return spark