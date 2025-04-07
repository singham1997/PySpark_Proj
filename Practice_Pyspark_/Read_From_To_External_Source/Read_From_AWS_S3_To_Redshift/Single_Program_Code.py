import os
import logging

from pyspark import SparkConf
from pyspark.sql.connect.session import SparkSession

from connections import *

APP_NAME = "READ_FROM_AWS_S3_WRITE_TO_REDSHIFT"
ENVIRONMENT = os.environ.get('environ')

def main():
    spark = None

    # Get Connections
    s3_conn = get_s3_conn()
    redshift_conn = get_redshift_conn()
    redshift_driver_path = get_redshift_driver_path()

    try:
        # Set Spark Config
        conf = SparkConf()

        conf.setAppName(APP_NAME)
        master = "local[*]" if ENVIRONMENT=='DEV' else "yarn"

        conf.setMaster(master)

        # Set Spark Session
        spark = SparkSession \
            .builder \
            .config(conf=conf) \
            .config(**s3_conn) \
            .getOrCreate()

        # Read data from AWS S3 Bucket
        s3_bucket_name = "your-bucket-name"
        s3_object = "your-object"
        s3_file_path = f"s3://{s3_bucket_name}/{s3_object}/"

        options = {
            "path": s3_file_path
        }

        read_format = "csv"

        read_s3_data_df = spark \
            .read \
            .format(read_format) \
            .options(**options) \
            .load()

        # Performing transformations
        transform_data_df = read_s3_data_df.select('id', 'name')

        # Write data to Redshift
        write_format = "jdbc"
        write_mode = ""

        transform_data_df \
            .write \
            .format(write_format) \
            .mode(write_mode) \
            .options(**redshift_conn) \
            .saveAsTable("my_database.my_table")
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()


if __name__ == '__main__':
    main()