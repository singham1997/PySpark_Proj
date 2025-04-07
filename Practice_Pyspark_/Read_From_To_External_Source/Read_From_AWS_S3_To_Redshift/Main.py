import os
import logging


from connections import *

from ..utils import get_spark_session
from ..read_df import read_data_from_external_source
from ..write_df import write_data_to_external_source_table
from transform_df import transform

APP_NAME = "READ_FROM_AWS_S3_WRITE_TO_REDSHIFT"
ENVIRONMENT = os.environ.get('environ')

def main():
    spark = None

    redshift_driver_path = get_redshift_driver_path()
    s3_conn = get_s3_conn()
    redshift_conn = get_redshift_conn()

    try:
        # Set additional config
        additional_config = {
            "spark.jars": redshift_driver_path
        }

        # Get spark session
        spark = get_spark_session(APP_NAME, ENVIRONMENT, **additional_config, **s3_conn)

        # Read data from AWS S3
        read_format = "csv"
        bucket_name = "users"
        file_name = "file.csv"
        s3_file_path = f"s3a://{bucket_name}/{file_name}"

        options = {
            "path": s3_file_path
        }

        users_data_df = read_data_from_external_source(spark, read_format, **options)

        # Perform transformations
        transformed_users_data_df = transform(users_data_df)

        # write data to AWS Redshift
        write_format = "jdbc"
        write_mode = "OVERWRITE"
        redshift_table_name = "mydatabase.tablename"

        write_data_to_external_source_table(transformed_users_data_df, write_format, write_mode, redshift_table_name,**redshift_conn)
    except Exception as e:
        logging.error(f"Running program is giving exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    main()