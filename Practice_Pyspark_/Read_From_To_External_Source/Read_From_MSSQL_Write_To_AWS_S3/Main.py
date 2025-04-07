import os
import logging

from connections import *

from ..utils import get_spark_session
from ..read_df import read_data_from_external_source
from transform_df import transform
from ..write_df import write_data_to_external_source

APP_NAME = "Read_MSSQL_Write_AWS_S3"
ENVIRON = os.environ.get('environ')

def main():

    spark = None

    mssql_driver_path = get_mssql_driver_path()
    s3_conn = get_aws_s3_connection()
    mssql_conn = get_mssql_connection("users")

    try:
        additional_conf = {
            "spark.jars": mssql_driver_path
        }

        spark = get_spark_session(APP_NAME, ENVIRON, **s3_conn, **additional_conf)

        # Read data from MSSQL
        format = "jdbc"
        user_data_df = read_data_from_external_source(spark, format, **mssql_conn)

        # Perform Transformations
        transformed_data_df = transform(user_data_df)

        # Write data to AWS S3
        bucket_name = "users"
        file_name = "file.parquet"
        format = "parquet"
        mode = "OVERWRITE"

        options = {
            "path": f"s3a://{bucket_name}//{file_name}"
        }

        write_data_to_external_source(transformed_data_df, format, mode, **options)

    except Exception as e:
        logging.error(f"Running program is throwing an exception: {str(e)}")
    finally:
        spark.stop()

if __name__ == '__main__':
    main()