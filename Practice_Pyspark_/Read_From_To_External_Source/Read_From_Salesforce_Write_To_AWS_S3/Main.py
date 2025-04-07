import os

from connection import *
from credentials import *

from ..utils import get_spark_session
from ..read_df import read_data_from_external_source
from transform_df import transform
from ..write_df import write_data_to_external_source

APP_NAME = "SalesForce_To_S3"
ENVIRONMENT = os.environ.get("environ")

def main():
    spark = None

    # Config
    s3_conn = get_s3_conn()

    # Read MSSQL data using spark
    table_name = "Users"
    salesforce_conn = get_salesforce_conn(table_name)

    try:
        # Get Spark Session
        conf = {
            "spark.jars": SALESFORCE_JDBC_DRIVER_PATH
        }

        spark = get_spark_session(APP_NAME, ENVIRONMENT, **conf, **s3_conn)

        # Get Spark Read
        read_format = "salesforce"
        users_data_df = read_data_from_external_source(spark, read_format, **salesforce_conn)

        # Transformations
        transform_users_data_df = transform(users_data_df)

        # Write the data from spark to AWS S3
        write_format = "parquet"
        bucket_name = "your_bucket_name"
        object_name = "salesforce_data"
        mode_type = "OVERWRITE"

        s3_file_path = f"s3a://{bucket_name}/{object_name}/"

        options = {
            "path": s3_file_path
        }

        write_data_to_external_source(transform_users_data_df, write_format, mode_type, **options)
    except Exception as e:
        print(f"Error reading data from S3: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    main()