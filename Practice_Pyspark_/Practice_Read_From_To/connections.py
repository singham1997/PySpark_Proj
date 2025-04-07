from credentials import *

def get_mssql_driver_path():
    return MSSQL_DRIVER_PATH

def get_mssql_connection(tableName: int) -> dict:
    mssql_conn = {
        "url": MSSQL_CLIENT_URL,
        "user": MSSQL_USER,
        "password": MSSQL_PASSWORD,
        "driver": MSSQL_DRIVER,
        "dbtable": tableName
    }
    return mssql_conn

def get_aws_s3_connection() -> dict:
    aws_s3_conn = {
        "spark.hadoop.fs.s3a.access.key": AWS_S3_ACCESS_KEY_ID,
        "spark.hadoop.fs.s3a.secret.access": AWS_S3_SECRET_ACCESS_KEY,
        "spark.hadoop.fs.s3a.region": AWS_S3_REGION
    }
    return aws_s3_conn