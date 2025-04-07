from credentials import *

def get_s3_conn():
    s3_conn = {
        "spark.hadoop.fs.s3.access.key": AWS_S3_ACCESS_KEY_ID,
        "spark.hadoop.fs.s3.secret.access": AWS_S3_SECRET_ACCESS_KEY,
        "spark.hadoop.fs.s3.region": AWS_S3_REGION
    }

    return s3_conn

def get_redshift_driver_path():
    return AWS_REDSHIFT_DRIVER_PATH

def get_redshift_conn():
    redshift_conn = {
        "url": AWS_REDSHIFT_CONN_URL,
        "user": AWS_REDSHIFT_USER,
        "password": AWS_REDSHIFT_PASSWORD,
        "driver": AWS_REDSHIFT_DRIVER
    }

    return redshift_conn