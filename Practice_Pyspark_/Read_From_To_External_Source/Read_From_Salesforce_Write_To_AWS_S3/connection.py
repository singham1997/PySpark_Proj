from credentials import *

def get_s3_conn():
	s3_conn = {
		"spark.hadoop.fs.s3a.access.key": AWS_S3_ACCESS_KEY_ID,
		"spark.hadoop.fs.s3a.secret.key": AWS_S3_SECRET_ACCESS_KEY,
		"spark.hadoop.fs.s3a.region": AWS_S3_REGION
	}
	return s3_conn

def get_salesforce_conn(table_name):
	salesforce_conn = {
		"url": SALESFORCE_JDBC_CONN_URL,
		"user": SALESFORCE_USER,
		"password": SALESFORCE_PASSWORD,
		"driver": SALESFORCE_JDBC_DRIVER_PATH,
		"objects": table_name

	}
	return salesforce_conn
