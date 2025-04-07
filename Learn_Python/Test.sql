Syntax -
copy <table_name>
from 's3://<bucket_name>/<object_prefix>'
authorization
{optional parameters};

Query -
copy customer
from 's3://mybucket/external_data/sales'
iam_role 'arn:aws:iam::0123456789012:role/test-redshift-s3-role';

