"""
What are we doing in the project - 
We have modularize the ETL project.
Extract - read.py
Transform - process.py
Load - write.py

sparkConf, sparkSession creation is done by util.py

The input data is present inside the data folder
Path - data/itv-ghactivity/landing/ghactivity

We are reading the files in json format
Data is read we are processing the data by creating 3
new columns from column 'created_at' 
year, month, day
Data is written to the path in the form of nested buckets
year, month, day

"""

# Create a python virtual environment

    python -m venv itvg-venv
    
    ./itvg-venv/Scripts/activate

# Downloading the data from the gharchive locally

    cd .\data\itv-github\landing\ghactivity\

# Run below command on git bash

from directory -
    
    /c/Users/amisingh35/PycharmProjects/PySparkProjects/itv-ghactivity/data/itv-github/landing/ghactivity


    curl -O https://data.gharchive.org/2021-01-13-0.json.gz
    curl -O https://data.gharchive.org/2021-01-14-0.json.gz
    curl -O https://data.gharchive.org/2021-01-15-0.json.gz

# Copy the files into HDFS landing folders.

    hdfs dfs -mkdir -p /user/${USER}/itv-github/landing/ghactivity
    hdfs dfs -ls /user/${USER}/itv-github/landing/ghactivity
    hdfs dfs -put * /user/${USER}/itv-github/landing/ghactivity/
 
# Validating Files in HDFS

    hdfs dfs -ls /user/${USER}/itv-github/landing/ghactivity
    hdfs dfs -ls /user/${USER}/itv-github/landing/ghactivity|wc -l
    hdfs dfs -du -s -h /user/${USER}/itv-github/landing/ghactivity

# Here are the export statements to set the environment variables.

    export ENVIRON=PROD
    export SRC_DIR=/user/${USER}/itv-github/landing/ghactivity
    export SRC_FILE_FORMAT=json
    export TGT_DIR=/user/${USER}/itv-github/raw/ghactivity
    export TGT_FILE_FORMAT=parquet
     
    export PYSPARK_PYTHON=python3

# Here are the spark submit commands to run application for 3 dates.

    export SRC_FILE_PATTERN=2021-01-13
     
    spark2-submit --master yarn \
        --py-files itv-ghactivity.zip \
        app.py
     
    export SRC_FILE_PATTERN=2021-01-14
     
    spark2-submit --master yarn \
        --py-files itv-ghactivity.zip \
        app.py
     
    export SRC_FILE_PATTERN=2021-01-15
     
    spark2-submit --master yarn \
        --py-files itv-ghactivity.zip \
        app.py

# Check for files in the target location.

    hdfs dfs -find /user/${USER}/itv-github/raw/ghactivity

# We can use pyspark2 --master yarn --conf spark.ui.port=0 to launch Pyspark and run the below code to validate.

    import getpass
    username = getpass.getuser()
     
    src_file_path = f'/user/{username}/itv-github/landing/ghactivity'
    src_df = spark.read.json(src_file_path)
    src_df.printSchema()
    src_df.show()
    src_df.count()
    from pyspark.sql.functions import to_date
    src_df.groupBy(to_date('created_at').alias('created_at')).count().show()
     
    tgt_file_path = f'/user/{username}/itv-github/raw/ghactivity'
    tgt_df = spark.read.parquet(tgt_file_path)
    tgt_df.printSchema()
    tgt_df.show()
    tgt_df.count()
    tgt_df.groupBy('year', 'month', 'day').count().show()

# Validate the pyspark on Jupyter notebook

df = spark \
    .read \
    .parquet('s3://itv-github-emr/prod/raw/ghactivity/')

df.printSchema()

df.show()

df.count()

df.select('created_at').show(20, truncate=False)

df.filter("substr(created_at, 1, 10) == '2025-03-25'").count()

from pyspark.sql.functions import col, lit, expr, count

df \
    .groupby(expr("substr(created_at, 1, 10)").alias("created_at")) \
    .agg(count(lit("*")).alias("activity_count")) \
    .show()

# Running spark job on cluster mode

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --conf "spark.yarn.appMasterEnv.ENVIRON=PROD" \
    --conf "spark.yarn.appMasterEnv.SRC_DIR=s3://itv-amit-github-emr/prod/landing/ghactivity" \
    --conf "spark.yarn.appMasterEnv.SRC_FILE_FORMAT=json" \
    --conf "spark.yarn.appMasterEnv.TGT_DIR=s3://itv-amit-github-emr/prod/raw/ghactivity/" \
    --conf "spark.yarn.appMasterEnv.TGT_FILE_FORMAT=parquet" \
    --conf "spark.yarn.appMasterEnv.SRC_FILE_PATTERN=2025-03-25" \
    --py-files itv-ghactivity.zip
    app.py

# Validate the data

aws s3 ls \
s3://itv-amit-github-emr/prod/raw/ghactivity \
--recursive