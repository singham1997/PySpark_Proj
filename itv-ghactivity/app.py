import os

from read import from_files
from process import transform
from write import to_files
from util import get_spark_session

def main():
    spark = None
    try:
        env = os.environ.get('ENVIRON')
        src_dir = os.environ.get('SRC_DIR')
        file_pattern = f"{os.environ.get('SRC_FILE_PATTERN')}-*"
        src_file_format = os.environ.get('SRC_FILE_FORMAT')
        tgt_dir = os.environ.get('TGT_DIR')
        tgt_file_format = os.environ.get('TGT_FILE_FORMAT')

        if env=='DEV':
            spark = get_spark_session(environment='local[*]', appname='GitHub Activity - Getting Started')
        else:
            spark = get_spark_session(environment='yarn', appname='GitHub Activity - Getting Started')

        # spark.sql('SELECT current_date').show()

        # Read Files
        df = from_files(spark, src_dir, file_pattern, src_file_format)

        # Process Files
        df_transformed = transform(df)
        df_transformed.printSchema()
        df_transformed.select('repo.*', 'year', 'month', 'day').show()

        # Write Files
        to_files(df_transformed, tgt_dir, tgt_file_format)
    except Exception as e:
        print(f"Running program has an exception: {str(e)}")
    finally:
        spark.stop()

if __name__ == '__main__':
    main()


