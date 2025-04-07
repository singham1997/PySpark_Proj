import os

from util import get_spark_session

def main():
    spark = None

    try:
        env = os.environ.get('ENVIRON')

        if env == 'DEV':
            spark = get_spark_session(environment='local[*]', appname='Modularizing Spark')
        else:
            spark = get_spark_session(environment='Prod', appname='Modularizing Spark')

        spark.sql('SELECT current_date').show()
    except Exception as e:
        print(f"Running program has an exception: {str(e)}")
    finally:
        spark.stop() if spark is not None else 1

if __name__ == '__main__':
    main()