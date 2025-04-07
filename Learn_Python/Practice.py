from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    conf = SparkConf.setAppName("Department_wise_avg_and_total_salary").setMaster("local[*]")

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # Sample data
    sales_data = [
        (1, "2023-01", 1, 100),
        (2, "2023-02", 1, 50),
        (3, "2023-01", 2, 25),
        (5, "2023-02", 2, 100),
        (6, "2023-03", 2, 150),
        (4, "2023-01", 3, 75)
    ]

    product_data = [
        (1, "TV"),
        (2, "Laptop"),
        (3, "Xbox"),
        (4, "Laptop"),
        (5, "Xbox")
    ]

    try:
        # Create DataFrames
        sales_df = spark.createDataFrame(sales_data, ["sale_id", "sale_month", "product_id", "sale_amount"])
        product_df = spark.createDataFrame(product_data, ["product_id", "product_category"])

        joinType = "inner"
        joinCond = usersDataDF["name"] == departmentDataDF["name"]

        usersDeptDataDF = usersDataDF.join(departmentDataDF, how=joinType, on=joinCond)

        usersDeptDataDF.show()
    except Exception as e:
        print(f"Running program is giving an exception: {str(e)}")
    finally:
        spark.stop()