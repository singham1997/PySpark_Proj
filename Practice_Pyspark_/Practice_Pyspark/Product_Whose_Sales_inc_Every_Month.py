"""
Product whose Sale increases every month

SALE - (sale_id, product_id, sale_month, sale_amount)
PRODUCT - (product_id, product_category)

WITH MonthlySale
AS
(SELECT
    s.product_id,
    p.product_category,
    s.sale_month,
    SUM(s.sale_amount) AS total_sale_amount
FROM SALE s INNER JOIN PRODUCT p
ON s.product_id=p.product_id
GROUP BY s.product_id, p.product_category, s.sale_month),
RankedSale AS (
    SELECT
        product_category,
        sale_month,
        total_sale_amount,
        LAG(sale_amount) OVER(PARTITION BY product_category ORDER BY sale_month) AS prev_month_sale
    FROM MonthlySale
)

SELECT DISTINCT product_category
FROM RankedSale
WHERE prev_month_sale<total_sale_amount
GROUP BY product_category
HAVING COUNT(*) = (
    SELECT COUNT(DISTINCT sale_month)
    FROM MonthlySale
    WHERE product_category=RankedSale.product_category)
)-1;
"""


import logging
import os

from utils import get_spark_session
from pyspark.sql.window import Window
from pyspark.sql.functions import sum, lag, countDistinct

APP_NAME = 'Calculate Department wise employees total and average salary'
ENVIRON = os.environ.get('ENVIRON')

def get_data_using_dataframe(data1: list[tuple], data2: list[tuple], schema1: list[str] = None, schema2: list[str]=None) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        sales_data_schema = ['sale_id', 'product_id', 'sale_month', 'sale_amount']
        product_data_schema = ['product_id', 'product_category']    

        sale_data_df = spark \
                .createDataFrame(data=data1, schema=sales_data_schema)

        product_data_df = spark \
            .createDataFrame(data=data2, schema=product_data_schema)

        join_cond = sale_data_df['product_id'] == product_data_df['product_id']
        join_type = "INNER"
        emp_product_joined_df = sale_data_df \
            .join(product_data_df, join_cond, join_type) \
            .drop(product_data_df['product_id']) \
            .select(sale_data_df['product_id'], product_data_df['product_category'], sale_data_df['sale_month'], sale_data_df['sale_amount'])

        monthly_sale_df = emp_product_joined_df.groupby('product_id', 'product_category', 'sale_month') \
            .agg(
                sum('sale_amount').alias('total_sale_amount')
            ) \
            .select('product_id', 'product_category', 'sale_month', 'total_sale_amount')
        
        window_spec = Window \
            .partitionBy('product_category') \
            .orderBy('sale_month')

        ranked_sale_df = monthly_sale_df \
            .withColumn('prev_month_sale', lag('total_sale_amount')) \
            .over(window_spec)

        prod_w_monthly_increase_sale_df = ranked_sale_df \
            .filter(ranked_sale_df['prev_month_sale'] < ranked_sale_df['total_sale_amount']) 
        
        cnt_prod_month_increase_sale_df = prod_w_monthly_increase_sale_df \
            .groupBy('product_category') \
            .agg(countDistinct('sale_month').alias('distinct_sale_month_count')) 

        total_sale_per_product_df = emp_product_joined_df \
        .groupBy('product_category') \
        .agg(countDistinct('sale_month').alias('total_month'))
    
        join_type = "inner"
        join_cond = cnt_prod_month_increase_sale_df['product_category'] == total_sale_per_product_df['product_category']

        result_df = cnt_prod_month_increase_sale_df.join(total_sale_per_product_df, how=join_type, on=join_cond) \
            .filter(cnt_prod_month_increase_sale_df['distinct_sale_month_count']==(total_sale_per_product_df['total_month']-1)) \
            .select('product_category')

        result_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

def get_data_using_spark_sql(data1: list[tuple], data2: list[tuple], schema1: list[str] = None, schema2: list[str]=None) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        sales_data_schema = ['sale_id', 'product_id', 'sale_month', 'sale_amount']
        product_data_schema = ['product_id', 'product_category']

        sale_data_df = spark \
            .createDataFrame(data=data1, schema=sales_data_schema)

        product_data_df = spark \
            .createDataFrame(data=data2, schema=product_data_schema)

        sale_data_df.createOrReplaceTempView("SALE")
        product_data_df.createOrReplaceTempView("PRODUCT")

        spark_sql = """
            WITH MonthlySale
            AS
            (SELECT
                s.product_id,
                p.product_category,
                s.sale_month,
                SUM(s.sale_amount) AS total_sale_amount
            FROM SALE s INNER JOIN PRODUCT p
            ON s.product_id=p.product_id
            GROUP BY s.product_id, p.product_category, s.sale_month),
            RankedSale AS (
                SELECT
                    product_category,
                    sale_month,
                    total_sale_amount,
                    LAG(sale_amount) OVER(PARTITION BY product_category ORDER BY sale_month) AS prev_month_sale
                FROM MonthlySale
            )
            
            SELECT DISTINCT product_category
            FROM RankedSale
            WHERE prev_month_sale<total_sale_amount
            GROUP BY product_category
            HAVING COUNT(*) = (
                SELECT COUNT(DISTINCT sale_month)
                FROM MonthlySale
                WHERE product_category=RankedSale.product_category)
            )-1;
        """

        emp_product_joined_data_df = spark.sql(spark_sql)

        emp_product_joined_data_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    # Sample data
    sale_data = [
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

    get_data_using_dataframe(sale_data, product_data, None, None)
    get_data_using_spark_sql(sale_data, product_data, None, None)