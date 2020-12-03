# This file turns the business_reviews_users_merged ZIP file into a single .csv file

import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('file conversion').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

# DataFrame[user_id: string, business_id: string, business_name: string, business_stars: string, business_review_count: string, is_open: string, date: string, review_id: 
# string, review_stars: double, average_stars: double, elite: string, username: string, user_review_count: bigint]

def main():
    reviews = spark.read.csv('business_reviews_users_merged')
    reviews = reviews.withColumnRenamed('_c0', 'user_id') \
    		.withColumnRenamed('_c1', 'business_id') \
    		.withColumnRenamed('_c2', 'business_name') \
    		.withColumnRenamed('_c3', 'business_stars') \
    		.withColumnRenamed('_c4', 'business_review_count') \
    		.withColumnRenamed('_c5', 'is_open') \
    		.withColumnRenamed('_c6', 'date') \
    		.withColumnRenamed('_c7', 'review_id') \
    		.withColumnRenamed('_c8', 'review_stars') \
    		.withColumnRenamed('_c9', 'average_stars') \
    		.withColumnRenamed('_c10', 'elite') \
    		.withColumnRenamed('_c11', 'username') \
    		.withColumnRenamed('_c12', 'user_review_count') 

    # reviews.show()

    reviews.toPandas().to_csv('business_reviews_users_merged.csv')


if __name__=='__main__':
    main()
