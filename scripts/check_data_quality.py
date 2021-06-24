import configparser
from datetime import datetime, timedelta
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import DateType,StringType
from pyspark.sql import functions as F
from pyspark.sql.functions import avg

def create_spark_session():
    """Creates a Spark Session.
    Args:
        None
        
    Returns:
        None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def check_airports_data(spark, output_data):
    airports_check_df = spark.read.parquet(os.path.join(output_data, 'airports/state_code=*/*.parquet'))

    if airports_check_df.count() == 0:
        raise AssertionError('Processing airports data failed.')


def check_demographics_data(spark, output_data):

    demographics_check_df = spark.read.parquet(os.path.join(output_data, 'demographics/state_code=*/*.parquet'))

    if demographics_check_df.count() == 0:
        raise AssertionError('Processing demographics data failed.')


def check_temperatures_data(spark, output_data):
    temperatures_check_df = spark.read.parquet(os.path.join(output_data, 'temperatures/year=*/month=*/*.parquet'))

    if temperatures_check_df.count() == 0:
        raise AssertionError('Processing temperatures data failed.')

def check_time_data(spark, output_data):
    time_check_df = spark.read.parquet(os.path.join(output_data, 'time/year=*/month=*/*.parquet'))

    if time_check_df.count() == 0:
        raise AssertionError('Processing time data failed.')

def main():
    """Main Script run in AWS cluster
    Args:
        None
        
    Returns:
        None
    """
    
    output_data = "s3a://hieuleoutputbucket/"

    spark = create_spark_session()

    check_airports_data(spark, output_data)
    check_demographics_data(spark, output_data)
    check_temperatures_data(spark, output_data)
    check_time_data(spark, output_data)
    

if __name__ == "__main__":
    main()
