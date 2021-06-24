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


def process_temperatures_data(spark, input_data, output_data):
    """ Processes temperatures data, creates the temperatures fact tables and time tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
    Returns:
        None
    """
    # get filepath to temperatures data file
    temperatures_data = os.path.join(input_data, 'temperatures_data/*.csv')


    # read temperatures data file
    temperatures_df = spark.read.csv(temperatures_data,header=True)
    
    # drop rows with missing average temperature
    temperatures_df = temperatures_df.dropna(subset=['AverageTemperature'])
    
    # drop duplicate rows
    temperatures_df = temperatures_df.drop_duplicates(subset=['dt', 'City', 'Country'])

    # filter city in US
    temperatures_df = temperatures_df.filter(temperatures_df.Country == 'United States')

    # extract columns to create time table
    time_table = temperatures_df.select(
        col('dt').alias('date'),
        dayofmonth('dt').alias('day'),
        weekofyear('dt').alias('week'),
        month('dt').alias('month'),
        year('dt').alias('year'),
        dayofweek('dt').alias('weekday')
    ) 
    time_table = time_table.drop_duplicates(subset=['date'])

    # write dimesion time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'),'overwrite')

    # create temperature fact table
    temperatures_table = temperatures_df.select(
        col('dt').alias('date'),
        month('dt').alias('month'),
        year('dt').alias('year'),
        col('AverageTemperature').alias('avg_temp'),
        col('AverageTemperatureUncertainty').alias('avg_temp_uncertainty'),
        col('City').alias('city'),
        col('Country').alias('country'),
        col('Latitude').alias('latitude'),
        col('Longitude').alias('longitude')
    )

    # write temperatures fact table to parquet files partitioned by year and month
    temperatures_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'temperatures'),'overwrite')


def main():
    """Main Script run in AWS cluster
    Args:
        None
        
    Returns:
        None
    """
    
    input_data = "s3a://hieuleinputbucket/"
    output_data = "s3a://hieuleoutputbucket/"
   
    temperatures_file = 'GlobalLandTemperaturesByCity.csv'

    spark = create_spark_session()

    process_temperatures_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
