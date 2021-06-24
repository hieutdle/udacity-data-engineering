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
    # temperatures_data = os.path.join(input_data, 'tests_data/*.csv')

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

def process_demographics_data(spark, input_data, output_data):
    """ Processes demographics data and creates the demographics dimension tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
    Returns:
        None
    """
    # get filepath to demographics data file
    demographics_data = os.path.join(input_data, 'demographics_data/*.json')
    
    # read demographics data file
    demographics_df = spark.read.option("multiline","true").json(demographics_data)
    
    # drop na
    demographics_df = demographics_df.dropna(subset=['Total Population'])

    # drop duplicate columns
    demographics_df = demographics_df.drop_duplicates(subset=['City', 'State', 'Race'])

    # create demographics dimension table
    demographics_table = demographics_df.select(
        col('City').alias('city'),
        col('State').alias('state_name'),
        col('Median Age').alias('median_age'),
        col('Male Population').alias('male_population'),
        col('Female Population').alias('female_population'),
        col('Total Population').alias('total_population'),
        col('Number of Veterans').alias('num_veterans'),
        col('Foreign-born').alias('foreign_born'),
        col('Average Household Size').alias('avg_household'),
        col('State Code').alias('state_code'),
        col('Race').alias('race'),
    )
    
    # write dimesion demographics table to parquet files
    demographics_table.write.partitionBy('state_code').parquet(os.path.join(output_data, 'demographics'),'overwrite')


def process_airports_data(spark, input_data, output_data):
    """ Processes demographics data and creates the demographics dimension tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
    Returns:
        None
    """
    # get filepath to airport data file
    airports_data = os.path.join(input_data, 'airports_data/*.csv')
    
    # read airport data file
    airports_df = spark.read.csv(airports_data,header=True)

    # filter airport in US
    airports_df = airports_df.filter(airports_df.continent == 'NA')
    airports_df = airports_df.filter(airports_df.iso_country == 'US')

    # extract 2-letter state code
    extract_state_code = F.udf(lambda x: x[3:], StringType())
    airports_df = airports_df.withColumn('state_code', extract_state_code('iso_region'))

    # extract columns to create songs table
    airports_table = airports_df.select(
        col('ident').alias('airport_code'),
        'state_code',
        'type',
        'name',
        col('municipality').alias('city')
    )

    # write aiports table to parquet files
    airports_table.write.partitionBy('state_code').parquet(os.path.join(output_data, 'airports'), 'overwrite')

def check_data_quality(spark, output_data):
    airports_check_df = spark.read.parquet(os.path.join(output_data, 'airports/state_code=*/*.parquet'))

    if airports_check_df.count() == 0:
        raise AssertionError('Processing airports data failed.')

    demographics_check_df = spark.read.parquet(os.path.join(output_data, 'demographics/state_code=*/*.parquet'))

    if demographics_check_df.count() == 0:
        raise AssertionError('Processing demographics data failed.')

    temperatures_check_df = spark.read.parquet(os.path.join(output_data, 'temperatures/year=*/month=*/*.parquet'))

    if temperatures_check_df.count() == 0:
        raise AssertionError('Processing temperatures data failed.')

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
    
    input_data = "s3a://hieuleinputbucket/"
    output_data = "s3a://hieuleoutputbucket/"
   
    temperatures_file = 'GlobalLandTemperaturesByCity.csv'
    demographics_file = 'us-cities-demographics.csv'
    airports_file = 'airport-codes_csv.csv'

    spark = create_spark_session()

    process_temperatures_data(spark, input_data, output_data)
    process_demographics_data(spark, input_data, output_data)
    process_airports_data(spark, input_data, output_data)
    check_data_quality(spark,output_data)

if __name__ == "__main__":
    main()
