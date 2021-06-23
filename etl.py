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
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_immigrations_data(spark, input_data, output_data, file):
    """ Processes immigrations data and creates the immigrations fact tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
        file: the immigrations data file
        
    Returns:
        None
    """
    # get filepath to the immigration data data file
    immigrations_data = input_data + file
    
    # read immigrantation data file
    immigrations_df = spark.read.format('com.github.saurfang.sas.spark').load(immigrations_data)
    
    # drop columns 
    
    immigrations_df = immigrations_df.dropna(how='all')

    # drop columns i don't like
    drop_columns = ['occup', 'entdepu','insnum']

    immigrations_df = immigrations_df.drop(*drop_columns)

    # create a udf to convert arrival date in SAS format to datetime object
    get_datetime = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    
    # convert arrival and departure date into datetime object
    immigrations_df = immigrations_df.withColumn("arrival_date", get_datetime(immigrations_df.arrdate))
    immigrations_df = immigrations_df.withColumn("departure", get_datetime(immigrations_df.depdate))

    # extract columns to create arrival time table
    time_table = immigrations_df.select(
        col('arrival_date').alias('arrival_date'),
        dayofmonth('arrival_date').alias('day'),
        weekofyear('arrival_date').alias('week'),
        month('arrival_date').alias('month'),
        year('arrival_date').alias('year'),
        dayofweek('arrival_date').alias('weekday')
    ) 
    time_table = time_table.drop_duplicates(subset=['arrival_date'])

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data, 'time'),'overwrite')

    # create immigrations fact table

    immigrations_table = immigrations_df.select(
        col('cicid').alias('record_id'),
        col('i94yr').alias('entry_year'),
        col('i94mon').alias('entry_month'),
        col('i94cit').alias('orginal_city_code'),
        col('i94res').alias('original_country_code'),
        col('i94port').alias('destination_city_code'),
        col('arrival_date').alias('arrival_date'),
        col('i94mod').alias('travel_code'),
        col('i94addr').alias('state_code'),
        col('departure').alias('departure_date'),
        col('i94bir').alias('age'),
        col('i94visa').alias('visa_code'),
        col('gender').alias('gender'),
        col('biryear').alias('birth_year'),
        col('admnum').alias('admission_number'),
        col('fltno').alias('flight_number'),
        col('dtaddto').alias('entry_date'),
        col('airline').alias('airline'),
        col('visatype').alias('visa_type')
    )

    immigrations_table.write.partitionBy('year', 'month','state_code').parquet(os.path.join(output_data, 'immigrations'),'overwrite')


def process_demographics_data(spark, input_data, output_data):
    """ Processes demographics data and creates the demographics dimension tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
        demo_file: the demographics data file
        temp_file: the temperatures data file
    Returns:
        None
    """
    # get filepath to demographics data file
    demographics_data = os.path.join(input_data, 'demographics_data/*.csv')
    
    # read demographics data file
    demographics_df = spark.read.csv(demographics_data, header=True, sep=';')
    
    # clean demographics null data
    demographics_df = demographics_df.dropna(how='any')
    
    # drop duplicate columns
    demographics_df = demographics_df.drop_duplicates(subset=['City', 'State', 'State Code', 'Race'])

    # drop column in demographics
    demographics_df = demographics_df.drop(columns=['Count'])

    # get filepath to temperatures data file
    temperatures_data = os.path.join(input_data, 'temperatures_data/*.csv')
    
    # read temperatures data file
    temperatures_df = spark.read.csv(temperatures_data,header=True)
    
    # drop rows with missing average temperature
    demographics_df = demographics_df.dropna(how='any')
    
    # drop duplicate rows
    temperatures_df = temperatures_df.drop_duplicates(subset=['dt', 'City', 'Country'])

    # filter city in US
    temperatures_df = temperatures_df.filter(temperatures_df.Country == 'United States')

    # calculate average temperature of a city
    temperatures_table = temperatures_df.select(['City', 'AverageTemperature']).groupby('City').avg()

    # extract columns to create temperatures table
    temperatures_table = temperatures_table.withColumn('average_temperature', 'avg(AverageTemperature)')
    temperatures_table = temperatures_table.withColumn('city', 'City')
    
    # create joined df with demographics_df and temperatures_df
    
    demographics_df = demographics_df.withColumn('city', 'City')
    joined_df = demographics_df.join(temperatures_table, demographics_df.city == temperatures_table.city)
    
    # create demographics dimension table
    demographics_table = joined_df.select(
        col('city').alias('city_name'),
        col('State').alias('state_code'),
        col('Median Age').alias('median_age'),
        col('Male Population').alias('male_population'),
        col('Female Population').alias('female_population'),
        col('Total Population').alias('total_population'),
        col('Number of Veterans').alias('num_veterans'),
        col('Foreign-born').alias('foreign_born'),
        col('Average Household Size').alias('avg_household'),
        col('State Code').alias('state_code'),
        col('Race').alias('race'),
        'average_temperature'
    )

    # write dimesion demographics table to parquet files
    demographics_table.write.partitionBy('state_code').parquet(os.path.join(output_data, 'demographics'),'overwrite')


def process_airports_data(spark, input_data, output_data):
    """ Processes demographics data and creates the demographics dimension tables
    Args:
        spark: SparkSession
        input_data: Input Files Link
        output_data: Storage Link
        file: the airports data file
        
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
        col('municipality').alias('city_name')
    )

    # write aiports table to parquet files
    airports_table.write.partitionBy('state_code').parquet(os.path.join(output_data, 'airports'), 'overwrite')

def main():
    """Main Script run in AWS cluster
    Args:
        None
        
    Returns:
        None
    """
    
    input_data = "s3a://hieuleinputbucket/"
    output_data = "s3a://hieuleoutputbucket/"

    immigrations_file = 'i94_apr16_sub.sas7bdat'
    temperatures_file = 'GlobalLandTemperaturesByCity.csv'
    demographics_file = 'us-cities-demographics.csv'
    airports_file = 'airport-codes_csv.csv'

    spark = create_spark_session()

    # process_immigrations_data(spark, input_data, output_data , immigrations_file)
    process_demographics_data(spark, input_data, output_data)
    # process_airports_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
