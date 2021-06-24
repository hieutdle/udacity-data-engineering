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


def process_immigrations_data(spark, input_data, output_data):
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
    immigrations_data = os.path.join(input_data, 'immigrations_data/*.sas7bdat')
    
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
        col('i94yr').alias('year'),
        col('i94mon').alias('month'),
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

def main():
    """Main Script run in AWS cluster
    Args:
        None
        
    Returns:
        None
    """
    
    input_data = "s3a://hieuleinputbucket/"
    output_data = "s3a://hieuleoutputbucket/"
   
    spark = create_spark_session()

    process_immigrations_data(spark,input_data,output_data)

if __name__ == "__main__":
    main()