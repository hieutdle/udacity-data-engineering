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
   
    airports_file = 'airport-codes_csv.csv'

    spark = create_spark_session()

    process_airports_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
