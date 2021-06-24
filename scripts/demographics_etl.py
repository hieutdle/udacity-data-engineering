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
    demographics_df = demographics_df.drop_duplicates(subset=['City', 'State'])

    # create demographics dimension table
    demographics_table = demographics_df.select(
        monotonically_increasing_id().alias('city_id'),
        col('City').alias('city_name'),
        col('State').alias('state_name'),
        col('Median Age').alias('median_age'),
        col('Male Population').alias('male_population'),
        col('Female Population').alias('female_population'),
        col('Total Population').alias('total_population'),
        col('Number of Veterans').alias('num_veterans'),
        col('Foreign-born').alias('foreign_born'),
        col('Average Household Size').alias('avg_household'),
        col('State Code').alias('state_code')
    )
    # write dimesion demographics table to parquet files
    demographics_table.write.parquet(os.path.join(output_data, 'demographics'),'overwrite')

def main():
    """Main Script run in AWS cluster
    Args:
        None
        
    Returns:
        None
    """
    
    input_data = "s3a://hieuleinputbucket/"
    output_data = "s3a://hieuleoutputbucket/"
   
    demographics_file = 'us-cities-demographics.json'


    spark = create_spark_session()

    process_demographics_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
