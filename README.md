# Data Engineering Capstone Project

## 1. Introduction

### 1.1. Overview

This is a capstone project for the Udacity DataEngineering Nanodegree. The purpose of the data engineering capstone project is to give you a chance to combine everything learned throughout the program.

### 1.2. Goal

The objective of this project was to create an ETL pipeline to form an datalake about Temperature affections in the US. The analytics database could be used to find the correlation between temperature, demographics and airport type.
...

### 1.3. Datasets

We are going to work with 3 different datasets and try to combine them in a useful way to extract meaningful information:

* World Temperature Data: This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)(`.CSV`, 8,5 million rows).
* U.S. City Demographic Data: This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)(`.JSON`).
* Airport Code Table: This is a simple table of airport codes and corresponding cities. It comes from [here](https://datahub.io/core/airport-codes#data)(`.CSV`).

All data for this project was loaded into my S3 inputbucket.

### 1.4 Technologies

We are going to store our data lake on Amazon S3, which is is an cloud storage service provided by Amazon.
Why we use cloud data storage:
* Rented server (the rent is cheap)
* Don’t need space (don’t need a room to store)
* Use the resources we need, at the time we need them
* The closer the server is to the user, the less latency they will experience when using our application
Amazon is also one of the best cloud provider which offer scalability, data availability, security, and performance for data storage services right now.

For our ETL process we are using Apache Spark running on an EMR cluster on AWS. IF the data is increased, we can increase the worker instances count to minimizing the execution time.

# 2. Data Model

### Fact table

1. temperatures fact table
* temperature_id: unique id of a part of a tempearature record; Primary Key, auto-incremented
* date: datetime, date of record, foreign key
* month: int, month of record
* year: int, year of record
* avg_temp: string, average temperature
* avg_temp_uncertainty: string, average temperature
* city: name of the city
* latitude: string, latitude
* longitude: string, longitude
* airport_code: airport_code, foreign key
* city_id: int, city unique id, foreign key

### Dimension tables

1. city demographics dimesion table: A demographic of a city
* city_id: unique id of a part of demographics in a city; Primary Key, auto-incremented
* city : string, name of the city
* state_name: string, state_code
* median_age : float, median age
* male_population: int, male population
* female_population: int, female population
* total_population: int, total population
* num_veterans: int, number of vererans
* foreign_born: int, number of foreign born
* avg_household: float, average household size
* state_code: string, 2-letter code of the state

2. airports: airport dimension table which store the information about airport
* airport_code: string, 4-character unique airport code, Primary Key
* city_name: string, name of the city where the airport is
* state_code: string, 2-letter code of the state
* type: string, type of airport (helicop,small, medium, large, ...)
* name:  string, name of the airport

3. time dimesion table: timestamps of records in temperatures broken down into specific units
* date : datetime, shows time when record temperature, Primary Key
* month : int, month
* year: int, year
* day: int, day
* week int, week
* weekday: int, day of week

# 3. Project structure

```
udacity-data-engineering
│   README.md                            # Project description  
│   requirements.txt                     # Python dependencies
│   dl.cfg                               # Config file
|
└───script                               # Python Scripts folder
|   | demographicss_etl.py               # Demographics data ETL
|   | airports_etl.py                    # Airports data ETL   
|   | temperatures_etl.py                # Temperatures data ETL   
|   | etl.py                             # All in one ETL   
|   | setup.py                           # Setup S3 buckets, iam_role, EMR cluster, send etl files to run in aws cluster
|   | check_data_quality.py              # Data Quality Check
|
└───sample_data                          # Sample data folder
|   | airport-codes_csv.csv              # Airports sample data
|   | temperatures_sample.csv            # Temperatures sample data
|   | us-cities-demographics.json        # Demographics sample_data   
|  
└───extra                                # Extra folder for test and new features in future
|   | spark-sas7bdat-3.0.0-s_2.12.jar    # Jar file for read sas7bdat files in the future
|   | test.ipynb                         # test notebook
|   | test.py                            # ETL for sas7bdat files in the future    
```

# 4. Running steps

```
cd scripts
python setup.py
```

# 5. Addressing Other Scenarios

1. The data was increased by 100x
    - Because we are using AWS EMR cluster, we should increase the worker instances count to minimizing the execution time.

2. The pipelines would be run on a daily basis by 7 am every day.
    - We can schedule Airflow pipelines so that they follow this pattern.
   
3. The database needed to be accessed by 100+ people.
    - We can load Parquet files into Amazon Redshift as data warehouse. We can increase Redshift nodes depending on data size and access rate.
    - we can also try using Amazon Athena. Amazon Athena is an interactive query service that makes it easy to analyze data directly in Amazon Simple Storage Service (Amazon S3) using standard SQL


# 6. Example of how  a user will do the query from the given schema.

They can query the demographics of a city which has the lowest average temperature through the city_id.

