# Data Engineering Capstone Project

## 1. Introduction

### 1.1. Overview

This is a capstone project for the Udacity DataEngineering Nanodegree. The purpose of the data engineering capstone project is to give you a chance to combine everything learned throughout the program.

### 1.2. Goal

The objective of this project was to create an ETL pipeline to form an datalake about Temperature affections in the US. The analytics database could be used to find the answers for some examples questions:
* How does temperature affect demographics of a city in the US?
* How does temperature affect airport type of a city in the US (small,big)
* Do Asians prefer hot or cold climates?
* Do Asians prefer to live in cities which have big airport ?
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
* date, month, year, avg_temp, avg_temp_uncertainty, city, latitude, longitude

### Dimension tables

1. demographics dimesion table
* city, state_name, median_age, male_population, female_population, total_population, num_veterans, foreign_born, avg_household, state_code, race

2. airports dimension table
* city, airport_code, state_code , type, name

3. time dimesion table
* date, month, year, day, week, weekday 

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


