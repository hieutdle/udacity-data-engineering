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

Data Lake
This is what we are using for analytics. The data is prepared to allow for fast query times.
We are consturcting a star schema with 1 fact table and multiple dimension tables.

Fact table: temperatures table.
* Decided by business use-case (analyze the average temperature through year of a city)
* Holds records of a metric(city,average temperature,...)
* Changes regularly. Update every day according to the data source.
* Connects to dimensions via foreign keys(via aiport_code code and city_id)

Dimesion table: city demographics table, airport table, time table
* Holds descriptions of attributes:
    * For the city table, it hold the demographics of a city
    * For the airport table, it hold the description of a airport
    * For the time table, it hold the description of a time
* These table are not change often (the demographics and the description of the airport are just provided at specify time according to the data source, do not have date attribute )

I choose the Star schema because is in a more de-normalized form and hence tends to be better for performce analysis. It uses less foreign key. The relationships inside the data are simple and don’t have many to many relationships .

The  temperatures fact table is the heart of the data model. This table's data comes from the temperatures data sets and contains keys that links to the dimension tables. The data dictionary of the temperatures dataset contains detailed information on the data that makes up the fact table.

The us city dimension table comes from the demographics dataset and links to the temperatures fact table at by city_id. This dimension would allow analysts to get insights from temperatures patterns into the US city demographics based on cities for example: overall population of a cities based on temperature. We could ask questions such as, do low temperatures have small population?. 

Example:

I find the average temperature in 2/2010 of Minneapolis its -9, its pretty low so i want to find its population and median age for further analysis.

The airport dimension table comes from airport dataset and link to the temperatures fact table by airport_code. This dimesion would allow analyst to get insights like does temperatures affect airport types?

Example: 

Now i have a average temperature of a specific time at the year (it can be very low), its population. So I want to find if there are any heliport in this city for further analysis like do low temperature and low population make it have less type of airports. ( low temperature make it harder for operate airport and low population make it less airport and also airport type )

**Cleaning data:**
I filter all data:
* drop duplicates in all table
* just about the city in the usa so that we can analysis about it.

The main idea is analyze about the temperature and also can check the demographic of the city and the airport type of the same city in the US when the analyst need it.

# 3. Data Dictionary

### Fact table

1. temperatures fact table
* temperature_id: int；unique id of a part of a tempearature record; Primary Key, auto-incremented
* date: datetime, shows the date when the the temperature was recorded, foreign key
* month: int, month of the record
* year: int, year of the record 
* avg_temp: float, Average temperature: show the average temperature of the city corresponding to the date. 
* avg_temp_uncertainty: float,  Average temperature uncertainty: you don't know for certain that the temperature was exactly {number}, so the avg_temp_uncertainty measure the fluctuations range.
* city: string, name of the city where recorded the temperature 
* latitude: string, latitude of the record
* longitude: string, longitude of the record
* airport_code: string, airport_code, foreign key, using for connect to the airport table
* city_id: int, city unique id, foreign key, using for connect to the demographic table

### Dimension tables

1. city demographics dimesion table: A demographic of a city, where we can get a data about demographic.
* city_id: int, unique id of a city; Primary Key, auto-incremented
* city : string, name of the city
* state_name: string, Name of the state where the city is
* median_age : float, median age of people in the city
* male_population: int, male population of the city
* female_population: int, female population of the city
* total_population: int, total populationn of the city
* num_veterans: int, number of vererans in the city
* foreign_born: int, number of foreign-born in the city
* avg_household: float, average household size, average number of people in a family
* state_code: string, 2-letter code of the state

2. airports: airport dimension table which store the information about airport
* airport_code: string, 4-character unique airport code, Primary Key
* city_name: string, name of the city where the airport is
* state_code: string, 2-letter code of the state
* type: string, type of airport (helicop,small, medium, large, ...)
* name:  string, name of the airport

3. time dimesion table: timestamps of records in temperatures broken down into specific units
* date : datetime, shows time when record temperature, Primary Key
* month : int, month of the record
* year: int, year of the cord
* day: int, day of the record in a month
* week int, week of the record in a month
* weekday: int, day of the record in a week

# 3. Project structure

```
udacity-data-engineering
│   README.md                            # Project description  
│   requirements.txt                     # Python dependencies
│   dl.cfg                               # Config file
|   test_parquet.ipynb                   # Check parquet files
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

