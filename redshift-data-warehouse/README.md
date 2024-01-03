# Project: Data Warehouse with Redshift - Udacity Data Engineer Nanodegree

## 1. Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

My tasks are building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. I'll be able to test my database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.


## 2. Project structure explanation

```
postgres-data-modeling
│   README.md               # Project description
│   dwh.cfg                 # Configuration file
│   
└───notebooks               # Jupyter notebooks
|   |               
│   └───test.ipynb          # Database test notebook
|  
└───python_scripts
│   |  create_cluster.py    # Cluster creation script
│   │  create_tables.py     # Tables creation script
|   |  etl.py               # ETL script
|   |  delete_cluster.py    # Cluster deletion script
|   |  sql_queries.py       # Definition of all sql queries
```

## 3. Instructions

First you have to install AWS CLI: https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2-windows.html and Boto3: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#using-boto3


**Create cluster**
Create cluster documentation: https://docs.aws.amazon.com/code-samples/latest/catalog/python-redshift-create_cluster.py.html
```
cd python_scripts
python create_cluster.py
```

You have to wait for the cluster to be created. After the cluster has been created, it will print the `HOST ENDPOINT`. Take it and paste it into `dwh.cfg` file.

```
python create_tables.py
python etl.py
```

You can check the database using `test.ipynb` in the notebook folder

Finally, delete the cluster.

```
python delete_cluster.py
```
