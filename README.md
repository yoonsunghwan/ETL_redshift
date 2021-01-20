# ETL_redshift
ETL project for udacity

# Description
ETL pipleline for a database hosted on AWS Redshift.
The data will be loaded from a public S3 Bucket to a staging table on Redshift, from the stage table the data will be transformed into tables then can be readily accessed for data analytics.

# Files
------ 

#### dwh.cfg
Configuration that must be edited prior to running the python scripts. Need to input values found on AWS console.

#### sql_queries.py
Python file that contains SQL queries.

#### etl.py
Python script to extract transfrom and load the data using the SQL scripts found in sql_queries.py

#### create_tables.py
Python script that will use SQL queries found in sql_queries.py to drop/create a new table for the ETL process.

# How To 
-----  
First edit dwh.cfg and add the necessary parameters.  
Then RUN create_tables.py, and etl.py in that order. This will create tables and extract into it, data from a public S3 bucket. 

