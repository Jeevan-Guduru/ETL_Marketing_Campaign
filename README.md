# ETL_Marketing_Campaign

# Introduction:

This utility which is developed in python and sql  consumes data from source files - user_data.csv and event_data.csv and loads into mysql DB after performing given transformations and analysis.
It begins with preprocessing of data - like anonymising sensitive data , formatting date fields and string fields as required.

# Pre-requisites:

DB:
1. MYSql is the DB where target data is stored. Please make sure it is set up.
2. Create a Database with  name - 'dwh_challenge'.
3. Run  scripts in SQL_Scripts folder (DDL scripts,Load_Active_Users,Load_Campaign_Performance) in the databse created above.
4. Make sure below tables are created in the DB 'dwh_challenge':
    Intermediate or staging tables:
      i.user_data
      ii.event_data
     Target Tables:
      iii.active_users
      iv.campaign_performance
 5.Make sure below stored procedures are created after running above SQL scripts:
    i.Load_Active_Users
    ii.Load_Campaign_Performance


Python:
1. As this utility is written in python it has to be present.
2. Make sure install the requirements using 'pip install requirements.txt' command in the terminal. <requirements.txt is in ..\SparkNetworks_dwh\requirements.txt>
3. Setup your DB connection parameters in 'Database_Params' in the path ..\SparkNetworks_dwh\dwh



# Steps to be followed:
1.Copy the zip folder as it is on your desktop (or the loaction where you wish to).
2.Please place both source files in the input path - ..\SparkNetworks_dwh\dwh\input\
3.Once files are placed, Open terminal and run the ETL_MySql.py from console as below:
  python .../<path to ETL_MySql.py >/ETL_MySql.py
4.Once it is run, check data in target tables - campaign_performance,active_users
