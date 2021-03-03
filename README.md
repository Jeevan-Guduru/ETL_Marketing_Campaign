# ETL_Marketing_Campaign

# Introduction:

This utility which is developed in python and sql  consumes data from source files - user_data.csv and event_data.csv and loads into mysql DB after performing required transformations and analysis.

# project Structure:
1. Main Folder : ***SparkNetworks_dwh***
    * contents : 
        1. SQL_Scripts
        2. requirements.txt
2. Sub folder inside above folder : ***dwh***
     * Contents:
        1. ETL_MySql.py - which comprises of the main code and entry point
        2. _init_.py -init file
        3. Sql_Wrapper - which is the wrapper used to connect to underlying database in the main code above.
        4. logger.conf - configuration file for logger.
        5. Database_params - Database connection parameters. User needs to set parameters of their DB here by editing this file.
        6. Input - Folder where user have to place the source files.

# Pre-requisites and setup:

DB:
1. MYSql is the DB where target data is stored. Please make sure it is set up.
2. Create a Database with  name - 'dwh_challenge'.
3. Run  scripts in SQL_Scripts folder (DDL scripts,Load_Active_Users,Load_Campaign_Performance) in the databse created above.
4. Make sure below tables are created in the DB 'dwh_challenge':
     * Intermediate or staging tables:
        * user_data
        * event_data
     * Target Tables:
        * active_users
        * campaign_performance
5. Make sure below stored procedures are created after running above SQL scripts.:
     * Load_Active_Users
     * Load_Campaign_Performance

#### Note: These stored procedures are written for performing transformations and for loading the target tables.


Python:
1. As this utility is written in python it has to be present in the users machine.
2. Make sure to install the dependencies which are present in ***requirements.txt***, using ***'pip install -r ..\path_to_this_file..\requirements.txt'*** command in the terminal. <requirements.txt is in ***..\SparkNetworks_dwh\requirements.txt>***
3. Setup your DB connection parameters in ***'Database_Params.ini'*** file in the path ***..\SparkNetworks_dwh\dwh***



# Steps to be followed:
1. Download the code as it is on your desktop (or the loaction where you wish to).
2. Please place both source files in the input path - ***..\SparkNetworks_dwh\dwh\input\***
3. Once files are placed, Open terminal and run the ETL_MySql.py from console as below:
   ***python  ../path to ETL_MySql.py.../ETL_MySql.py***
4. Once it is run, check data is loaded into target tables - ***campaign_performance,active_users***.

----------------------------------------------End---------------------------------------------------
