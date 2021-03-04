#!/usr/bin/env python
# coding: utf-8

# In[116]:


import os
import sys
import pandas as pd
from sql_wrapper import db_connector
import dask.dataframe as dd
from io import StringIO
from sqlalchemy import create_engine, func

import logging
import logging.config


#logger is configured from 'logger.conf' file in the project directory
logging.config.fileConfig(os.path.dirname(__file__)+r"\logger.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)

#creating a list of file names that we use in the upcoming ETL functions below'''
file_names=['user_data.csv','event_data.csv']


def file_check():
    '''
        Parameters: None
        
        Returns: None
        
        Definition:
        Checks if source files are present in the input path and exits if file is not present.
        
    '''
    file_exist = [file for file in file_names if os.path.isfile(os.path.dirname(__file__)+"\input\{}".format(file))];
    #sorting the lists
    file_exist.sort()
    file_names.sort()
    
    if file_exist==file_names:
        pass
    else:
        logger.error("Source file is not present in the input path.Please check!!")
        sys.exit()


# In[87]:



def user_data_extract(Filepath):
        '''
            Parameter: Filepath (string)
            
            Returns: user_data_df (dataframe)
            
            Description:
            1.Function to extract the User data from source CSV file 'user_data.csv'.
              Used dask dataframe dd.read_csv for efficient and fast data read.

            2.Also calls Data masking method that anonymises 'user_id' and 'email' fields of 'user_data.csv' as per the requirement.
        '''
        logger.info("Extracting user data from source file user_data.csv")
        user_data_df=dd.read_csv(Filepath,delimiter=';',usecols=['user_id','email'])
        user_data_df=user_data_df.compute() #converting dask dataframe to pandas dataframe for further processing
        user_data_df=data_masking(user_data_df)
        count_user_data=len(user_data_df)
        logger.info("{} records fetched from user_data.csv!".format(count_user_data))
        return user_data_df



# In[102]:


def event_data_extract(Filepath):
    '''
        Parameters: Filepath(String)
        
        Returns: event_data_df (dataframe)
        
        Description:
        
        1.Function to extract the event data from source CSV file 'event_data.csv' into dataframe 'event_data_df'.
          Used dask dataframe dd.read_csv for efficient and fast data read.

        2.Converts event_date field of 'event_data_df' dataframe to standard date format using pandas.to_datetime()' method.

        3.Week_number is retrived from 'event_date' and added to new column in 'event_data_df' dataframe using insert() method.
        
        4.Also calls Data masking method to anonymizes user_id field of 'event_data.csv' as per the requirement.

    '''
    logger.info("Extracting event data from source file event_data.csv")
    event_data_df=dd.read_csv(Filepath,delimiter=';',usecols=['event_date','event_id','user_id'])
    event_data_df=event_data_df.compute() #converting dask dataframe to pandas dataframe for further processing
    event_data_df.event_date= pd.to_datetime(event_data_df.event_date,format='%d.%m.%y')
    #inserting week number column
    event_data_df.insert(3,'week_number',pd.Series([date.strftime("%V") for date in list(event_data_df.event_date)]))
    event_data_df=data_masking(event_data_df)
    count_user_data=len(event_data_df)
    logger.info("{} records fetched from event_data.csv!".format(count_user_data))
    return event_data_df


# In[103]:



def data_masking(data_df):
    
    '''

    Parameters: data_df (dataframe) - Dataframe obtained from source files in above methods
    
    Retruns : data_df (Dataframe) - with masked data.
    
    Description:
    
    1.This fucntion anonymises the user sensitive information as below:

        user_id: it is multiplied with 5 and then converted this result to octadecimal. 
        Also replaced additional letter 'o' in the resultant which indicates that number is octadecimal with ''.

        email: only retained domain name after '@' by removing the user name.
    
    '''
    data_df.user_id=pd.Series([int(oct(x*5).replace('o','')) for x in data_df.user_id])
    if 'email' in data_df.columns:
        data_df.email =pd.Series([x[1] for x in data_df.email.str.split('@')])
    return data_df







# In[113]:


@db_connector
def ingest_to_temp_tables(connection,event_data_df,user_data_df,event_table_name,user_table_name):
    
    ''' 
    Parameters: 
    1.connection - DB connection string, will receive this from sql_wrpper.py module using @db_connector
    2.user_data_df - Dataframe formed from user_data.csv post preprocessing in above methods
    3.event_data_df - Dataframe formed from event_data.csv post preprocessing in above methods
    4.user_table_name - User_data DB table name
    5.event_table_name - Event_data DB table name
    
    Returns: none
    
    Description:
    
    1.Mysql file upload folder path is retrived using query - "SHOW VARIABLES LIKE \"secure_file_priv\";".
      Replace \\ with / which is the path format accepted by LOAD INFILE .. statement.
      
    2.Above source dataframes - user_data_df,event_data_df are written to corresponding temp csv files.
    
    3.Forming LOAD INFILE sql query that can be executed in mysql DB.
    
    4.Truncating tables before load.
    
    5.Executing LOAD INFILE .. statments for both user_data and event_data.

    '''
    #1
    default_path_rows=connection.execute("SHOW VARIABLES LIKE \"secure_file_priv\";").fetchall()
    load_infile_path=[row[1].replace('\\','/') for row in default_path_rows]
    #2
    user_data_df.to_csv(load_infile_path[0]+r"user_data_temp.csv",index=False,chunksize=1000)
    event_data_df.index+=1
    event_data_df.to_csv(load_infile_path[0]+r"event_data_temp.csv",index_label='id',chunksize=1000)
    
    logger.info("Ingesting source data to temp tables in DB ... ")
    
    #3
    user_data_load_sql="LOAD DATA INFILE "+"\""+load_infile_path[0]+r"user_data_temp.csv"+"\""+" INTO TABLE "+user_table_name+" FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
    event_data_load_sql="LOAD DATA INFILE "+"\""+load_infile_path[0]+r"event_data_temp.csv"+"\""+" INTO TABLE "+event_table_name+" FIELDS TERMINATED BY ',' ENCLOSED BY '\"' IGNORE 1 LINES;"
    
    #4
    connection.execute("Truncate table "+ user_table_name+";")
    connection.execute("Truncate table "+ event_table_name+";")
    #5
    connection.execute(user_data_load_sql)
    connection.execute(event_data_load_sql)
    
    logger.info("Source Data ingested to DB successfully!!")   
    
 




    
@db_connector
def transform_and_load(connection):
    '''
    Parameters:connection - DB connection string,this will be received from sql_wrpper.py module using @db_connector
    
    Returns: none
    
    Description:
    
    1.Executes the stored procedure that has sql query for applying transformations as part of Task1.
      This procedure also loads the final target table campaign_performance.
      
    2.Executes the stored procedure that has sql query for applying transformations as part of Task2.
      This procedure also loads the final target table active_users.
    
    '''
    logger.info("Applying Transformations..")
    logger.info("Loading Target tables..")
    with connection.begin() as conn:
        conn.execute("CALL `Load_Active_Users`")
        conn.execute("CALL `Load_Mail_Campaign_performance`")
    logger.info("Transformations applied and Target tables loaded successfully.")
    logger.info('EXIT 0')

    
    
    
    

# In[112]:

    '''
    Description:
    
    1. Main module, from here all the above functions are called.
    
    2. Please note that input source files are to be placed inside 'input' folder of the project space.
    
    '''

if __name__ == '__main__':
    
    file_check()   
    filepath=os.path.dirname(__file__)+"\input"
    for file in file_names:
        if file=='event_data.csv':
            event_data_df=event_data_extract(filepath+r"\{}".format(file))
        else:
            user_data_df=user_data_extract(filepath+r"\{}".format(file))
    ingest_to_temp_tables(event_data_df,user_data_df,file_names[0].split('.')[0],file_names[1].split('.')[0])
    transform_and_load()


# In[ ]:
