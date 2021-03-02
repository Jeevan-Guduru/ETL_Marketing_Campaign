#!/usr/bin/env python
# coding: utf-8

# In[ ]:
from configparser import ConfigParser
import psycopg2
import logging 
from sqlalchemy import create_engine
import os

import logging
import logging.config

logging.config.fileConfig(os.path.dirname(__file__)+r"\logger.conf", disable_existing_loggers=False)
logger = logging.getLogger(__name__)



#Postgresql Wrapper. Decorater to handle DB connection in functions that are wrapped by this.
def db_connector(func):
    
    '''
    Parameters: Function - takes function as its parameter, upon which this decorater is wrapped.
    
    Returns : Function, by including connection parameter to its parameters.
    
    Description:
    
    1.Calls config() which fetches Database connection parameters defined in Database_Params.ini path.
    
    2.Connection string is formed from above parameters.
    
    3.Calls the fucntion that is wrapped by this decorator in try,catch block.
    
    '''

    def with_connection_(*args,**kwargs):
        #1
        params = config()
        #2
        con_string='mysql+pymysql://'+params["user"]+':'+params["password"]+'@'+params["host"]+':'+params["port"]+'/'+params["database"]
        engine = create_engine(con_string)
        conn = engine.connect()
       
        logger.info('Connecting to the mysql database...')
        #3
        try:
            rv = func(engine, *args,**kwargs)
        except Exception:
            logger.error("Database connection error")
            raise
        else:
            conn.execute("commit;")
            logger.info("Commit done!")
        finally:
            conn.close()
            logger.info("DB connection closed.")
        return rv
    return with_connection_




#Function to read Database connection details from parameters file - Database.ini
def config(DBParamsPath=os.path.dirname(__file__)+'\Database_Params.ini', section='mysql'):
    '''
    
    Parameters:Keyword arguments , Database params path and section to refer in params file.
    
    Returns: Dictionary of DB connection parameters.
    
    Description:
    
    1.Using ConfigParser() method, it is parsing .ini file and captures parameters in dictionary.
    
    2.If section not found in .ini file , it raises exception.
    
    '''
    parser = ConfigParser()
    parser.read(DBParamsPath)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, DBParamsPath))
    return db

