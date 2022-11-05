import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import datetime

try:
    load_dotenv()

    #define csv filenames
    csv_name = [
        'employees',
        'timesheets'
    ]
    
    print('data ingestion started')
    
    #define db properties and connection
    db_user=os.getenv('DB_USER')
    db_password=os.getenv('DB_PASSWORD')
    db_host=os.getenv('DB_HOST')
    db_schema=os.getenv('DB_SCHEMA')
    conn_string = f'postgresql://{db_user}:{db_password}@{db_host}/{db_schema}'
    db = create_engine(conn_string)
    conn = db.connect()

    # iterate each csv file
    for val in csv_name:
        #read csv files
        df = pd.read_csv('../CSV/'+val+'.csv')
        
        #add new column `etl_datetime`
        df['etl_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        #load data to DB
        df.to_sql(val, con=conn, if_exists='replace', index=False)
        
        #create staging table
        conn.execute('create schema if not exists stg;')
        conn.execute(f'drop table stg.{val}; create table stg.{val} as select * from public.{val};')
        
    print('data ingestion finished')
    
except Exception as e:
    print(f'Something went wrong, {e}')
finally:
    conn.close()