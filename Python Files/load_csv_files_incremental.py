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

    #define pkey on each table
    table_pkey = [
        'employe_id',
        'timesheet_id'
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
    for i in range(len(csv_name)):
        val = csv_name[i]
        pkey = table_pkey[i]
        
        #read csv
        df = pd.read_csv('../CSV/'+val+'.csv')
        
        #add new column `etl_datetimg`
        df['etl_datetime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        #load data to DB
        df.to_sql(val, con=conn, if_exists='replace', index=False)
        
        print(f'{val} data ingested')
        
        #################
        ## Incremental ##
        #################
        
        # read data from raw and staging
        raw_data = pd.read_sql(f""" select {pkey} as pkey, * from public.{val}; """, conn)
        staging_date = pd.read_sql(f'select {pkey} as pkey, * from stg.{val}', conn)
        
        # detect data change
        data_changes = raw_data[~raw_data.apply(tuple,1).isin(staging_date.apply(tuple,1))]
        
        # get new data to be inserted
        new_data = data_changes[~data_changes.pkey.isin(staging_date.pkey)]
        
        #drop helper column
        new_data = new_data.drop('pkey', axis=1)
        
        #insert into staging table
        new_data.to_sql(f'{val}', con=conn, if_exists='append', index=False, schema='stg')

    print('incremental finished')
    
except Exception as e:
    print(f'Something went wrong. {e}')
finally:
    conn.close()