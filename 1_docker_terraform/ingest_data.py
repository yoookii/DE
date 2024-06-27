#!/usr/bin/env python
# coding: utf-8

import argparse, os

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time

def main(params): 
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    parquet_name = 'yellow_tripdata_2024-01.parquet'
    os.system(f"wget {url} -O {parquet_name}")

    df = pd.read_parquet(parquet_name)

    # tell pandas to convert these columns to datetime format 
    pd.to_datetime(df.tpep_pickup_datetime)
    pd.to_datetime(df.tpep_dropoff_datetime)
 
    pd.io.sql.get_schema(df, name='yellow_taxi_data')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print(pd.io.sql.get_schema(df, name=table_name, con=engine))

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    def insert_chunks(parquet_file, table_name, engine):
        num_row_groups = parquet_file.num_row_groups
        start = 0

        while start < num_row_groups:
            t_start = time()
            end = start + 1  

            try:
                df = parquet_file.read_row_group(start).to_pandas()

                print(f"Row group {start+1}/{num_row_groups}, records: {len(df)}")

                df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
                df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

                df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

                t_end = time()
                print(f"Inserted chunk {start+1}/{num_row_groups}, took {t_end - t_start:.3f} seconds")

            except Exception as e:
                print(f"Error inserting chunk {start+1}: {str(e)}")

            start = end

    parquet_file = pq.ParquetFile(parquet_name)
    insert_chunks(parquet_file, table_name, engine)

if __name__=='__main__': 
    parser = argparse.ArgumentParser(description="Ingest parquet/csv data to Postgres")
    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument("--table_name", help="table name where we will write the results to")
    parser.add_argument("--url", help="url of the parquet/csv file")

    args = parser.parse_args()

    main(args)

# df_iter = pd.read_csv('yellow_tripdata_2024-01.csv', iterator=True, chunksize=100000)
# df = next(df_iter)
# len(df)

# pd.to_datetime(df.tpep_pickup_datetime)
# pd.to_datetime(df.tpep_dropoff_datetime)
# df.head()

# # only insert heads 
# df.head(n=0).to_sql(name='yellow_taxi_data', con=engine, if_exists='replace')

# check the columns & column types in pgcli 
# \d yellow_taxi_data 
# %time df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

# insert all data in chunks 
# while True: 
#     t_start = time()
#     df = next(df_iter)
    
#     pd.to_datetime(df.tpep_pickup_datetime)
#     pd.to_datetime(df.tpep_dropoff_datetime)

#     df.to_sql(name='yellow_taxi_data', con=engine, if_exists='append')

#     t_end = time()

#     print('inserted another chunk..., took %.3f second', % (t_end - t_start))