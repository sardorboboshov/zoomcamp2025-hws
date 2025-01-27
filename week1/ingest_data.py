#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from sqlalchemy import create_engine
import pandas as pd
from time import time

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_1 = params.table_1
    table_2 = params.table_2
    green_trip_data_url = params.green_trip_data_url
    zone_lookup_url = params.zone_lookup_url

    csv_green_trip = "green_tripdata_2019-10.csv.gz"
    zone_lookup = "taxi_zone_lookup.csv"

    os.system(f"wget {green_trip_data_url} -O {csv_green_trip}")
    os.system(f"wget {zone_lookup_url} -O {zone_lookup}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    df_iter = pd.read_csv(csv_green_trip, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_1, con=engine, if_exists='replace')
    df.to_sql(name=table_1, con=engine, if_exists='append')
    for df in df_iter:
        t_start = time()
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_1, con=engine, if_exists='append')
        t_end = time()
        print('inserted another chunk....., took %.3f second' % (t_end - t_start))

    df = pd.read_csv(zone_lookup)
    df.to_sql(name=table_2, con=engine, if_exists='replace')


if __name__ == '__main__':
    # main()
    print('inside of reading args')
    parser = argparse.ArgumentParser(description='Ingest CSV data to postgres')

    # user, password, host, port, database name, table name, url of the csv

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_1', help='name of the table where we will write the results to')
    parser.add_argument('--table_2', help='name of the table where we will write the results to')
    parser.add_argument('--green_trip_data_url', help='url of the csv file')
    parser.add_argument('--zone_lookup_url', help='url of the csv file')


    args = parser.parse_args()

    main(args)