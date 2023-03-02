#!/usr/bin/env python
# coding: utf-8

import argparse
from pathlib import Path
import os
import pandas as pd
# from prefect import flow, task
# from prefect_gcp.cloud_storage import GcsBucket
# from prefect.tasks import task_input_hash
from datetime import timedelta

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.columns)    
    pickupColName = "pickup_datetime"
    dropoffColName = "dropOff_datetime"
    
    if pickupColName in df.columns:
        df[pickupColName] = pd.to_datetime(df[pickupColName])

    if dropoffColName in df.columns:        
        df[dropoffColName] = pd.to_datetime(df[dropoffColName])
    
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df

def write_local(df: pd.DataFrame, folder: str, file_name: str) -> Path:
    """Write DataFrame out locally as parquet file"""

    path = Path(f"{folder}")
    if not os.path.exists(path):
        path.mkdir(parents=True, exist_ok=True)
    
    file_path = Path(f"{folder}/{file_name}")

    if not os.path.isfile(file_path):
        df.to_csv(file_path, compression="gzip")
    else:
        df.to_csv(file_path, header=None, compression="gzip", mode="a", )
        
    return file_path


def etl_web_to_gcs(year: int, month: int, prefix: str, url: str) -> None:
    """The main ETL function"""
    dataset_file = f"{prefix}_{year}-{month:0>2}"
    dataset_url = f"{url}/{dataset_file}.csv.gz"
    print(dataset_url)    

    df_iter = pd.read_csv(dataset_url, iterator=True, chunksize=10000) 
    if df_iter:
        path = f"../data/{prefix}"
        file_name = f"{dataset_file}.csv.gz"

        # remove an older file
        file_path = Path(f"{path}/{file_name}")
        if os.path.exists(file_path):
             print(f"File found {file_path}")
            #  write_gcs(file_path, block_name)
             return

        for df in df_iter:
        # while True:
            try:                
                # df = next(df_iter)
                print(df.columns)                
                # df_clean = clean(df)
                write_local(df, path, file_name)
            except StopIteration as ex:
                print(f"Finished reading file {ex}")
                break
            except Exception as ex:
                print(f"Error found {ex}")
                return
                
        print(f"file was loaded {file_path}")
        # write_gcs(file_path, block_name)
        # os.remove(file_path)  
    else:
        print("dataframe failed")



def main_flow(params) -> None:
    """entry point to import data into GCS"""    
    year = params.year
    month = params.month
    prefix = params.prefix    
    url = params.url
    try:
        if month == "0":
            for idx in range(1, 13):
                etl_web_to_gcs(year, idx, prefix, url)
        else:    
            etl_web_to_gcs(year, month, prefix, url)
    except Exception as ex:
        print(f"Error found {ex}")

if __name__ == '__main__':
    """main entry point with argument parser"""
    os.system('clear')
    print('running...')
    parser = argparse.ArgumentParser(description='Download data locally : {url}/{prefix}_{year}-{month:02}.csv.gz')
    
    parser.add_argument('--year', required=True, help='File year ')
    parser.add_argument('--month', required=True, help='File month')
    parser.add_argument('--prefix', required=True, help='The file prefix')     
    parser.add_argument('--url', required=True, help='The URL location')
    
    args = parser.parse_args()

    main_flow(args)
    print('end')

#  usage:
#  python3 web_to_local.py --prefix=fhvhv_tripdata --year=2021 --month=6 --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv
#  https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz