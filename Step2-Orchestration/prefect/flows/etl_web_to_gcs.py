#!/usr/bin/env python
# coding: utf-8

import argparse
from pathlib import Path
import os.path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta

# remove to prevent erros on container  cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
@task(name="download the data", log_prints=True, retries=2)
def fetch(dataset_url: str, color: str, dataset_file: str ) -> Path:
    """Read taxi data from web into pandas DataFrame"""
    print(f"data url {dataset_url}")
    df_iter = pd.read_csv(dataset_url, iterator=True, chunksize=100000) 

    while True:
        try:
            df = next(df_iter)
            df_clean = clean(df)
            path = write_local(df_clean, color, dataset_file)

        except StopIteration:
            print("Finished creating parquet data")
            break
        

    return path


@task(name="clean data",log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task(name="write locally")
def write_local(df: pd.DataFrame, path: Path) -> Path:
    """Write DataFrame out locally as parquet file"""    

    if not os.path.isfile(path):
        df.to_parquet(path, compression="gzip", engine='fastparquet')
    else:
        df.to_parquet(path, compression="gzip", engine='fastparquet', append=True)
        
    return path


@task(name="write to gcs")
def write_gcs(path: Path, block_name: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load(block_name)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name="Flow - download the data and send to GCS")
def etl_web_to_gcs(year: int, month: int, color: str, block_name: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    #  examples:
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2019-03.csv.gz

    df_iter = pd.read_csv(dataset_url, iterator=True, chunksize=100000) 
    if df_iter:
        path = Path(f"../data/{color}/{dataset_file}.parquet")
        while True:
            try:
                df = next(df_iter)
                df_clean = clean(df)
                write_local(df_clean, path)
            except StopIteration:
                print("Finished creating parquet data")
                break

        print(f"dataframe was loaded {path}")
        # df_clean = clean(df)
        # path = write_local(df_clean, color, dataset_file)
        write_gcs(path, block_name)
    else:
        print("dataframe failed")


# @flow()
# def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow") -> None:
#     for month in months:
#         etl_web_to_gcs(year, month, color)

@flow(name="Flow entry function")
def main_flow(params) -> None:
    """entry point to import data into GCS"""    
    year = params.year
    month = params.month
    color = params.color
    block_name = params.block_name
    etl_web_to_gcs(year, month, color, block_name)
    
# if __name__ == "__main__":
#     color = "yellow"
#     months = [1, 2, 3]
#     year = 2021
#     etl_parent_flow(months, year, color)

if __name__ == '__main__':
    """main entry point with argument parser"""
    parser = argparse.ArgumentParser(description='Ingest CSV data to GCS fileformat: {color}_tripdata_{year}-{month:02}')
    
    parser.add_argument('--year', required=True, help='File year ')
    parser.add_argument('--month', required=True, help='File month')
    parser.add_argument('--color', required=True, help='File color or brand')    
    parser.add_argument('--block_name', required=True, help='GCS block name')
        
    args = parser.parse_args()

    main_flow(args)
