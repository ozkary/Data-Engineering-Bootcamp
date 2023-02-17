#!/usr/bin/env python
# coding: utf-8

import argparse
from pathlib import Path
import os
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


@task(name="clean data",log_prints=False)
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


@task(name="write locally")
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


@task(name="write to gcs")
def write_gcs(path: Path, block_name: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load(block_name)
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow(name="Flow - download the FHV data and send to GCS")
def etl_web_to_gcs(year: int, month: int, prefix: str, block_name: str, url: str) -> None:
    """The main ETL function"""
    dataset_file = f"{prefix}_{year}-{month:02}"
    dataset_url = f"{url}/{dataset_file}.csv.gz"
    print(dataset_url)

    df_iter = pd.read_csv(dataset_url, iterator=True, chunksize=150000) 
    if df_iter:
        path = f"../data/{prefix}"
        file_name = f"{dataset_file}.csv.gz"

        # remove an older file
        file_path = Path(f"{path}/{file_name}")
        if os.path.exists(file_path):
             print(f"File found {file_path}")
            #  write_gcs(file_path, block_name)
             return

        while True:
            try:
                df = next(df_iter)
                print(df.columns)                
                df_clean = clean(df)
                write_local(df_clean, path, file_name)
            except StopIteration as ex:
                print(f"Finished reading file {ex}")
                break
            except Exception as ex:
                print(f"Error found {ex}")
                return
                

        print(f"file was loaded {file_path}")
        # df_clean = clean(df)
        # path = write_local(df_clean, color, dataset_file)
        write_gcs(file_path, block_name)
        # os.remove(file_path)  
    else:
        print("dataframe failed")


# @flow()
# def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow") -> None:
#     for month in months:
#         etl_web_to_gcs(year, month, color)

@flow(name="Flow entry function - FHV files")
def main_flow(params) -> None:
    """entry point to import data into GCS"""    
    year = params.year
    month = params.month
    prefix = params.prefix
    block_name = params.block_name
    url = params.url
    try:
        if month == "0":
            for idx in range(1, 13):
                etl_web_to_gcs(year, idx, prefix, block_name, url)
        else:    
            etl_web_to_gcs(year, month, prefix, block_name, url)
    except Exception as ex:
        print(f"error found {ex}")

if __name__ == '__main__':
    """main entry point with argument parser"""
    parser = argparse.ArgumentParser(description='Updaload data to GCS fileformat: {prefix}_{year}-{month:02}.csv.gz')
    
    parser.add_argument('--year', required=True, help='File year ')
    parser.add_argument('--month', required=True, help='File month')
    parser.add_argument('--prefix', required=True, help='The file prefix')    
    parser.add_argument('--block_name', required=True, help='GCS block name')
    parser.add_argument('--url', required=True, help='The URL location')
        
    args = parser.parse_args()

    main_flow(args)
