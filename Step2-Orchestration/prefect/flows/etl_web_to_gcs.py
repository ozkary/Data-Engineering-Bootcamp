#!/usr/bin/env python
# coding: utf-8

import argparse
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(name="download the data", retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    return df


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
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"../data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
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
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-01.csv.gz

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path, block_name)


@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow") -> None:
    for month in months:
        etl_web_to_gcs(year, month, color)

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
