#!/usr/bin/env python
# coding: utf-8

import argparse
from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(name="Return the paqrquet file path from GCS", retries=3)
def extract_from_gcs(color: str, year: int, month: int, block_name: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load(block_name)
    gcs_block.get_directory(from_path=gcs_path, local_path="./data/")
    return Path(f"data/{gcs_path}")


@task(name="Data trasnformation task")
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    # df["passenger_count"].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df


@task(name="Write data to GBQ", log_prints=True)
def write_bq(df: pd.DataFrame, acc_block_name: str, table_name: str, project_id: str) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load(acc_block_name)

    df.to_gbq(
        destination_table=table_name,
        project_id=project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(name="Mode data from GCS to GBQ")
def etl_gcs_to_bq(year: int, month: int, color: str, block_name: str, acc_block_name: str, table_name: str, project_id: str) -> None:
    """Main ETL flow to load data into Big Query"""    
    path = extract_from_gcs(color, year, month, block_name)
    df = transform(path)
    write_bq(df, acc_block_name, table_name, project_id)

@flow(name="Flow entry function")
def main_flow(params) -> None:
    """entry point to import data into GCS"""    
    year = params.year
    month = params.month
    color = params.color
    block_name = params.block_name
    acc_block_name = params.acc_block_name
    table_name = params.table_name
    project_id = params.project_id
    
    etl_gcs_to_bq(year, month, color, block_name, acc_block_name, table_name, project_id)
    
if __name__ == "__main__":
    etl_gcs_to_bq()
    

if __name__ == '__main__':
    """main entry point with argument parser"""
    parser = argparse.ArgumentParser(description='Ingest parquet data to BigQuery: {color}_tripdata_{year}-{month:02}')
    
    parser.add_argument('--year', required=True, help='File year ')
    parser.add_argument('--month', required=True, help='File month')
    parser.add_argument('--color', required=True, help='File color or brand')    
    parser.add_argument('--block_name', required=True, help='GCS block name')
    parser.add_argument('--acc_block_name', required=True, help='GCP account block name')
    parser.add_argument('--table_name', required=True, help='GBQ table name')
    parser.add_argument('--project_id', required=True, help='GCP Project id')
        
    args = parser.parse_args()

    main_flow(args)