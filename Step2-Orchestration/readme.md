Install Dependencies

Open the working directory where the requirements file is located

$ pip install -r prefect-requirements.txt
Start the Orion Server
$ prefect orion start

Note: Make sure to run the terraform script on Week 1 to build the datalake and BigQuery resources.
Create the PREFECT Cloud Account
Add a prefect block with the GCP credentials

Define the resource names that are needed
- GCS bucket name
    - Check GCP for the name
- Prefect Acc block name
    - blk-gcp-svc-acc
- Prefect GCS block name
    - blk-gcs_name
- GCP BigQuery dataset name
    - Trips_data
- Docker container name
    - This is used by the deployment scripts

Install the prefect blocks
$ cd ./Step2-Orchestration/prefect/blocks
$ python gcp_acc_block.py —file_path –gcp_block_acc_name
$ python gcs_block.py –gcp_block_acc_name –gcs_bucket_name –gcs_block_name

Create a docker image and push to DockerHub
