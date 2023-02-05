# Install Dependencies and Execution Plan

**Note: See Prefect folder for the code**


Open the working directory where the requirements file is located to install the dependencies.

- $ pip install -r prefect-requirements.txt
- Start the Orion Server
   - $ prefect orion start

**Note: Make sure to run the terraform script on Week 1 to build the datalake and BigQuery resources.**

**Note: Make sure to run the terraform script on Week 1 to build the datalake and BigQuery resources.**

Copy the GCP credentials to follow this format
- $ cd ~ && mkdir -p ~/.gcp/
- $ cp <path to JSON file> ~/.gcp/credentials.json

### Create the PREFECT Cloud Account
- Add a prefect block with the GCP credentials
- Run terraform plan to get the GCP resource names

### Define the resource names that are needed
- GCS bucket name
    - ozkary_data_lake_ozkary-de-101
- Prefect Acc block name
    - blk-gcp-svc-acc
- Prefect GCS block name
    - blk-gcs_name
- Prefect Deployments
    - dep-docker-de-101
    - dep-docker-de-102
- GCP BigQuery dataset name (database alias)
    - trips_data
    - table = trips_data
- Docker container name after pushing to dockerhub
    - ozkary/prefect:de-101

- Copy the GCP credentials account

### Install the prefect blocks and install our custom blocks for GCP credentials and GCS access
- $ prefect block register -m prefect_gcp
- $ cd ./Step2-Orchestration/prefect/blocks
- $ python gcp_acc_block.py --file_path=/home/codespace/.gcp/ozkary-de-101.json --gcp_acc_block_name=blk-gcp-svc-acc
- $ python gcs_block.py --gcp_acc_block_name=blk-gcp-svc-acc --gcs_bucket_name=ozkary_data_lake_ozkary-de-101 --gcs_block_name=blk-gcs-name

### Create a docker image and push to DockerHub
- $ docker login --username USER --password PW
- $ docker image build -t ozkary/prefect:de-101 .
- $ docker image push ozkary/prefect:de-101

### Create the prefect block with the docker image
- $ cd ./Step2-Orchestration/prefect/blocks
- $ python docker_block.py --block_name=blk-docker-de-101 --image_name=ozkary/prefect:de-101

### Create the prefect deployments with the docker image
- $ cd ./Step2-Orchestration/prefect/flows
- $ python docker_deploy_etl_web_to_gcs.py --block_name=blk-docker-de-101 --deploy_name=dep-docker-de-gcs-101
- $ python docker_deploy_etl_gcs_to_bq.py --block_name=blk-docker-de-101 --deploy_name=dep-docker-de-gbq-101
- $ prefect deployments ls
- $ prefect agent start -q default
- $ prefect deployment run dep-docker-de-101 -p "year=2020 month=1 color=green block_name=blk-gcs-name"

### Manual Test Runs (Test Plan)
- $ python etl_web_to_gcs.py --year=2020 --month=1 --color=green --block_name=blk-gcs-name

$ python etl_gcs_to_bq.py --year=2019 --month=2 --color=yellow --block_name=blk-gcs-name --acc_block_name=blk-gcp-svc-acc --table_name=trips_data --project_id=

### Load the data from GCS to Google Big Query
- load the bigquery data

### GitHub storage block
- Create the Prefect Github block
  - name: blk-github-web-gcs