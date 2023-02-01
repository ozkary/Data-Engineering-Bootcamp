Install Dependencies

Open the working directory where the requirements file is located

$ pip install -r prefect-requirements.txt
Start the Orion Server
$ prefect orion start

Note: Make sure to run the terraform script on Week 1 to build the datalake and BigQuery resources.
Create the PREFECT Cloud Account
Link your local preface to the cloud instance

Add a prefect block with the GCP credentials
