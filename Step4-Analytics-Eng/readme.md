# Overview of Analytics Engineering

**Note: Look at the dbt folder for the source code**

A data Analyst Engineer is a role in which there is expertise in software Engineer best practices as well as the ability to perform data analysts tasks. With the specialization of roles
of roles in Data Engineering, developers became either:

- Data Engineer
  
     This role is dedicated to the writing software for data pipelines and build the data warehose resources

- Data Analysts

     This specialization is for developers that use data analysis tools and create visualizations that can help provide insights for the data today or predictive
     analysis models for the future.

## Implementation Plan

We are using dbt (data build tools) to build the data analysis resources on BigQuery. With this tool, we can define the lookup, facts and dimensions table in a way 
that enables us to support a CICD process by rebuilding the project resources and pushing the changes to the cloud hosting environment.

### Create the Zone lookup seed table

This is where we can add lookup or dimensions tables by providing a CSV file and seeding the database.

  - Add the CSV file to the seeds folder
  - Add the properties.yml file to describe the seed file


### Create a macro file to provide content mapping for the payment type information

 Macros are reusable code snippets that can be importing into other SQL scripts. This makes the development modular, so we can reuse macros and packages.

  - In the macros folder, add the payment_type.sql
    - This file maps the payment type id to a description

  - Add the properties.yml file to describe the macro

### Create the models

  In the models folder, we create the folder and files for the process. 
  
  - staging
    This folder contains the raw data in the form of specialized views to make the different data sources uniforms. These files are used by the core files to build the
    actual tables.

    - Create the schema.yml file which provides the database connection information as well as the schema definition for the models
    - Add the models with the view materialization strategies. 
       - A view for each data source with a common field names as this goes into the fact tables

    - core   
     This folder hosts the resources that are made available for the data analysis process. This includes fact and dimension tables

     - Add the dimension zone tables from the zones lookup seed
     - Add the fact table with all the views as source
         - Use common table expressions to be able to join all the views
     - Add a summay or rollup table that agregates the measures from the fact table
        - Group by zone, month and service type
    - Add a schema.yml file to describe all the tables

### dbt Commands

- $ dbt deps 
  - Add the package dependencies in the packages.yml (root folder)   
  
  ``
    packages:
    - package: dbt-labs/dbt_utils
        version: 0.8.0
  ``

- $ dbt seed 
  - to create the seed tables/lookup with a CSV file
- $ dbt run --m <model.sql>
  - Run the model
- $ dbt test
  - Test your data: 
- $ dbt build 
  - Runs the seed, run and test at the same time
- $ dbt docs generate
  - Generate documentation 
- $ dbt debug --config-dir
  - To see the project folder configuration

## Execution Plan

### Configure your project with the profile information for the GCP BigQuery profile
- See the dbt_project.yml
- If the files are not at the root folder, add the path where the dbt folders should be located

```
# This setting configures which "profile" dbt uses for this project.
profile: 'Analytics'

model-paths: ["Step4-Analytics-Eng/dbt/models"]
analysis-paths: ["Step4-Analytics-Eng/dbt/analysis"]
test-paths: ["Step4-Analytics-Eng/dbt/tests"]
seed-paths: ["Step4-Analytics-Eng/dbt/seeds"]
macro-paths: ["Step4-Analytics-Eng/dbt/macros"]
snapshot-paths: ["Step4-Analytics-Eng/dbt/snapshots"]

```

### In the seeds folder, add the csv file and define the schema
Add any lookup resources that we may have on vsc files if not on the database already
- Add the csv files in the seeds folder
- Add the model name on the schema.yml file
- $ dbt seed
    - This should create a lookup table in the database with the name of your model

```
version: 2

seeds: 
  - name: taxi_zone_lookup
    description: >
      Taxi Zones roughly based on NYC Department of City Planning's ...
```

### Create the source model in the models/staging folder
- Update the stg_green_tripdata and stg_yellow_tripdata
    - Filter by years 2019, 2020
    - Cast the vendor id to int to avoid the float data type errors
- Add the model stg_fhv_tripdata.sql file 
- Add the schema.yml information
    - Make sure to add package dependencies (dbt deps) Only if using external macros
    - Make sure to add the database (project name), schema (dataset) and require tables
- Run $ dbt run --model stg_fhv_tripdata.sql
    - This creates a view in the database with a limit count
    - To remove the limit run 
        - $ dbt run --m <model.sql> --var 'is_test_run: false'

```

sources:
    - name: staging
      #For bigquery:
      database: ozkary-de-101

      # For postgres:
      # database: production

      schema: trips_data

      # loaded_at_field: record_loaded_at
      tables:
        - name: fhv_rides
         # freshness:
           # error_after: {count: 6, period: hour}

```

### Create the fact and dim model in the models/core folders
- Add the fact_fhv_trips.sql model
    - Use the stg_fhv_tripdata as the source
    - Add a partition on the pickup_datetime field
    - add a cluster on the affiliated_base_number
- Run $ dbt run --model fact_fhv_trips.sql 
    - By default, it will do a test run with a row count limit
    - This should create a fact table in the database
- Validate the table in the database
    - Select count(1) from fact_fhv_trips

### Build the entire model with a full dataset
- Build the fact tables (test run with row count limit)
    - $ dbt build --select m dim_zones 
    - $ dbt build --select fact_trips 
    - $ dbt build --select fact_fhv_trips
    - Validate the tables in BigQuery
    - $ dbt build  --var 'is_test_run: false'
- Build the entire dataset (no rowcount limit)    
    - $ dbt build  --var 'is_test_run: false'
    - Validate the tables in BigQuery
- Build the documentation
    - $ dbt docs generate


    
    
