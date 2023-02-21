# dbt project 

## Create the models

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
- $ dbt build --select <model.sql>
  - Runs the seed, run and test at the same time
- dbt build --select <model.sql> --var 'is_test_run: false'
  - Builds the model and uses variables to allow for the full dataset to be created
- $ dbt docs generate
  - Generate documentation 
- $ dbt debug --config-dir
  - To see the project folder configuration