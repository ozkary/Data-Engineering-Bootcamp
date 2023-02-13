# Overview of Analytics Engineering

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
     - Add the fac table with all the views as source
         - Use common table expressions to be able to join all the views
     - Add a summay or rollup table that agregates the measures from the fact table
        - Group by zone, month and service type
    - Add a schema.yml file to describe all the tables

    

    
    
