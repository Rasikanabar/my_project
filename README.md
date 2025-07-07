Welcome to the project -

Logic to get the data from google drive into MySQL is written in python and same file is available in ingest/data_connect.py folder.
In local MySQL database, data_ingestion database is created -
1) Data from google drive is written into issues_raw table
2) Tranformation is done in dbt core in staging(Clean and transformed data) and modeled(Week over week data) models.
3) Tests are implemented in schema.yml
