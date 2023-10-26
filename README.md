# OpenAQ API ETL Pipeline Project

## Project Summary
This project is an ETL pipeline project that is used to extract data from the OpenAQ API, transform the data, and load the data into a  psql database. 
```
- Ingest data from the OpenAQ API (https://docs.openaq.org/docs) to retrieve data on the current air quality around the world.
- Transform the data to filter only data from Jakarta,Indonesia
- Transform it to a format that can be stored in a database table.
- Create a PostgreSQL connection that can be used to ingest data into a database.
- Store the transformed data into a new database table.
- Schedule the DAG to run daily at 4am.
```

## Tech Stack
- Docker
- Airflow
- PostgreSQL

## How to Run
Run this project using `docker compose up` command in the root directory of this project.
