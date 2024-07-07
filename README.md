# icustomer-test

# Overview

This project implements an end-to-end data pipeline to process user interaction data. The data is ingested from a CSV file, cleaned and transformed using PySpark, and finally stored in a PostgreSQL database. The pipeline includes schema validation, data transformation, and storage optimization techniques to ensure efficient processing and retrieval of the interaction data.

## Steps

1. Data Ingestion

Data is ingested from a CSV file stored in AWS S3 into a PySpark DataFrame. The schema is defined explicitly to ensure proper data types and structure.

2. Data Cleaning

The cleaning process includes:

* Removing records with null values in critical columns (`interaction_id`, `user_id`, `product_id`, `action`, `timestamp`).
* Validating the timestamp format (`YYYY-MM-DD hh:Mi:ss`).
* Removing records where `interaction_id` or `user_id` are zero.
* Ensuring `interaction_id` is unique and non-null.

Invalid records are written to a separate directory in S3.

3. Data Transformation

The transformation process involves:

* Calculating the number of interactions per user and per product.
* Adding new columns `user_interaction_count` and `product_interaction_count` to the DataFrame.
* Combining these interaction counts into a single column `interaction_count`.

4. Data Storage

The cleaned and transformed data is written to a PostgreSQL database. The schema is designed to store all necessary fields and includes indexes and partitioning for performance optimization.

5. Data Retrieval

SQL queries are provided to retrieve insights from the stored data:

1. Total number of interactions per day.
2. Top 5 users by the number of interactions.
3. Most interacted products based on the number of interactions.
4. Optimization

Optimization techniques include creating indexes, partitioning the table, and running maintenance commands like `VACUUM` and `ANALYZE`.

## Description of Folders

- **task1/**: Contains scripts related ETL pipeline.
- **task1/input**: Contains input data .
- **task1/output**: Contains output data .
- **task1/src/**: Contains scripts related to the data pipeline.
- **task1/src/app**: Code entry point.
- **task1/src/randomDataGenerator**: Used to produce the random data for the .
- **task1/src/ingestion**: Script to read the CSV and push in stage area as is.
- **task1/src/dataCleaning**: Script to clean the data like schema valiation, null checks and segregate data in valid and invalid.
- **task1/src/tranformation**: apply the business logic.
- **task1/src/loader**: used to write the data to postgres SQL.
- **task1/output/stage**: Stage area for the input file .
- **task1/output/clean**: Clean dataset after the dataCleaning step.
- **task2/**: Contains scripts related ETL pipeline.
- **task2/data_pipeline.py**: Airflow DAG schedule to run on daily basis for ETl pipeline.
- **task2/Dockerfile**: Dockerfile to build on local and deploy.
- **task2/requirements.txt**: additional python requirement required in airflow.
- **task2/docker-compose.yaml**: docker compose file.
- **requirements.txt**: File listing all Python dependencies.
- **README.md**: Readme file describing the project.
- **.gitignore**: Git ignore file to exclude unnecessary files from version control.

## Setup and Usage

### Prerequisites

- Apache Spark
- PySpark
- AWS S3 access
- PostgreSQL
- JDBC driver for PostgreSQL

### Running the Data Pipeline

1. **Clone the repository:**

```sh
git clone https://github.com/your-username/user-interaction-data-pipeline.git
cd assignment
```

Run PySpark Code:

```sh
pyspark task1/app.py
```


Run Airflow on Local

```sh
cd task2/docker
docker compose up
```

**Minimum 4GB memory is required for airflow.**

check Airflow on local:http://localhost:8080

Note: We can need to deploy the airflow and spark integration to make the airflow DAG work. That can also be done by setting up the communication b/w airflow and spark cluster. 
