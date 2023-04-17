# Recruitment ETL Pipeline

## Introduction
- This project uses Apache Spark (with PySpark) to Extract-Transform-Load (ETL) data from a Data Lake (CassandraDB) into a Data Warehouse (MySQL database) with Star-schema for a Recruitment system. The data is processed using *Batch Processing*.
- The project also uses Docker to install and run the services.

## Requirements
- Docker
- Docker Compose

## Installation
- Clone the project from the Github repository:
```bash
git clone https://github.com/shinie19/Recruitment_ETL_Pipeline.git
```
- Start the Docker containers using Docker Compose:
```bash
docker-compose up -d
```
## Result
- Raw data:

![image](https://user-images.githubusercontent.com/57434654/232560334-d2d37059-3b07-4efc-bcc6-2618d0159749.png)

- Final data:

![image](https://user-images.githubusercontent.com/57434654/232559286-8bd17cb4-d05f-4779-9fc7-f1c795bfe8d0.png)
