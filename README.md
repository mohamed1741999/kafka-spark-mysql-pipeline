# Kafka-Spark-MySQL Data Pipeline

## Project Overview
This project demonstrates a near-real-time data pipeline where data is fetched from an external API, processed using Apache Kafka and Apache Spark Streaming, and stored in a MySQL database. The workflow is orchestrated using Apache Airflow to run the pipeline hourly.

## Technologies Used

-Apache Kafka - Message broker for streaming data.

-Apache Spark Streaming - For real-time processing of data.

-MySQL - Relational database to store processed data.

-Apache Airflow - Workflow orchestration and scheduling.

-Python - Used for implementing Kafka producer and Spark job.

-Ubuntu 22.04 - Operating system environment.

## Project Workflow
1-Data Source:
  
  -The Kafka producer fetches data from the public API: https://jsonplaceholder.typicode.com/posts.
  -The fetched JSON data is sent to a Kafka topic.

2-Data Processing:

  -Spark Streaming reads the data from the Kafka topic.
  -Transforms the data as needed. 
  -Writes the transformed data into a MySQL database.

3-Orchestration:

  -Apache Airflow schedules the Spark job to run every hour.
  
  -Monitors the entire pipeline execution. 

## Project Structure

  ├── dags/
  │   ├── kafka_to_mysql_dag.py
  │   ├── kafka_to_mysql_DAG.py
  ├── config/
  │   ├── kafka_config.json
  ├── requirements.txt
  ├── README.md
