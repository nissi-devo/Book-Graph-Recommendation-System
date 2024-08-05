<h1><strong>Book Recommendation Data Pipeline</strong></h1>  

<h2><strong>Overview</strong></h2>

This repository contains a comprehensive data pipeline for creating a book recommendation system. The pipeline processes book metadata and reviews in JSON format, partitions and stores the data in S3 buckets, transforms the data, and loads it into a Neo4j database. The Neo4j database is then queried to serve recommendations through a FastAPI endpoint.

Introduction
This project is designed to provide an end-to-end solution for creating a book recommendation system. The pipeline takes raw book metadata and review data in JSON format, processes it, and stores it in S3 buckets. The data is then transformed and loaded into a Neo4j graph database, which is used to generate book recommendations. The recommendations are served via a FastAPI endpoint.

Architecture
The data pipeline consists of the following stages:

Data Ingestion: Collects book metadata and reviews in JSON format and partitions the data into chunks stored in S3 buckets.
Data Transformation: Extracts data from S3, transforms it, and loads it into a Neo4j graph database.
Data Serving: Uses FastAPI to serve book recommendations based on the Neo4j database.


<h2><strong>Technologies Used</strong></h2><br>

**Amazon S3**: For storage of raw and processed data.
**Apache Spark**: For data transformation and partitioning.
**Neo4j**: Graph database for storing book metadata and relationships.
**FastAPI**: For serving book recommendations.
**Docker**: For containerization of the application components.
**Apache Airflow**: For cataloging and ETL processes (optional). <br>

<h2><strong>Setup</strong></h2><br>
**Prerequisites**
Docker
Docker Compose
AWS Account (with S3 and IAM setup)
Python 3.7+

