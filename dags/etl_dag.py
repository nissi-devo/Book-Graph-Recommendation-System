from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import sys
from dotenv import load_dotenv

from operators.extract_from_s3_operator import ExtractFromS3Operator
from operators.load_to_neo4j_operator import ConditionalLoadOperator

load_dotenv()


# Initialize the Neo4j driver
neo4j_uri = "bolt://localhost:7687"
neo4j_user = os.getenv("NEO4J_USER")
neo4j_password = os.getenv("NEO4J_PASSWORD")

default_args = {
    'owner': 'nissi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'book_reviews_etl',
    default_args=default_args,
    description='ETL pipeline for Book Recommendations',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 23),
    catchup=False,
)

# TaskGroup for Extraction Tasks
with TaskGroup('extraction_group', dag=dag) as extraction_group:
    extract_books_task = ExtractFromS3Operator(
        task_id='extract_books',
        aws_conn_id='aws_default',
        bucket_name='book-reviews',
        data_categ='meta_books',
        year='2024',
        dag=dag
    )

    extract_reviews_task = ExtractFromS3Operator(
        task_id='extract_reviews',
        aws_conn_id='aws_default',
        bucket_name='book-reviews',
        data_categ='review_books',
        year='2024',
        dag=dag
    )

# TaskGroup for Loading Tasks
with TaskGroup('loading_group', dag=dag) as loading_group:
    load_books_task = ConditionalLoadOperator(
        task_id='load_books',
        aws_conn_id='aws_default',
        bucket_name='book-reviews',
        data_categ='meta_books',  # Process books
        neo4j_conn_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        dag=dag
    )

    load_reviews_task = ConditionalLoadOperator(
        task_id='load_reviews',
        aws_conn_id='aws_default',
        bucket_name='book-reviews',
        data_categ='review_books',  # Process reviews
        neo4j_conn_uri=neo4j_uri,
        neo4j_user=neo4j_user,
        neo4j_password=neo4j_password,
        dag=dag
    )

# Define dependencies between TaskGroups
extraction_group >> loading_group