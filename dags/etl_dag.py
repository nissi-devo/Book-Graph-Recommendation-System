from airflow import DAG
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from airflow.operators import ExtractFromS3Operator, ConditionalLoadOperator

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

extract_books_task = ExtractFromS3Operator(
    task_id='extract_books',
    aws_conn_id='aws_default',
    bucket_name='book-reviews',
    data_categ='meta_books',  # This should be the same for both book and review tasks, change as needed
    year='2024',
    dag=dag
)

extract_reviews_task = ExtractFromS3Operator(
    task_id='extract_reviews',
    aws_conn_id='aws_default',
    bucket_name='book-reviews',
    data_categ='review_books',  # This should be the same for both book and review tasks, change as needed
    year='2024',
    dag=dag
)

load_books_task = ConditionalLoadOperator(
    task_id='load_books',
    aws_conn_id='aws_default',
    bucket_name='book-reviews',
    data_categ='meta_books',  # Process books
    neo4j_conn_uri=neo4j_uri,
    neo4j_user=neo4j_user,
    neo4j_password=neo4j_password,
    dag=dag,
)

load_reviews_task = ConditionalLoadOperator(
    task_id='load_reviews',
    aws_conn_id='aws_default',
    bucket_name='book-reviews',
    data_categ='review_books',  # Process reviews
    neo4j_conn_uri=neo4j_uri,
    neo4j_user=neo4j_user,
    neo4j_password=neo4j_password,
    dag=dag,
)

[extract_books_task, extract_reviews_task] >> [load_books_task, load_reviews_task]