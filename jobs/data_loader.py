"""
This script is to be run once. It splits up the large JSON files into smaller JSON files and stores them in S3, organizing the files by date beginning with the current dat
"""
import boto3
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import re

# Load environment variables from .env file
load_dotenv()
# S3 bucket and object settings

def upload_to_s3(data, date, base_name, s3, bucket_name):
    json_data = json.dumps(data, indent=2)
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    file_name = f'{base_name}/{year}/{month}/{day}.json'
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=json_data)
    print(f'Uploaded {file_name} to S3.')



def load_data(base_file_name):
    bucket_name = 'book-reviews'
    # Local file path
    local_file_path = f'./data/{base_file_name}'
    # Access AWS credentials
    aws_access_key = os.getenv('AWS_ACCESS_KEY')
    aws_secret = os.getenv('AWS_SECRET')

    # Connect to S3
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret,
                      region_name='us-east-1'
                      )

    # Read the content of the file
    with open(local_file_path, 'r') as file:
        content = file.read()

    # Replace single quotes with double quotes
    # This method might not handle all edge cases perfectly, but it's a start
    content = re.sub(r"'", r'"', content)
    # Split content by newlines to get individual JSON objects
    lines = content.strip().split('\n')

    # Initialize variables for date and chunking
    current_date = datetime.now()
    chunk = []
    chunk_size = 0
    max_file_size = 5 * 1024 * 1024  # 5 MB for each chunk
    # Process each JSON object
    for line in lines:
        try:
            data = json.loads(line)
            data_size = len(json.dumps(data).encode('utf-8')) # calculates the length of this byte sequence

            # Check if adding this object would exceed the max file size
            if chunk_size + data_size > max_file_size:
                # Upload current chunk to S3
                upload_to_s3(chunk, current_date, base_file_name.split('.')[0], s3, bucket_name)
                # Reset chunk and chunk_size, and decrement the date
                chunk = []
                chunk_size = 0
                current_date -= timedelta(days=1)

            # Add the current data to the chunk
            chunk.append(data)
            chunk_size += data_size
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")

    # Upload any remaining data after the end of the loop
    if chunk:
        upload_to_s3(chunk, current_date, base_file_name.split('.')[0], s3, bucket_name)


if __name__ == "__main__":
    #Metadata about books
    load_data('meta_books.json')
    #Book reviews
    load_data('review_books.json')