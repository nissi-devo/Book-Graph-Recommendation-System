from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class ExtractFromS3Operator(BaseOperator):

    @apply_defaults
    def __init__(self, aws_conn_id, bucket_name, data_categ, year, *args, **kwargs):
        super(ExtractFromS3Operator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.data_categ = data_categ
        self.year = year

    def execute(self, context):
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        prefix = f"{self.data_categ}/{self.year}/"
        json_files = []
        continuation_token=None

        # Using the list_keys method provided by S3Hook
        while True:
            # List keys using S3Hook
            response = s3.list_keys(bucket_name=self.bucket_name, prefix=prefix)

            if response:
                json_files.extend(response)  # Extend with the keys
            else:
                break

            # Check if continuation is needed (for pagination)
            if 'IsTruncated' in response and response['IsTruncated']:
                continuation_token = response['NextContinuationToken']
            else:
                break

        # Push the list of JSON files to XCom
        context['ti'].xcom_push(key=f'{self.data_categ}_json_files', value=json_files)

