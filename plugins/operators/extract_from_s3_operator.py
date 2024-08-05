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
        continuation_token = None
        json_files = []

        while True:
            list_params = {'Bucket': self.bucket_name, 'Prefix': prefix}
            if continuation_token:
                list_params['ContinuationToken'] = continuation_token
            response = s3.list_keys(bucket_name=self.bucket_name, prefix=prefix, continuation_token=continuation_token)
            if response:
                json_files.extend(response)
            if 'IsTruncated' in response and response['IsTruncated']:
                continuation_token = response['NextContinuationToken']
            else:
                break

        context['ti'].xcom_push(key=f'{self.data_categ}_json_files', value=json_files)
