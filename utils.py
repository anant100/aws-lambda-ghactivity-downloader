from datetime import datetime as dt
from datetime import timedelta as td
import boto3
from botocore.errorfactory import ClientError


def get_client():
    return boto3.client('s3')


def set_bookmark(s3_bucket, file_prefix, bookmark_file, new_bookmark_file):
    s3_client = get_client()
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=f'{file_prefix}/{bookmark_file}',
        Body=new_bookmark_file.encode('utf-8')
    )


def get_previous_file_name(s3_bucket, file_prefix, bookmark_file, baseline_bookmark):
    s3_client = get_client()
    try:
        bookmark_file = s3_client.get_object(
            Bucket=s3_bucket,
            Key=f'{file_prefix}/{bookmark_file}'
        )
        bookmark_content = bookmark_file['Body'].read().decode('utf-8')
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            bookmark_content = baseline_bookmark
    return bookmark_content


def get_next_file_name(previous_file_name):
    dt_part = previous_file_name.split('.')[0]
    next_file = f"{dt.strftime(dt.strptime(dt_part, '%Y-%M-%d-%H') + td(hours=1), '%Y-%M-%d-%-H')}.json.gz"
    return next_file
