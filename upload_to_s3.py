from utils import get_client

def upload_s3(bucket_name, body, file):
    s3_client = get_client()
    res = s3_client.put_object(
        Bucket=bucket_name,
        Key=file,
        Body=body
    )
    return res