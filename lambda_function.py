import os
from download_from_gharchive import download_file
from upload_to_s3 import upload_s3
from utils import get_previous_file_name, get_next_file_name, set_bookmark


def lambda_handler(event, context):
    s3_bucket = os.environ.get('BUCKET_NAME')
    file_prefix = os.environ.get('FILE_PREFIX')
    bookmark_file_name = os.environ.get('BOOKMARK_FILE')
    baseline_file = os.environ.get('BASELINE_FILE')
    # os.environ.setdefault('AWS_PROFILE', 'default')

    while True:
        # Fetching the latest file bookmark from S3 bucket
        latest_file_name = get_previous_file_name(s3_bucket=s3_bucket, file_prefix=file_prefix, bookmark_file=bookmark_file_name, baseline_bookmark=baseline_file)
        # Generating next file name from the latest bookmark
        next_file_name = get_next_file_name(previous_file_name=latest_file_name)
        # Downloading the next file
        download_res = download_file(next_file_name)

        # If next file is available, then upload it to S3
        if download_res.status_code != 200:
            print("Invalid_File_name OR No new file available to download.")
            print(f"The status code of new file '{next_file_name}' is '{download_res.status_code}'.")
            break

        upload_res = upload_s3(
            bucket_name=s3_bucket,
            body=download_res.content,
            file=f'{file_prefix}/{next_file_name}'
        )
        print(f"File '{next_file_name}' successfully downloaded.")
        set_bookmark(s3_bucket=s3_bucket, file_prefix=file_prefix, bookmark_file=bookmark_file_name, new_bookmark_file=next_file_name)
        print(upload_res)
        print("-------- Bookmark updated --------")
    return "Completed"