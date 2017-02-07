import os
from datetime import datetime as dt
import boto

from s3_mysql_backup import s3_bucket


def copy_file(aws_access_key_id, aws_secret_access_key, bucket_name, file, s3_folder):
    """
    copies file to bucket s3_folder
    """

    #  Connect to the bucket

    bucket = s3_bucket(aws_access_key_id, aws_secret_access_key, bucket_name)
    key = boto.s3.key.Key(bucket)

    if s3_folder:
        target_name = '%s/%s' % (s3_folder, os.path.basename(file))
    else:
        target_name = os.path.basename(file)

    key.key = target_name
    print('Uploading %s to %s' % (file, target_name))
    key.set_contents_from_filename(file)
    print('Upload %s FINISHED: %s' % (file, dt.now()))
