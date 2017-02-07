import argparse

from s3_mysql_backup import s3_bucket

parser = argparse.ArgumentParser(
    description='S3 bucket listing')

parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='php-apps-cluster')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY', default='deadbeef')


def get_bucket():
    """
    Get listing of S3 Bucket
    """
    args = parser.parse_args()

    bucket = s3_bucket(args.aws_access_key_id, args.aws_secret_access_key, args.bucket_name)
    for b in bucket.list():
        print(''.join([i if ord(i) < 128 else ' ' for i in b.name]))
