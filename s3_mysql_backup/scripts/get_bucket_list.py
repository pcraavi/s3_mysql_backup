import argparse
from s3_mysql_backup.s3_mysql_backup import s3_conn

parser = argparse.ArgumentParser(
    description='S3 bucket list')

parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY', default='deadbeef')


def get_bucket_list():
    """
    Get list of S3 Buckets
    """
    args = parser.parse_args()
    for b in s3_conn(args).get_all_buckets():
        print(''.join([i if ord(i) < 128 else ' ' for i in b.name]))


