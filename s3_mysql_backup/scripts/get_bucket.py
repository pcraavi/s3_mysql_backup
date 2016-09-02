import argparse
import os
from s3_mysql_backup import mkdirs
from s3_mysql_backup.s3_mysql_backup import s3_key

parser = argparse.ArgumentParser(
    description='S3 Download DB Bzckups')

parser.add_argument('project', help='project name',choices=['rrg', 'biz'])
parser.add_argument('--db-backups-dir', required=True, help='database backups directory',
                    default='/php-apps/cake.rocketsredglare.com/rrg/data/backups/')

parser.add_argument('--db-user', required=True, help='database user', default='marcdba')
parser.add_argument('--project-name', required=True, help='Project Name', default='rrg')
parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='php-apps-cluster')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY_ID', default='deadbeef')


def download_last_db_backup():
    """
    download last project db backup from S3
    """
    args = parser.parse_args()

    archive_file_extension = 'sql.bz2'
    if os.name == 'nt':
        raise NotImplementedError

    else:
        key, bucketlist = s3_key(**args)
