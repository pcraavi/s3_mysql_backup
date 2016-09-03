import argparse

from s3_mysql_backup.copy_file import copy_file

parser = argparse.ArgumentParser(description='copy file to S3 bucket/folder')
parser.add_argument('--s3-folder', help='S3 Folder')
parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='php-apps-cluster')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY', default='deadbeef')
parser.add_argument('file', help='file to be copied')
#
# Databases to backup
#
# dbs = ['biz', 'personal', 'rrg', 'coppermine']


def cp_file():
    """
    dumps databases into /backups, uploads to s3, deletes backups older than a month
    fab -f ./fabfile.py backup_dbs
    """

    args = parser.parse_args()
    copy_file(args)
