import argparse

from s3_mysql_backup import copy_file

parser = argparse.ArgumentParser(description='copy file to S3 bucket/folder')

parser.add_argument('file', help='file to be copied', required=True)

parser.add_argument('--s3-folder', required=True, help='S3 Folder', default='rrg')
parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='php-apps-cluster')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY_ID', default='deadbeef')


#
# Databases to backup
#
# dbs = ['biz', 'personal', 'rrg', 'coppermine']


def backup_db():
    """
    dumps databases into /backups, uploads to s3, deletes backups older than a month
    fab -f ./fabfile.py backup_dbs
    """

    args = parser.parse_args()
    backup_project_db(**args)
