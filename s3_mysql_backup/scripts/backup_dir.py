import argparse

from s3_mysql_backup.backup_dir import backup as s3_backup_dir

parser = argparse.ArgumentParser(description='S3 Datadir Backup')

parser.add_argument(
    '--datadir', required=True,
    help='datadir dir with cached data',
    default='/php-apps/cake.rocketsredglare.com/rrg/data/')
parser.add_argument('--zip-backups-dir', help='backup directory', default='/php-apps/db_backups/')

parser.add_argument('--s3-folder',  help='S3 Folder', default='')
parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='php-apps-cluster')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY_ID', default='deadbeef')
parser.add_argument('--backup-aging-time', help='delete backups older than backup-aging-time', default=30)

parser.add_argument('project', help='project name')


def backup():
    """
    zips into db_backups_dir and uploads to bucket_name/s3_folder
    fab -f ./fabfile.py backup_dbs
    """

    args = parser.parse_args()

    s3_backup_dir(
        args.datadir,
        args.aws_access_key_id,
        args.aws_secret_access_key,
        args.bucket_name,
        args.zip_backups_dir, args.backup_aging_time, args.s3_folder, args.project)

