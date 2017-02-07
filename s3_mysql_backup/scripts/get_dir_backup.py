import argparse

from s3_mysql_backup.backup_dir import s3_get_dir_backup

parser = argparse.ArgumentParser(description='S3 Get Datadir Backup')

parser.add_argument('--zip-backups-dir', help='backup directory', default='/php-apps/db_backups/')

parser.add_argument('--s3-folder',  help='S3 Folder', default='')
parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='php-apps-cluster')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY_ID', default='deadbeef')

parser.add_argument('project', help='project name')


def get_dir_backup():
    """
    retrieves directory backup
    """
    args = parser.parse_args()
    s3_get_dir_backup(
        args.aws_access_key_id,
        args.aws_secret_access_key,
        args.bucket_name,
        args.s3_folder,
        args.zip_backups_dir, args.project)

