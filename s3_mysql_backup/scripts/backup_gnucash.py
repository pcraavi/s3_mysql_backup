import boto
import argparse
from datetime import datetime as dt
import re

from s3_mysql_backup import get_local_backups_by_pattern
from s3_mysql_backup import s3_bucket
from s3_mysql_backup.backup_file import gnu_file_date
from s3_mysql_backup.backup_file import delete_old_s3_gnu_backups
from s3_mysql_backup.backup_file import existing_backups

parser = argparse.ArgumentParser(description='S3 Gnucach Backups')

parser.add_argument('--gdir', help='Gnucash backup directory',
                    default='c:/Users/marc/Desktop/accounting/GnuCash/')
parser.add_argument('--bucket-name', help='Bucket Name', default='ameliaqb')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY', default='deadbeef')
parser.add_argument('--backup-aging-time', help='delete backups older than backup-aging-time', default=30)

pat = "Personal041008.[0-9]*.gnucash.[0-9]*.gnucash.[0-9]*.gnucash$"


def backup():
    args = parser.parse_args()
    bucket = s3_bucket(args)
    key = boto.s3.key.Key(bucket)
    #
    # Get list of local QB and Gnucash Backups
    #
    g_bks = get_local_backups_by_pattern(pat, args.gdir)
    s3_backups, s3_backup_names = existing_backups(bucket, pat)
    #
    # Check to see if backup files are already on S3
    #
    if len(g_bks) > 0:
        # add dates from filename
        for b in g_bks:
            b['date'] = gnu_file_date(b['filename'])
        # only consider last backup for upload
        g_bks = sorted(g_bks, key=lambda k: k['date'], reverse=True)
        if g_bks[0]['filename'] not in s3_backup_names:
            key.key = g_bks[0]['filename']
            print 'Uploading to bucket %s STARTING %s to %s: %s' % (
                args.bucket_name, g_bks[0]['filename'], key.key, dt.now())
            key.set_contents_from_filename(g_bks[0]['fullpath'])
            print 'Upload %s FINISHED: %s' % (g_bks[0]['filename'], dt.now())
    delete_old_s3_gnu_backups(args, s3_backups, bucket)

