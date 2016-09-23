import argparse
from datetime import datetime as dt
import re
import boto
from s3_mysql_backup import get_local_backups_by_pattern
from s3_mysql_backup import s3_bucket
from s3_mysql_backup.backup_file import delete_old_s3_qb_backups
from s3_mysql_backup.backup_file import qb_file_date
from s3_mysql_backup.backup_file import existing_backups

parser = argparse.ArgumentParser(description='S3 Quickbooks Backups')

parser.add_argument('--qdir', help='Quickbooks backup directory',
                    default='c:/Users/marc/Desktop/accounting/QuickBooks Backups/')
parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='ameliaqb')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY', default='deadbeef')

parser.add_argument('--backup-aging-time', help='delete backups older than backup-aging-time', default=30)

pat = "ROCKETS_REDGLARE_2005 \(Backup [A-Z][a-z][a-z] [0-9][0-9],[0-9][0-9][0-9][0-9]  [0-9][0-9] [0-9][0-9] " \
      "[A-Z][A-Z]\).QBB"


def backup():

    args = parser.parse_args()
    bucket = s3_bucket(args)
    key = boto.s3.key.Key(bucket)
    #
    # Get list of local QB and Gnucash Backups
    #
    qb_bks = get_local_backups_by_pattern(pat, args.qdir)

    s3_backups, s3_backup_names = existing_backups(bucket, pat)
    #
    # Check to see if backup files are already on S3
    #
    if len(qb_bks) > 0:
        # add dates from filename
        for b in qb_bks:
            b['date'] = qb_file_date(b['filename'])
        # only consider last backup for upload
        qb_bks = sorted(qb_bks, key=lambda k: k['date'], reverse=True)
        if qb_bks[0]['filename'] not in s3_backup_names:
            key.key = qb_bks[0]['filename']
            print 'uploading STARTING %s to %s: %s' % (qb_bks[0]['filename'], key.key, dt.now())
            key.set_contents_from_filename(qb_bks[0]['fullpath'])
            print 'upload %s FINISHED: %s' % (qb_bks[0]['filename'], dt.now())

    delete_old_s3_qb_backups(args, s3_backups, bucket)
