import argparse
from datetime import datetime as dt
from datetime import timedelta as td
import re
import boto
from s3_mysql_backup import get_local_backups_by_pattern
from s3_mysql_backup import s3_bucket

parser = argparse.ArgumentParser(description='S3 Quickbooks Backups')

parser.add_argument('--qdir', help='Quickbooks backup directory',
                    default='c:/Users/marc/Desktop/accounting/QuickBooks Backups/')
parser.add_argument('--bucket-name', required=True, help='Bucket Name', default='ameliaqb')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY', default='deadbeef')

parser.add_argument('--backup-aging-time', help='delete backups older than backup-aging-time', default=30)
#


def backup():

    bucket_list = []

    args = parser.parse_args()
    bucket = s3_bucket(args)
    key = boto.s3.key.Key(bucket)
    for f in bucket.list():
        bucket_list.append(f.name)
    #
    # Get list of local QB and Gnucash Backups
    #
    pat = "ROCKETS_REDGLARE_2005 \(Backup [A-Z][a-z][a-z] [0-9][0-9],[0-9][0-9][0-9][0-9]  [0-9][0-9] [0-9][0-9] " \
          "[A-Z][A-Z]\).QBB"
    qb_bks = get_local_backups_by_pattern(pat, args.qdir)
    #
    # Check to see if backup files are already on S3
    #
    if len(qb_bks) > 0:
        # add dates from filename
        for b in qb_bks:
            dstr = b['filename'][30:len(b['filename']) - 5]
            fdate = dt.strptime(dstr, '%b %d,%Y  %I %M %p')
            b['date'] = fdate
        qb_bks = sorted(qb_bks, key=lambda k: k['date'], reverse=True)
        if qb_bks[0]['filename'] not in bucket_list:
            key.key = qb_bks[0]['filename']
            print 'uploading STARTING %s to %s: %s' % (qb_bks[0]['filename'], key.key, dt.now())
            key.set_contents_from_filename(qb_bks[0]['fullpath'])
            print 'upload %s FINISHED: %s' % (qb_bks[0]['filename'], dt.now())

    # delete files over a month old

    bk_count = 0  # don't delete only backup
    for f in bucket.list():
        if re.match(pat, f.name):
            bk_count += 1
            dstr = f.name[args.backup_aging_time:len(f.name) - 5]
            fdate = dt.strptime(dstr, '%b %d,%Y  %I %M %p')
            if fdate < dt.now() - td(args.backup_aging_time) and bk_count > 1:
                bucket.delete_key(f.name)
                print 'Deleted %s ' % f.name

