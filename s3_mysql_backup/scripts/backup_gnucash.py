import boto
import argparse
from datetime import datetime as dt
from datetime import timedelta as td
import re

from s3_mysql_backup import get_local_backups_by_pattern
from s3_mysql_backup import s3_bucket

parser = argparse.ArgumentParser(description='S3 Gnucach Backups')

parser.add_argument('--gdir', help='Gnucash backup directory',
                    default='c:/Users/marc/Desktop/accounting/GnuCash/')
parser.add_argument('--bucket-name', help='Bucket Name', default='ameliaqb')
parser.add_argument('--aws-access-key-id', required=True, help='AWS_ACCESS_KEY_ID', default='rrg')
parser.add_argument('--aws-secret-access-key', required=True, help='AWS_SECRET_ACCESS_KEY', default='deadbeef')
parser.add_argument('--backup-aging-time', help='delete backups older than backup-aging-time', default=30)
#


def backup():

    bucket_list = []

    args = parser.parse_args()
    bucket = s3_bucket(args)

    for f in bucket.list():
        bucket_list.append(f.name)
    #
    # Get list of local QB and Gnucash Backups
    #
    pat = "Personal041008.[0-9]*.gnucash.[0-9]*.gnucash$"
    g_bks = get_local_backups_by_pattern(pat, args.gdir)

    #
    # Check to see if backup files are already on S3
    #

    if len(g_bks) > 0:
        # add dates from filename
        for b in g_bks:
            dstr = b['filename'][38:len(b['filename']) - 8]
            fdate = dt.strptime(dstr, '%Y%m%d%H%M%S')
            b['date'] = fdate
        g_bks = sorted(g_bks, key=lambda k: k['date'], reverse=True)
        if g_bks[0]['filename'] not in bucket_list:
            key.key = g_bks[0]['filename']
            print 'Uploading to bucket %s STARTING %s to %s: %s' % (
                args.bucket_name, g_bks[0]['filename'], key.key, dt.now())
            key.set_contents_from_filename(g_bks[0]['fullpath'])
            print 'Upload %s FINISHED: %s' % (g_bks[0]['filename'], dt.now())

    # delete files over a month old
    # don't delete last backup
    bk_count = 0
    for f in bucket.list():
        if re.match(pat, f.name):
            bk_count += 1
            dstr = f.name[38:len(f.name) - 8]
            fdate = dt.strptime(dstr, '%Y%m%d%H%M%S')
            if fdate < dt.now() - td(args.backup_aging_time) and bk_count > 1:
                bucket.delete_key(f.name)
                print 'Deleted %s ' % f.name
