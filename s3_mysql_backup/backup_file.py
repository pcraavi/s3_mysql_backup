from datetime import datetime as dt
from datetime import timedelta as td
import re
"""
utilities for aiding gnucash/quickbooks backups
"""

gnu_date_format = '%Y%m%d%H%M%S'
qb_date_format = '%b %d,%Y  %I %M %p'


def gnu_file_date(fname):
    return dt.strptime(fname[61:len(fname) - 8], gnu_date_format)


def qb_file_date(fname):
    return dt.strptime(fname[30:len(fname) - 5], qb_date_format)


def delete_old_s3_gnu_backups(args, s3_backups, bucket):
    # don't delete last 2 without regard to age
    # don't delete < args.backup_aging_time
    for f in s3_backups[0:len(s3_backups) - 2]:
        if gnu_file_date(f.name) < dt.now() - td(args.backup_aging_time):
            bucket.delete_key(f.name)
            print 'Deleted %s ' % f.name


def delete_old_s3_qb_backups(args, s3_backups, bucket):
    # don't delete last 2 without regard to age
    # don't delete < args.backup_aging_time
    for f in s3_backups[0:len(s3_backups) - 2]:
        if qb_file_date(f.name) < dt.now() - td(args.backup_aging_time):
            bucket.delete_key(f.name)
            print 'Deleted %s ' % f.name


def existing_backups(bucket, pat):
    s3_backups = []
    s3_backup_names = []
    for f in bucket.list():
        if re.match(pat, f.name):
            s3_backups.append(f)
            s3_backup_names.append(f.name)
    return s3_backups, s3_backup_names
