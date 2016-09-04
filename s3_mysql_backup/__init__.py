import os
import re
import errno
import operator
import subprocess
from datetime import datetime as dt
from datetime import timedelta as td

import boto

YMD_FORMAT = '%Y-%m-%d'
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M-%S'
DIR_CREATE_TIME_FORMAT = '%a %b %d %H:%M:%S %Y'


def s3_conn(args):
    return boto.connect_s3(args.aws_access_key_id, args.aws_secret_access_key, is_secure=False)


def s3_bucket(args):
    """
    connect to S3, return connection key and bucket list
    """

    return s3_conn(args).get_bucket(args.bucket_name)


def get_local_backups_by_pattern(pat, dir):
    bks = []

    for dirname, dirnames, filenames in os.walk(dir):
        #
        for filename in filenames:
            if re.match(pat, filename):
                bks.append(
                    {
                        'fullpath': os.path.join(dirname, filename),
                        'filename': filename
                    }
                )
    return bks


def delete_expired_backups_in_bucket(bucket, bucketlist, pat, backup_aging_time=30):

    backup_expiration_date = dt.now() - td(days=backup_aging_time)
    for f in bucketlist:
        filename = os.path.basename(f.name)

        if re.match(pat, os.path.basename(filename)):
            bk_date = dt.strptime(os.path.basename(filename)[0:19], TIMESTAMP_FORMAT)
            if bk_date < backup_expiration_date:
                print 'Removing old S3 backup %s' % filename

                bucket.delete_key(f.name)


def download_last_db_backup(args):
    matches = []
    for f in args.bucketlist:
        if re.match(args.pat, f.name):
            bk_date = dt.strptime(f.name[0:19], TIMESTAMP_FORMAT)
            matches.append({
                'key': f,
                'file': f.name,
                'date': bk_date
            })
    if matches:
        last_bk = sorted(matches, key=operator.itemgetter('date'))[0]
        dest = os.path.join(args.db_backups_dir, last_bk.file)
        if not os.path.exists(dest):

            last_bk.get_contents_to_filename(dest)
            print('Downloaded %s to %s' % (f.name, dest))
        else:
            print('Last backup %s already exists' % dest)


def delete_local_db_backups(pat, args):
    #
    # Delete old local backups
    #

    backup_expiration_date = dt.now() - td(days=args.backup_aging_time)
    for dirName, subdirList, filelist in os.walk(args.db_backups_dirFd, topdown=False):
        for f in filelist:
            if re.search(pat, f):
                bk_date = dt.strptime(f[0:19], TIMESTAMP_FORMAT)
                if bk_date < backup_expiration_date:
                    print 'Removing old local backup %s' % f
                    os.remove(os.path.join(dirName, f))


def mkdirs(path, writeable=False):

    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
    if not writeable:

        subprocess.call(['chmod', '0755', path])
    else:
        subprocess.call(['chmod', '0777', path])