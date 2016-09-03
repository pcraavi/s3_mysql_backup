import os
import re
import errno

import subprocess
from datetime import datetime as dt
from datetime import timedelta as td

YMD_FORMAT = '%Y-%m-%d'
TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M-%S'
DIR_CREATE_TIME_FORMAT = '%a %b %d %H:%M:%S %Y'


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


def delete_local_db_backups(pat, backup_dir='backups', backup_aging_time=30):
    #
    # Delete old local backups
    #

    backup_expiration_date = dt.now() - td(days=backup_aging_time)
    for dirName, subdirList, filelist in os.walk(backup_dir, topdown=False):
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