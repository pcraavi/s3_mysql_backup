import os
import re
import errno

import subprocess
from datetime import datetime as dt
from datetime import timedelta as td
import boto
from boto.s3.key import Key

from fabric.operations import local

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


def delete_expired_backups_in_bucket(bucket, bucketlist, FILEPATTERN, backup_aging_time=30):

    backup_expiration_date = dt.now() - td(days=backup_aging_time)
    for f in bucketlist:
        filename = os.path.basename(f.name)

        if re.match(FILEPATTERN, os.path.basename(filename)):
            bk_date = dt.strptime(os.path.basename(filename)[0:19], TIMESTAMP_FORMAT)
            if bk_date < backup_expiration_date:
                print 'Removing old S3 backup %s' % filename

                bucket.delete_key(f.name)


def delete_local_db_backups(FILEPATTERN, backup_dir='backups', backup_aging_time=30):
    #
    # Delete old local backups
    #

    backup_expiration_date = dt.now() - td(days=backup_aging_time)
    for dirName, subdirList, filelist in os.walk(backup_dir, topdown=False):
        for f in filelist:
            if re.search(FILEPATTERN, f):
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


def backup_project_db(db_pass, mysql_host, mysql_port,  aws_access_key_id, aws_secret_access_key, bucket_name,
        project_name='biz', db_backups_dir='backups', s3folder="mysql-backups", backup_aging_time=30):
    """
    dumps database into /backups, uploads to s3, deletes backups older than a month

    """
    #  Connect to the bucket
    #
    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    if conn.lookup(bucket_name):
        print '----- bucket already exists! -----'
    else:
        print '----- creating bucket -----'
        conn.create_bucket(bucket_name)

    bucket = conn.get_bucket(bucket_name)
    key = boto.s3.key.Key(bucket)

    bucketlist = bucket.list()

    FILEPATTERN = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.sql.bz2"\
                  % project_name

    sql_file = '%s-%s.sql' % (dt.now().strftime(TIMESTAMP_FORMAT), project_name)
    print 'Dumping database %s to %s.bz2' % (project_name, sql_file)

    sql_full_target = os.path.join(db_backups_dir, sql_file)
    local('mysqldump -h"%s" -P"%s" -uroot -p"%s" %s > %s' % (
        mysql_host, mysql_port, db_pass, project_name, sql_full_target))
    local('bzip2 %s' % sql_full_target)
    # append '.bz2'
    sql_local_full_target = sql_full_target
    sql_full_target = '%s.bz2' % os.path.join(db_backups_dir, sql_file)
    target_name = os.path.join(s3folder, os.path.basename(sql_full_target))

    key.key = target_name
    print 'uploading STARTING %s to %s: %s' % (sql_file, target_name, dt.now())
    try:
        key.set_contents_from_filename(sql_full_target)
        print 'upload %s FINISHED: %s'%(sql_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, FILEPATTERN, backup_aging_time=backup_aging_time)
        delete_local_db_backups(FILEPATTERN, backup_aging_time=backup_aging_time)

