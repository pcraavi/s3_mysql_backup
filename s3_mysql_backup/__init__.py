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


def s3_key(bucket_name='php-apps-cluster'):
    """
    connect to S3, return connection key and bucket list
    """

    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    if conn.lookup(bucket_name):
        print('----- bucket already exists! -----')
    else:
        print('----- creating bucket -----')
        conn.create_bucket(bucket_name)

    bucket = conn.get_bucket(bucket_name)

    return boto.s3.key.Key(bucket), bucket.list()


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


def download_last_db_backup(db_backups_dir='backups', project_name='biz', bucket_name='php-apps-cluster'):
    """
    download last project db backup from S3
    """
    archive_file_extension = 'sql.bz2'
    if os.name == 'nt':
        raise NotImplementedError

    else:
        key, bucketlist = s3_key(bucket_name=bucket_name)

        TARFILEPATTERN = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.%s" % \
                         (project_name, archive_file_extension)

        #
        # delete files over a month old, locally and on server
        #
        backup_list = []
        for f in bucketlist:
            parray = f.name.split('/')
            filename = parray[len(parray)-1]
            if re.match(TARFILEPATTERN, filename):
                farray = f.name.split('/')
                fname = farray[len(farray)-1]
                dstr = fname[0:19]

                fdate = dt.strptime(dstr, "%Y-%m-%d-%H-%M-%S")
                backup_list.append({'date': fdate, 'key': f})

        backup_list = sorted(
            backup_list, key=lambda k: k['date'], reverse=True)

        if len(backup_list) > 0:
            last_backup = backup_list[0]
            keyString = str(last_backup['key'].key)
            basename = os.path.basename(keyString)
            # check if file exists locally, if not: download it

            mkdirs(db_backups_dir)

            dest = os.path.join(db_backups_dir, basename)
            print('Downloading %s to %s' % (keyString, dest))
            if not os.path.exists(dest):
                with open(dest, 'wb') as f:
                    last_backup['key'].get_contents_to_file(f)
            return last_backup['key']
        else:
            print 'There is no S3 backup history for project %s' % project_name
            print 'In ANY Folder of bucket %s' % bucket_name


def backup_project_db(
        project_name='biz', db_backups_dir='backups', bucket_name='php-apps-cluster', s3folder="mysql-backups",
        backup_aging_time=30):
    """
    dumps database into /backups, uploads to s3, deletes backups older than a month

    """
    try:
        AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
        AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        host = os.getenv('MYSQL_PORT_3306_TCP_ADDR')
        port = os.getenv('MYSQL_PORT_3306_TCP_PORT')
        rootpw = os.getenv('MYSQL_ENV_MYSQL_ROOT_PASSWORD')
    except Exception, e:
        print(e)
        quit()
    #
    #  Connect to the bucket
    #
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
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
    local('mysqldump -h"%s" -P"%s" -uroot -p"%s" %s > %s' % (host, port, rootpw, project_name, sql_full_target))
    local('bzip2 %s' % sql_full_target)
    # append '.bz2'
    sql_local_full_target = sql_full_target
    sql_full_target = '%s.bz2' % os.path.join(db_backups_dir, sql_file)
    target_name = os.path.join(s3folder, os.path.basename(sql_full_target))

    key.key = target_name
    print 'uploading STARTING %s to %s: %s'%(sql_file, target_name, dt.now())
    try:
        key.set_contents_from_filename(sql_full_target)
        print 'upload %s FINISHED: %s'%(sql_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, FILEPATTERN, backup_aging_time=backup_aging_time)
        delete_local_db_backups(FILEPATTERN, backup_aging_time=backup_aging_time)

