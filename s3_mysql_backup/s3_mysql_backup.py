import os
import re
from datetime import datetime as dt
from datetime import timedelta as td
import boto
from boto.s3.key import Key

TIMESTAMP_FORMAT = '%Y-%m-%d-%H-%M-%S'


def delete_expired_backups_in_bucket(bucket, bucketlist, FILEPATTERN, backup_aging_time=30):

    backup_expiration_date = dt.now() - td(days=backup_aging_time)
    for f in bucketlist:
        filename = os.path.basename(f.name)

        if re.match(FILEPATTERN, os.path.basename(filename)):
            bk_date = dt.strptime(os.path.basename(filename)[0:19], TIMESTAMP_FORMAT)
            if bk_date < backup_expiration_date:
                print 'Removing old S3 backup %s' % filename

                bucket.delete_key(f.name)


def delete_local_db_backups(FILEPATTERN, BACKUP_DIR='backups'):
    #
    # Delete old local backups
    #
    for dirName, subdirList, filelist in os.walk(BACKUP_DIR, topdown=False):
        for f in filelist:
            if re.search(FILEPATTERN, f):
                bk_date = dt.strptime(f[0:19], TIMESTAMP_FORMAT)
                if bk_date < backup_expiration_date:
                    print 'Removing old local backup %s' % f
                    os.remove(os.path.join(dirName, f))


def s3_key(BUCKET_NAME='php-apps-cluster'):

    #
    #
    #  Connect to the bucket
    #

    AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    if (conn.lookup(BUCKET_NAME)):
        print('----- bucket already exists! -----')
    else:
        print('----- creating bucket -----')
        conn.create_bucket(BUCKET_NAME)

    bucket = conn.get_bucket(BUCKET_NAME)

    return boto.s3.key.Key(bucket), bucket.list()


def mkdirs(dir, writable=False):
    if not os.path.exists(dir):
        if not writable:
            os.makedirs(dir, 0755)
        else:
            os.makedirs(dir, 0777)


def download_last_db_backup(db_backups_dir, project_name='biz'):
    """
    download last project db backup from S3
    """
    archive_file_extension = 'sql.tar.gz'
    if os.name == 'nt':
        raise NotImplementedError

    else:
        key, bucketlist = s3_key()

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

        last_backup = backup_list[0]
        keyString = str(last_backup['key'].key)

        # check if file exists locally, if not: download it
        dest = db_backups_dir+keyString
        print('Downloading %s to %s' % (keyString, dest))
        if not os.path.exists(dest):
            with open(db_backups_dir+keyString, 'wb') as f:
                last_backup['key'].get_contents_to_file(f)
        return last_backup['key']


def backup_project_db(project='biz', db_backups_dir='backups', BUCKET_NAME='php-apps-cluster', S3FOLDER="mysql-backups"):
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
    if (conn.lookup(BUCKET_NAME)):
        print '----- bucket already exists! -----'
    else:
        print '----- creating bucket -----'
        conn.create_bucket(BUCKET_NAME)

    bucket = conn.get_bucket(BUCKET_NAME)
    key = boto.s3.key.Key(bucket)

    bucketlist = bucket.list()

    FILEPATTERN = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.sql.bz2" % project
    sql_file = '%s-%s.sql' % (dt.now().strftime(TIMESTAMP_FORMAT), project)
    print 'Dumping database %s to %s.bz2' % (d, sql_file)

    sql_full_target = os.path.join(db_backups_dir, sql_file)
    local('mysqldump -h"%s" -P"%s" -uroot -p"%s" %s > %s' % (host, port, rootpw, project, sql_full_target))
    local('bzip2 %s' % sql_full_target)
    # append '.bz2'
    sql_local_full_target = sql_full_target
    sql_full_target = '%s.bz2' % os.path.join(db_backups_dir, sql_file)
    target_name = os.path.join(S3FOLDER, os.path.basename(sql_full_target))

    key.key = target_name
    print 'uploading STARTING %s to %s: %s'%(sql_file, target_name, dt.now())
    try:
        key.set_contents_from_filename(sql_full_target)
        print 'upload %s FINISHED: %s'%(sql_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, FILEPATTERN)
        delete_local_db_backups(FILEPATTERN)
