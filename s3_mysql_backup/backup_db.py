import os
from datetime import datetime as dt
import boto
import subprocess
from s3_mysql_backup import s3_bucket
from s3_mysql_backup import TIMESTAMP_FORMAT
from s3_mysql_backup import delete_expired_backups_in_bucket
from s3_mysql_backup import delete_local_db_backups


def backup_db(
        aws_access_key_id,
        aws_secret_access_key,
        bucket_name,
        s3_folder,
        database,
        mysql_host,
        mysql_port, 
        db_user, 
        db_pass, 
        db_backups_dir, 
        backup_aging_time):
    """
    dumps databases into /backups, uploads to s3, deletes backups older than a month
    fab -f ./fabfile.py backup_dbs
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param bucket_name:
    :param database:
    :param mysql_host:
    :param mysql_port:
    :param db_pass:
    :param db_backups_dir:
    :param backup_aging_time:
    :return:
    """

    #  Connect to the bucket

    bucket = s3_bucket(aws_access_key_id, aws_secret_access_key, bucket_name)
    key = boto.s3.key.Key(bucket)

    bucketlist = bucket.list()

    pat = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.sql.bz2" % database

    sql_file = '%s-%s.sql' % (dt.now().strftime(TIMESTAMP_FORMAT), database)
    print 'Dumping database %s to %s.bz2' % (database, sql_file)

    sql_full_target = os.path.join(db_backups_dir, sql_file)
    f = open(sql_full_target, "wb")
    cmd = '/usr/bin/mysqldump -h%s -P%s -u%s -p%s %s ' % (mysql_host, mysql_port, db_user, db_pass, database)
    print(cmd)
    subprocess.call(cmd.split(), stdout=f)
    cmd = 'bzip2 %s' % sql_full_target
    print(cmd)
    subprocess.call(cmd.split())
    sql_local_full_target = sql_full_target
    # append '.bz2'
    key.key = os.path.join(s3_folder, '%s.bz2' % sql_file)
    print 'STARTING upload of %s to %s: %s' % (sql_file, key.key, dt.now())
    try:
        key.set_contents_from_filename('%s.bz2' % os.path.join(db_backups_dir, sql_full_target))
        print 'Upload of %s FINISHED: %s' % (sql_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, pat, backup_aging_time=backup_aging_time)
        delete_local_db_backups(pat, db_backups_dir, backup_aging_time)
