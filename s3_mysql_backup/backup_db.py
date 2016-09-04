import os
from datetime import datetime as dt
import boto
import subprocess
from s3_mysql_backup import s3_bucket
from s3_mysql_backup import TIMESTAMP_FORMAT
from s3_mysql_backup import delete_expired_backups_in_bucket
from s3_mysql_backup import delete_local_db_backups


def backup_db(args):
    """
    dumps databases into /backups, uploads to s3, deletes backups older than a month
    fab -f ./fabfile.py backup_dbs
    """

    #  Connect to the bucket

    bucket = s3_bucket(args)
    key = boto.s3.key.Key(bucket)

    bucketlist = bucket.list()

    pat = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.sql.bz2" % args.database

    sql_file = '%s-%s.sql' % (dt.now().strftime(TIMESTAMP_FORMAT), args.database)
    print 'Dumping database %s to %s.bz2' % (args.database, sql_file)

    sql_full_target = os.path.join(args.db_backups_dir, sql_file)
    f = open(sql_full_target, "wb")
    cmd = '/usr/bin/mysqldump -h%s -P%s -uroot -p%s %s ' % (
        args.mysql_host, args.mysql_port, args.db_pass, args.database)
    print(cmd)
    subprocess.call(cmd.split(), stdout=f)
    cmd = '/usr/bin/bzip2 %s' % sql_full_target
    print(cmd)
    subprocess.call(cmd.split())
    sql_local_full_target = sql_full_target
    # append '.bz2'
    key.key = '%s.bz2' % sql_file
    print 'STARTING upload of %s to %s: %s' % (sql_file, key.key, dt.now())
    try:
        key.set_contents_from_filename(os.path.join(args.db_backups_dir, sql_full_target))
        print 'Upload pf %s FINISHED: %s' % (sql_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, pat, backup_aging_time=args.backup_aging_time)
        delete_local_db_backups(pat, backup_aging_time=args.backup_aging_time)


