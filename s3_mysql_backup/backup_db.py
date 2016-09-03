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

    pat = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.sql.bz2" % args.project_name

    sql_file = '%s-%s.sql' % (dt.now().strftime(TIMESTAMP_FORMAT), args.project_name)
    print 'Dumping database %s to %s.bz2' % (args.project_name, sql_file)

    sql_full_target = os.path.join(args.db_backups_dir, sql_file)
    subprocess.call_check('mysqldump -h"%s" -P"%s" -uroot -p"%s" %s > %s' % (
        args.mysql_host, args.mysql_port, args.db_pass, args.project_name, sql_full_target))
    subprocess.call_check('bzip2 %s' % sql_full_target)
    # append '.bz2'
    sql_local_full_target = sql_full_target
    sql_full_target = '%s.bz2' % os.path.join(args.db_backups_dir, sql_file)
    target_name = os.path.join(args.s3folder, os.path.basename(sql_full_target))

    key.key = target_name
    print 'Uploading STARTING %s to %s: %s' % (sql_file, target_name, dt.now())
    try:
        key.set_contents_from_filename(sql_full_target)
        print 'Upload %s FINISHED: %s' % (sql_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, pat, backup_aging_time=args.backup_aging_time)
        delete_local_db_backups(pat, backup_aging_time=args.backup_aging_time)


