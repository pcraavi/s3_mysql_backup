import os
from datetime import datetime as dt
import boto
from s3_mysql_backup import s3_bucket
from s3_mysql_backup import TIMESTAMP_FORMAT
from s3_mysql_backup import delete_expired_backups_in_bucket
from s3_mysql_backup import delete_local_db_backups


def backup_dir(args):
    """
    zips dir into /backups, uploads to s3, deletes backups older than args.backup_aging_time.
    fab -f ./fabfile.py backup_dbs
    """

    #  Connect to the bucket

    bucket = s3_bucket(args)
    key = boto.s3.key.Key(bucket)

    bucketlist = bucket.list()

    pat = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.zip" % args.project

    zip_file = '%s-%s.zip' % (dt.now().strftime(TIMESTAMP_FORMAT), args.project)
    print 'Zipping datadir %s to %s' % (args.datadir, zip_file)
    zip_full_target = os.path.join(args.db_backups_dir, zip_file)
    os.system('zip -r %s %s' % (zip_full_target, args.datadir))

    zip_local_full_target = zip_full_target
    # append '.bz2'
    key.key = '%s/%s/' % (args.s3_folder, zip_file)
    print 'STARTING upload of %s to %s: %s' % (zip_file, key.key, dt.now())
    try:
        key.set_contents_from_filename(zip_full_target)
        print 'Upload of %s FINISHED: %s' % (zip_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, pat, backup_aging_time=args.backup_aging_time)
        delete_local_db_backups(pat, args)
