import os
import re
import operator
from datetime import datetime as dt
import boto
from s3_mysql_backup import s3_bucket
from s3_mysql_backup import TIMESTAMP_FORMAT
from s3_mysql_backup import delete_expired_backups_in_bucket
from s3_mysql_backup import delete_local_zip_backups

dir_zip_pat = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-[0-9][0-9]-%s.zip"


def s3_get_dir_backup(args):
    """
    get last uploaded directory backup
    :param args:
    :return:
    """

    matches = []
    pat = dir_zip_pat % args.project
    print('looking for pat "%s" in bucket %s' % (pat, args.bucket_name))
    bucket = s3_bucket(args)
    bucketlist = bucket.list()
    for f in bucketlist:
        print(f.name)
        if re.match(pat, f.name):
            print('%s matches' % f.name)
            bk_date = dt.strptime(f.name[0:19], TIMESTAMP_FORMAT)
            matches.append({
                'key': f,
                'file': f.name,
                'date': bk_date
            })
    if matches:
        last_bk = sorted(matches, key=operator.itemgetter('date'))[0]
        dest = os.path.join(args.zip_backups_dir, last_bk['file'])
        if not os.path.exists(dest):
            last_bk['key'].get_contents_to_filename(dest)
            print('Downloaded %s to %s' % (f.name, dest))
        else:
            print('Last backup %s already exists' % dest)
    else:
        print('no matches')


def backup(args):
    """
    zips dir into /backups, uploads to s3, deletes backups older than args.backup_aging_time.
    fab -f ./fabfile.py backup_dbs
    """

    #  Connect to the bucket

    bucket = s3_bucket(args)
    key = boto.s3.key.Key(bucket)

    bucketlist = bucket.list()

    pat = dir_zip_pat % args.project

    zip_file = '%s-%s.zip' % (dt.now().strftime(TIMESTAMP_FORMAT), args.project)
    print 'Zipping datadir %s to %s' % (args.zip_backups_dir, zip_file)
    zip_full_target = os.path.join(args.zip_backups_dir, zip_file)
    os.system('zip -r %s %s' % (zip_full_target, args.datadir))

    zip_local_full_target = zip_full_target
    # append '.bz2'
    key.key = '%s/%s' % (args.s3_folder, zip_file)
    print 'STARTING upload of %s to %s: %s' % (zip_file, key.key, dt.now())
    try:
        key.set_contents_from_filename(zip_full_target)
        print 'Upload of %s FINISHED: %s' % (zip_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, pat, backup_aging_time=args.backup_aging_time)
        delete_local_zip_backups(pat, args)
