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


def s3_get_dir_backup(aws_access_key_id, aws_secret_access_key, bucket_name, s3_folder, zip_backups_dir, project):
    """
    get last uploaded directory backup
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param bucket_name:
    :param s3_folder:
    :param zip_backups_dir:
    :param project:
    :return:
    """

    matches = []
    pat = dir_zip_pat % project
    print('looking for pat "%s" in bucket %s' % (pat, bucket_name))
    bucket = s3_bucket(aws_access_key_id, aws_secret_access_key, bucket_name)
    bucketlist = bucket.list()
    for f in bucketlist:
        
        if re.search(pat + '$', f.name):
            print('%s matches' % f.name)
            bk_date = dt.strptime(f.name.replace(s3_folder + '/', '')[0:19], TIMESTAMP_FORMAT)
            matches.append({
                'key': f,
                'file': f.name,
                'date': bk_date
            })
    if matches:
        last_bk = sorted(matches, key=operator.itemgetter('date'))[0]
        dest = os.path.join(zip_backups_dir, last_bk['file'].replace(s3_folder + '/', ''))
        if not os.path.exists(dest):
            last_bk['key'].get_contents_to_filename(dest)
            print('Downloaded %s to %s' % (f.name, dest))
        else:
            print('Last backup %s already exists' % dest)
    else:
        print('no matches')


def backup(
        datadir,
        aws_access_key_id,
        aws_secret_access_key,
        bucket_name,
        zip_backups_dir, backup_aging_time, s3_folder, project):
    """
    zips dir into /backups, uploads to s3, deletes backups older than backup_aging_time.
    fab -f ./fabfile.py backup_dbs
    :param datadir:
    :param aws_access_key_id:
    :param aws_secret_access_key:
    :param bucket_name:
    :param zip_backups_dir:
    :param backup_aging_time:
    :param s3_folder:
    :param project:
    :return:
    """

    #  Connect to the bucket

    bucket = s3_bucket(aws_access_key_id, aws_secret_access_key, bucket_name)
    key = boto.s3.key.Key(bucket)

    bucketlist = bucket.list()

    pat = dir_zip_pat % project

    zip_file = '%s-%s.zip' % (dt.now().strftime(TIMESTAMP_FORMAT), project)
    print 'Zipping datadir %s to %s' % (zip_backups_dir, zip_file)
    zip_full_target = os.path.join(zip_backups_dir, zip_file)
    os.system('zip -r %s %s' % (zip_full_target, datadir))

    zip_local_full_target = zip_full_target
    # append '.zip'
    key.key = '%s/%s' % (s3_folder, zip_file)
    print 'STARTING upload of %s to %s: %s' % (zip_file, key.key, dt.now())
    try:
        key.set_contents_from_filename(zip_full_target)
        print 'Upload of %s FINISHED: %s' % (zip_local_full_target, dt.now())
    finally:
        delete_expired_backups_in_bucket(bucket, bucketlist, pat, backup_aging_time=backup_aging_time)
        delete_local_zip_backups(pat, zip_backups_dir, backup_aging_time)
