import os
from datetime import datetime as dt
import boto

from s3_mysql_backup import s3_bucket


def copy_file(args):
    """
    copies args.file to args.bucket args.s3_folder
    """

    #  Connect to the bucket

    bucket = s3_bucket(args)
    key = boto.s3.key.Key(bucket)

    if args.s3_folder:
        target_name = '%s/%s' % (args.s3_folder, os.path.basename(args.file))
    else:
        target_name = os.path.basename(args.file)

    key.key = target_name
    print('Uploading %s to %s' % (args.file, target_name))
    key.set_contents_from_filename(args.file)
    print('Upload %s FINISHED: %s' % (args.file, dt.now()))
