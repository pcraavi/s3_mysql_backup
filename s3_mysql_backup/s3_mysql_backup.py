import boto


def s3_conn(args):
    return boto.connect_s3(args.aws_access_key_id, args.aws_secret_access_key, is_secure=False)


def s3_bucket(args):
    """
    connect to S3, return connection key and bucket list
    """

    return s3_conn(args).get_bucket(args.bucket_name)

