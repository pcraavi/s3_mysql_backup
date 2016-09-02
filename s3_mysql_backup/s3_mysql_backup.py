


def s3_key(aws_access_key_id, aws_secret_access_key, bucket_name='php-apps-cluster'):
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

