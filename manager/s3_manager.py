import boto3

# client = boto3.client('lambda')
s3_client = boto3.client('s3')

def upload_to_s3(filepath):
    pass
    # bucket = ''
    # upload_path = '/tmp/resized-{}'.format(tmpkey)
    # s3_client.upload_file(upload_path, '{}-resized'.format(bucket), key)

