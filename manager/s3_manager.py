import boto3
import urllib
import os
from logger import logger

s3_client = boto3.client('s3',region_name='us-west-2')
s3 = boto3.resource('s3')

def download_from_s3(s3_path, download_path):
    url_parsed = urllib.parse.urlparse(s3_path)
    bucket_name = url_parsed[1]
    bucket = s3.Bucket(bucket_name)
    key = url_parsed.path[1:]
    try:
        logger.info("INFO - start downloading file %s to - %s"%(key,download_path))
        bucket.download_file(key, download_path)
        logger.info("INFO - end downloading file %s to - %s"%(key,download_path))
    except Exception as err:
        logger.error("ERROR - fail to download file %s to - %s"%(key,download_path))
        logger.error(err)
        raise err

def upload_to_s3(upload_path, bucket_name, key):
    try:
        bucket = s3.Bucket(bucket_name)
        logger.info("INFO - start uploading file %s to - %s"%(upload_path,key))
        bucket.upload_file(upload_path, key)
        logger.info("INFO - end uploading file %s to - %s"%(upload_path,key))
    except Exception as err:
        logger.error("ERROR - fail to upload file %s to - %s"%(upload_path,key))
        logger.error(err)
        raise err    

def upload_to_s3_repo_sofr(upload_path, filename, bucket_name, key_prefix, year, month):
    key = os.path.join(key_prefix, str(year), str(month), filename)
    upload_to_s3(upload_path, bucket_name, key)

