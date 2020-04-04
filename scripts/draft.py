import sys
sys.path.append(".")

from sdaab.disk.storage_disk import StorageDisk
from sdaab.s3boto.storage_s3_boto import StorageS3boto
from sdaab.s3bdl.storage_s3_bdl import StorageS3BDL
from sdaab.utils.get_config import dict_config

'''
s3boto = StorageS3boto(
    host=dict_config["S3"]["HOST"],
    port=dict_config["S3"]["PORT"],
    access_key=dict_config["S3"]["ACCESS_KEY"],
    secret_key=dict_config["S3"]["SECRET_KEY"], 
    bucket=dict_config["S3"]["BUCKET"],
    calling_format=dict_config["S3"]["CALLING_FORMAT"],
    secure=dict_config["S3"]["SECURE"],
    root_path="/testing/"
)
'''

s3bdl = StorageS3BDL(
    url=dict_config["S3BDL"]["URL"],
    secret_key=dict_config["S3BDL"]["SECRET_KEY"],
    root_path="/"
)