from sdaab.disk.storage_disk import StorageDisk
from sdaab.s3boto.storage_s3_boto import StorageS3boto
from sdaab.utils.get_config import dict_config


disk_root_path = dict_config["DISK"]["ROOT_PATH"]


s = StorageDisk(root_path=disk_root_path)

s.ls()
s.mkdir("folder1")
s.ls()
my_variable = "Hello World!"

s.upload_from_memory(my_variable, "my_variable")

