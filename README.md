# sdaab

[![Build Status](https://travis-ci.org/ngshya/sdaab.svg?branch=master)](https://travis-ci.org/ngshya/sdaab)

Simple Data Abstraction provides an abstraction layer to access different types of storage (e.g. local disk, S3).
Basic file management methods are implemented for each type of storage. 


## Installation

To install the package:
~~~~
pip install sdaab
~~~~


## Example of usage:

~~~~Python
from sdaab.disk.storage_disk import StorageDisk

# Initialize the storage object
s = StorageDisk(root_path="/tmp/")

# You can mkdir, rename, ls, cp, mv, etc.
s.mkdir("folder1")
s.rename("folder1", "folder2")

# You can also upload or download files.
v1 = "Hello world!"
s.upload_from_memory(v1, "v1")
v2 = s.download_to_memory(v2, "/folder3/v2")

# When you want to switch to another storage, 
# just re-initialize the storage object and 
# the methods will work exactly as before. 
s = StorageS3boto(
    host="s3.eu-west-3.amazonaws.com",
    port=1234,
    access_key="xxxyyyzzz",
    secret_key="aaabbbccc", 
    bucket="bucket-x",
    calling_format="boto.s3.connection.SubdomainCallingFormat",
    secure=True,
    root_path="/"
)
~~~~