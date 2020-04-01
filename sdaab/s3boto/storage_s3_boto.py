from abc import ABC, abstractmethod
import pickle
from pathlib import Path
from os import makedirs, chmod, remove, walk, rename
from os.path import isdir, isfile, getsize, join, islink
from shutil import copyfile, move, copytree, rmtree
from re import sub
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from io import BytesIO
from .logger import logger
from ..storage.storage import Storage


def safe_folder_path_str(path):
    path = str(path)
    path = sub('[^a-zA-Z0-9-_./]+', '', path)
    if len(path) > 0:
        path = path + "/"
        path = sub('[/]+', '/', path)
    return path


def safe_file_path_str(path):
    path = Path(path)
    path_folder = path.parent
    file_name = path.name
    path_folder = safe_folder_path_str(path_folder)
    file_name = sub('[^a-zA-Z0-9-_.]+', '', file_name)
    path = path_folder + file_name
    return path


def get_folder_size(start_path = '.'):
    # This function is copied from:
    # https://stackoverflow.com/questions/1392413/
    total_size = 0
    for dirpath, dirnames, filenames in walk(start_path):
        for f in filenames:
            fp = join(dirpath, f)
            # skip if it is symbolic link
            if not islink(fp):
                total_size += getsize(fp)
    return total_size


class StorageS3boto(Storage):


    def __init__(
        self, 
        host,
        port,
        access_key,
        secret_key, 
        bucket,
        calling_format,
        secure,
        root_path="/",
    ):
        try:
            self.__storage_type = "S3boto"
            root_path = str(root_path)
            assert root_path[0] == "/", "Root path should start with /."
            root_path = Path(root_path).resolve()
            assert isdir(root_path), "Root folder not found."
            self.__root_path_full = root_path
            self.__cd_full = root_path
            self.__cd = Path("/")

            self.__host = str(host)
            self.__port = int(port)
            self.__access_key = str(access_key)
            self.__secret_key = str(secret_key)
            self.__bucket = str(bucket)
            self.__calling_format = str(calling_format)
            if type(secure) == bool:
                self.__secure = secure 
            else:
                self.__secure = secure == "True" 

            self.__connection = S3Connection(
                host=self.__host,
                port=self.__port,
                aws_access_key_id=self.__access_key,
                aws_secret_access_key=self.__secret_key,
                calling_format=self.__calling_format,
                is_secure=self.__secure
            )

            assert self.__connection.lookup(self.__bucket) is not None, \
                "The bucket specified doesn't exists!"
            
            self.__connection_bucket = self.__connection\
                .get_bucket(self.__bucket)

            self.__initialized = True
            logger.debug("Storage DISK initialized.")

        except Exception as e:
            self.__initialized = False
            logger.error("Initialization failed. " + str(e))


    def initialized(self):
        return self.__initialized


    def __path_expand(self, path):
        path = str(path)
        if len(path) == 0:
            path_full = self.__cd_full
        elif path[0] == "/":
            path_full = Path(str(self.__root_path_full) + path).resolve()
        else:
            path_full = (self.__cd_full / path).resolve()
        return path_full
    

    def __check_path_full(self, path_full):
        assert str(Path(path_full).resolve())\
            .startswith(str(self.__root_path_full)), \
            "Impossible to go beyond the root path."


    def __full_path_4_s3(self, path_full):
        path_full = str(path_full)
        if path_full[0] == "/":
            path_full = path_full[1:]
        return path_full


    def get_type(self):
        try:
            assert self.__initialized, "Storage not initialized."
            logger.debug("Storage type: " + self.__storage_type)
            return self.__storage_type
        except Exception as e:
            logger.error("Failed to get the storage type. " + str(e))


    def cd(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            assert self.exists(self.__full_path_4_s3(path_full)), \
                "Current directory not found."
            self.__cd_full = path_full
            if path[0] == "/":
                self.__cd = Path(path).resolve()
            else:
                self.__cd = Path(str(self.__cd) + "/" + path).resolve()
            logger.debug("cd " + str(path) + ": True")
        except Exception as e:
            logger.error("cd failed. " + str(e))
    

    def pwd(self):
        try:
            assert self.__initialized, "Storage not initialized."
            #logger.debug("pwd full path: " + str(self.__cd_full))
            logger.debug("pwd: " + str(self.__cd))
            return str(self.__cd)
        except Exception as e:
            logger.error("pwd failed. " + str(e))


    def ls(self, path=""):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full) + "/"
            assert self.exists(path_full_4_s3),\
                 "Folder not found."
            iterable = self.__connection_bucket.list(prefix=path_full_4_s3)
            output = [x.name for x in iterable if x.name != path_full_4_s3]
            output = [sub("^%s" % path_full_4_s3, "", x) for x in output]
            logger.debug("ls " + str(path) + ": " + " ".join(output))
            return output
        except Exception as e:
            logger.error("Failed to list objects inside the folder. " + str(e))


    def exists(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            path_full = self.__full_path_4_s3(path_full)
            k = Key(self.__connection_bucket)
            k.key = path_full
            if k.exists():
                output = True
            elif path_full[-1] != "/": 
                k.key = path_full + "/"
                output = k.exists()
            else:
                output = False
            logger.debug("exists " + str(path) + ": " + str(output))
            return output
        except Exception as e:
            logger.error("Failed to check the existence. " + str(e))


    def mkdir(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_folder_path_str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full) + "/"
            assert not self.exists(path_full_4_s3), "Directory already exists."
            k = self.__connection_bucket
            k.new_key(path_full_4_s3)
            k.set_contents_from_string('')
            assert self.exists(path_full_4_s3), "Directory check failed."
            logger.debug("mkdir " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to create the directory. " + str(e))  


    def upload(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_dest = str(path_dest)
            path_dest = safe_file_path_str(path_dest)
            path_full = self.__path_expand(path_dest)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full)
            assert isfile(path_source), "Source file not found."
            assert not self.exists(path_full_4_s3), \
                "Destination file already exists."
            assert not self.exists(path_full_4_s3 + "/"), \
                "Destination folder already exists."
            k = self.__connection_bucket.new_key(path_full_4_s3)
            with open(path_source, "rb") as fp:
                k.set_contents_from_file(fp)
            assert self.exists(path_full_4_s3), \
                "Destination file check failed."
            logger.debug("upload " + str(path_dest) + ": True")
        except Exception as e:
            logger.error("Failed to upload. " + str(e))  


    def download(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_dest = str(path_dest)
            path_source = safe_file_path_str(path_source)
            path_full = self.__path_expand(path_source)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full)
            assert self.exists(path_full_4_s3), "Source file not found."
            assert not isfile(path_dest), "Destination file already exists."
            assert not isdir(path_dest), "Destination folder already exists."
            with open(path_dest, "wb") as fp:
                self.__connection_bucket\
                    .get_key(path_full_4_s3)\
                    .get_contents_to_file(fp)
            assert isfile(path_dest), "Destination file check failed."
            logger.debug("download " + str(path_source) + ": True")
        except Exception as e:
            logger.error("Failed to download. " + str(e)) 


    def rm(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_folder_path_str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full)
            if self.exists(path_full_4_s3):
                self.__connection_bucket.delete_key(path_full_4_s3)
            elif self.exists(path_full_4_s3 + "/"):
                iterable = self.__connection_bucket\ 
                    .list(prefix=path_full_4_s3 + "/")
                output = [x.name for x in iterable]
                for k in output:
                    self.__connection_bucket.delete_key(k)
            else:
                error("File/folder not found.")
            assert ((not self.exists(path_full_4_s3)) \
                and (not self.exists(path_full_4_s3 + "/"))), \
                "File/folder still exists."
            logger.debug("rm " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to remove the file/folder. " + str(e)) 


    def size(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full)
            if self.exists(path_full_4_s3):
                output = self.__connection_bucket.get_key(path_full_4_s3).size
            elif self.exists(path_full_4_s3 + "/"):
                iterable = self.__connection_bucket\ 
                    .list(prefix=path_full_4_s3 + "/")
            output = sum([x.size for x in iterable])
            else:
                error( "File/folder not found.")
            logger.debug("size " + str(path) + ": " + str(output))
            return output
        except Exception as e:
            logger.error("Failed to get the size. " + str(e))


    def upload_from_memory(self, variable, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full)
            assert not self.exists(path_full_4_s3), "File already exists."
            assert not self.exists(path_full_4_s3 + "/"), "Folder already exists."
            content = pickle.dumps(variable)
            k = self.__connection_bucket.new_key(path_full_4_s3)
            k.set_contents_from_string(content)
            assert self.exists(path_full_4_s3), "File check failed."
            logger.debug("upload_from_memory " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to upload. " + str(e))  


    def download_to_memory(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            path_full_4_s3 = self.__full_path_4_s3(path_full)
            assert self.exists(path_full_4_s3), "File not found."
            with BytesIO() as b:
                k = self.__connection_bucket.get_key(path_full_4_s3)
                k.get_file(b)
                b.seek(0)
                output = pickle.loads(b.read())
            logger.debug("download_to_memory " + str(path) + ": True")
            return output
        except Exception as e:
            logger.error("Failed to download. " + str(e))  


    def rename(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_source_full = self.__path_expand(path_source)
            self.__check_path_full(path_source_full)
            path_source_full_4_s3 = self.__full_path_4_s3(path_source_full)

            path_dest = str(path_dest)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest)
            self.__check_path_full(path_dest_full)
            path_dest_full_4_s3 = self.__full_path_4_s3(path_dest_full)

            assert path_source_full.parent == path_dest_full.parent, \
                "Different parent directories."

            iterable = self.__connection_bucket\ 
                    .list(prefix=path_source_full_4_s3)
            array_sources = [x.name for x in iterable]

            assert len(output) > 0, \
                "Source file/folder not found."

            array_dests = [sub(path_source_full_4_s3, path_dest_full_4_s3, x) \
                for x in array_sources]

            for item_dest in array_dests:
                assert not self.exists("/" + item_dest), \
                    "Destination already exists."

            for j, item_source in enumerate(array_sources):
                self.__connection_bucket.copy_key(array_dests[j], self.__bucket, item_source)
                self.__connection_bucket.delete_key(item_source)
                assert self.exists("/" + array_dests[j]), \
                    "Destination check failed."
                assert not self.exists("/" + item_source), \
                    "Source check failed."
            logger.debug("rename " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to rename. " + str(e)) 


# TODO 


    def mv(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_dest = str(path_dest)
            path_source_full = self.__path_expand(path_source)
            self.__check_path_full(path_source_full)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest)
            self.__check_path_full(path_dest_full)
            assert (isfile(path_source_full) or isdir(path_source_full)), \
                "Source file/folder not found."
            assert not (isfile(path_dest_full) or isdir(path_dest_full)), \
                "Destination already exists."
            move(path_source_full, path_dest_full)
            assert (isfile(path_dest_full) or isdir(path_dest_full)), \
                "Destination check failed."
            assert not (isfile(path_source_full) or isdir(path_source_full)), \
                "Source check failed."
            logger.debug("mv " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to move. " + str(e)) 


    def cp(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_dest = str(path_dest)
            path_source_full = self.__path_expand(path_source)
            self.__check_path_full(path_source_full)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest)
            self.__check_path_full(path_dest_full)
            assert (isfile(path_source_full) or isdir(path_source_full)), \
                "Source file/folder not found."
            assert not (isfile(path_dest_full) or isdir(path_dest_full)), \
                "Destination already exists."
            if isfile(path_source_full):
                copyfile(path_source_full, path_dest_full)
            else:
                copytree(path_source_full, path_dest_full)
            assert (isfile(path_dest_full) or isdir(path_dest_full)), \
                "Destination check failed."
            assert (isfile(path_source_full) or isdir(path_source_full)), \
                "Source check failed."
            logger.debug("cp " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to copy. " + str(e)) 


    def append(self, path, content):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path)
            self.__check_path_full(path_full)
            assert isfile(path_full), "File not found."
            with open(path_full, "a") as f:
                f.write(content)
            logger.debug("append " + str(path) + ": " + str(content))
        except Exception as e:
            logger.error("Failed to append. " + str(e)) 