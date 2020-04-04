from abc import ABC, abstractmethod
import pickle
from pathlib import Path
from os.path import isdir, isfile
from os import stat
from re import sub
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from io import BytesIO
from filechunkio import FileChunkIO
from numpy import unique
from math import ceil
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
            assert len(root_path) > 0, "No root path provided."
            assert root_path[0] == "/", "Root path should start with /."
            root_path = str(Path(root_path).resolve())
            if root_path[-1] != "/":
                root_path = root_path + "/"
            self.__root_path_full = root_path
            self.__cd_full = root_path
            self.__cd = "/"
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

            if len(self.__root_path_full) > 0: 
                k = Key(self.__connection_bucket)
                k.key = self.__rm_lead_slash(self.__root_path_full)
                assert k.exists(), "Root folder not found!"

            self.__initialized = True
            logger.debug("Storage DISK initialized.")

        except Exception as e:
            self.__initialized = False
            logger.error("Initialization failed. " + str(e))
            raise ValueError("init failed!")


    def initialized(self):
        return self.__initialized


    def __path_expand(self, path, bool_file=True):
        path = str(path)
        if len(path) == 0:
            assert not bool_file, "Not a file."
            path_full = self.__cd_full
        elif path[0] == "/":
            path_full = str(Path(self.__root_path_full + path).resolve())
            if not bool_file:
                path_full = path_full + "/"
        else:
            path_full = str((Path(self.__cd_full) / path).resolve())
            if not bool_file:
                path_full = path_full + "/"
        assert path_full.startswith(str(self.__root_path_full)), \
            "Impossible to go beyond the root path."
        return path_full
    

    def __rm_lead_slash(self, path):
        if path[0] == "/":
            return path[1:]
        else:
            return path
    

    def __exists(self, key):
        k = Key(self.__connection_bucket)
        k.key = key
        return k.exists()

    
    def __exists_parent(self, key):
        if (key == "/") or (key == ""):
            return True
        key_parent = str(Path("/" + key).parent)
        if key_parent[-1] != "/":
            key_parent = key_parent + "/"
        key_parent = key_parent[1:]
        k = Key(self.__connection_bucket)
        k.key = key_parent
        return self.__exists(key_parent)


    def get_type(self):
        try:
            assert self.__initialized, "Storage not initialized."
            logger.debug("Storage type: " + self.__storage_type)
            return self.__storage_type
        except Exception as e:
            logger.error("Failed to get the storage type. " + str(e))
            raise ValueError("get_type failed!")


    def cd(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path_full = self.__path_expand(path, bool_file=False)
            assert self.__exists(path_full), "Current directory not found."
            self.__cd_full = path_full
            self.__cd = "/" + sub(self.__root_path_full, "", self.__cd_full)
            logger.debug("cd " + str(path) + ": True")
        except Exception as e:
            logger.error("cd failed. " + str(e))
            raise ValueError("cd failed!")
    

    def pwd(self):
        try:
            assert self.__initialized, "Storage not initialized."
            output = self.__cd
            if output != "/" and output[-1] == "/":
                output = output[:-1]
            logger.debug("pwd: " + output)
            return output
        except Exception as e:
            logger.error("pwd failed. " + str(e))
            raise ValueError("pwd failed!")


    def ls(self, path=""):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path_full = self.__path_expand(path, bool_file=False)
            path_full_4_s3 = self.__rm_lead_slash(path_full) 
            if len(path_full_4_s3) > 0:
                assert self.__exists(path_full_4_s3), "Folder not found."
                iterable = self.__connection_bucket.list(prefix=path_full_4_s3)
                output = [x.name for x in iterable]
                output = [x.name for x in iterable if x.name != path_full_4_s3]
                output = [sub("^"+path_full_4_s3+"|/.*$", "", x) for x in output]
            else:
                iterable = self.__connection_bucket.list()
                output = [x.name for x in iterable]
                output = [sub("/.*$", "", x) for x in output]
            logger.debug("ls " + str(path) + ": " + " ".join(output))
            return unique(output)
        except Exception as e:
            logger.error("Failed to list objects inside the folder. " + str(e))
            raise ValueError("ls failed!")


    def exists(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path_full = self.__path_expand(path, bool_file=True)
            path_full = self.__rm_lead_slash(path_full)
            k = Key(self.__connection_bucket)
            k.key = path_full
            if k.exists():
                output = True
            else:
                k.key = path_full + "/"
                output = k.exists()
            logger.debug("exists " + str(path) + ": " + str(output))
            return output
        except Exception as e:
            logger.error("Failed to check the existence. " + str(e))
            raise ValueError("exists failed!")


    def mkdir(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_folder_path_str(path)
            path_full = self.__path_expand(path, bool_file=False)
            path_full_4_s3 = self.__rm_lead_slash(path_full)
            assert not self.__exists(path_full_4_s3), \
                "Directory already exists."
            assert self.__exists_parent(path_full_4_s3), \
                "Parent folder not found"
            k = self.__connection_bucket.new_key(path_full_4_s3)
            k.set_contents_from_string('')
            assert self.__exists(path_full_4_s3), "Directory check failed."
            logger.debug("mkdir " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to create the directory. " + str(e))  
            raise ValueError("mkdir failed!")


    def upload(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_dest = str(path_dest)
            path_dest = safe_file_path_str(path_dest)
            path_full = self.__path_expand(path_dest, bool_file=True)
            path_full_4_s3 = self.__rm_lead_slash(path_full)
            assert self.__exists_parent(path_full_4_s3), \
                "Parent folder not found."
            assert isfile(path_source), "Source file not found."
            assert not self.__exists(path_full_4_s3), \
                "Destination file already exists."
            assert not self.__exists(path_full_4_s3 + "/"), \
                "Destination folder already exists."
            source_size = stat(path_source).st_size
            if source_size == 0:
                k = self.__connection_bucket.new_key(path_full_4_s3)
                with open(path_source, "rb") as fp:
                    k.set_contents_from_file(fp)
            else:
                chunk_size = 5242880
                mp = self.__connection_bucket\
                    .initiate_multipart_upload(path_full_4_s3)
                chunk_count = int(ceil(source_size / float(chunk_size)))
                for i in range(chunk_count):
                    offset = chunk_size * i
                    int_bytes = min(chunk_size, source_size - offset)
                    with FileChunkIO(
                        path_source, 
                        'r', 
                        offset=offset, 
                        bytes=int_bytes
                    ) as fp:
                        mp.upload_part_from_file(fp, part_num=i + 1)
                mp.complete_upload()
            assert self.__exists(path_full_4_s3), \
                "Destination file check failed."
            logger.debug("upload " + str(path_dest) + ": True")
        except Exception as e:
            logger.error("Failed to upload. " + str(e))  
            raise ValueError("upload failed!")


    def download(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_dest = str(path_dest)
            path_source = safe_file_path_str(path_source)
            path_full = self.__path_expand(path_source, bool_file=True)
            path_full_4_s3 = self.__rm_lead_slash(path_full)
            assert self.__exists(path_full_4_s3), "Source file not found."
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
            raise ValueError("download failed!")


    def rm(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_folder_path_str(path)
            path_full = self.__path_expand(path, bool_file=True)
            path_full_4_s3 = self.__rm_lead_slash(path_full)
            assert len(path_full_4_s3) > 0, "Nothing to remove."
            if self.__exists(path_full_4_s3):
                self.__connection_bucket.delete_key(path_full_4_s3)
                assert not self.__exists(path_full_4_s3), \
                    "File/folder still exists."
            iterable = self.__connection_bucket\
                .list(prefix=path_full_4_s3+"/")
            output = [x.name for x in iterable]
            for k in output:
                self.__connection_bucket.delete_key(k)
                assert not self.__exists(k), "File/folder still exists."
            logger.debug("rm " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to remove the file/folder. " + str(e))
            raise ValueError("rm failed!") 


    def size(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path, bool_file=True)
            path_full_4_s3 = self.__rm_lead_slash(path_full)
            if self.__exists(path_full_4_s3):
                output = self.__connection_bucket.get_key(path_full_4_s3).size
            elif self.__exists(path_full_4_s3 + "/"):
                iterable = self.__connection_bucket\
                    .list(prefix=path_full_4_s3 + "/")
                output = sum([x.size for x in iterable])
            else:
                raise ValueError( "File/folder not found.")
            logger.debug("size " + str(path) + ": " + str(output))
            return output
        except Exception as e:
            logger.error("Failed to get the size. " + str(e)) 
            raise ValueError("size failed!")


    def upload_from_memory(self, variable, path, bool_bin=False):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path, bool_file=True)
            path_full_4_s3 = self.__rm_lead_slash(path_full)
            assert not self.__exists(path_full_4_s3), "File already exists."
            assert not self.__exists(path_full_4_s3 + "/"), "Folder already exists."
            if bool_bin:
                content=variable
            else:
                content = pickle.dumps(variable)
            if len(content) == 0:
                k = self.__connection_bucket.new_key(path_full_4_s3)
                k.set_contents_from_string("")
            else:
                mp = self.__connection_bucket.initiate_multipart_upload(path_full_4_s3)
                chunk_size = 5242880
                source_size = len(content)
                chunk_count = int(ceil(source_size / float(chunk_size)))
                for i in range(chunk_count):
                    offset = chunk_size * i
                    int_bytes = min(chunk_size, source_size - offset)
                    fp = BytesIO(content[offset:offset+int_bytes])
                    mp.upload_part_from_file(fp, part_num=i+1)
                mp.complete_upload()
            assert self.__exists(path_full_4_s3), "File check failed."
            logger.debug("upload_from_memory " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to upload. " + str(e))  
            raise ValueError("upload_from_memory failed!")


    def download_to_memory(self, path, bool_bin=False):
        try:
            assert self.__initialized, "Storage not initialized."
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path, bool_file=True)
            path_full_4_s3 = self.__rm_lead_slash(path_full)
            assert self.__exists(path_full_4_s3), "File not found."
            with BytesIO() as b:
                k = self.__connection_bucket.get_key(path_full_4_s3)
                k.get_file(b)
                b.seek(0)
                if bool_bin:
                    output = b.read()
                else:
                    output = pickle.loads(b.read())
            logger.debug("download_to_memory " + str(path) + ": True")
            return output
        except Exception as e:
            logger.error("Failed to download. " + str(e))  
            raise ValueError("download_to_memory failed!")


    def rename(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_source_full = self.__path_expand(path_source, bool_file=True)
            path_source_full_4_s3 = self.__rm_lead_slash(path_source_full)
            path_dest = str(path_dest)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest, bool_file=True)
            path_dest_full_4_s3 = self.__rm_lead_slash(path_dest_full)
            assert Path(path_dest_full_4_s3).parent \
                == Path(path_source_full_4_s3).parent, \
                "Different parent directories."
            if self.__exists(path_source_full_4_s3) \
                and not self.__exists(path_dest_full_4_s3):
                self.__connection_bucket.copy_key(
                    path_dest_full_4_s3, 
                    self.__bucket, 
                    path_source_full_4_s3
                )
                self.__connection_bucket.delete_key(path_source_full_4_s3)
            else:
                assert self.__exists(path_source_full_4_s3+"/") \
                    and not self.__exists(path_dest_full_4_s3+"/"), \
                    "Source not found or destination already exists."
                iterable = self.__connection_bucket\
                    .list(prefix=path_source_full_4_s3+"/")
                array_sources = [x.name for x in iterable]
                array_dests = [sub("^"+path_source_full_4_s3, \
                    path_dest_full_4_s3, x) \
                    for x in array_sources]
                for item_dest in array_dests:
                    assert not self.__exists(item_dest), \
                        "Destination already exists."
                for j, item_source in enumerate(array_sources):
                    self.__connection_bucket.copy_key(
                        array_dests[j], 
                        self.__bucket, 
                        item_source
                    )
                    self.__connection_bucket.delete_key(item_source)
                    assert self.__exists(array_dests[j]), \
                        "Destination check failed."
                    assert not self.__exists(item_source), \
                        "Source check failed."
            logger.debug("rename " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to rename. " + str(e)) 
            raise ValueError("rename failed!")


    def mv(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_source_full = self.__path_expand(path_source, bool_file=True)
            path_source_full_4_s3 = self.__rm_lead_slash(path_source_full)
            path_dest = str(path_dest)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest, bool_file=True)
            path_dest_full_4_s3 = self.__rm_lead_slash(path_dest_full)
            if self.__exists(path_source_full_4_s3) \
                and not self.__exists(path_dest_full_4_s3):
                self.__connection_bucket.copy_key(
                    path_dest_full_4_s3, 
                    self.__bucket, 
                    path_source_full_4_s3
                )
                self.__connection_bucket.delete_key(path_source_full_4_s3)
            else:
                assert self.__exists(path_source_full_4_s3+"/") \
                    and not self.__exists(path_dest_full_4_s3+"/"), \
                    "Source not found or destination already exists."
                iterable = self.__connection_bucket\
                    .list(prefix=path_source_full_4_s3+"/")
                array_sources = [x.name for x in iterable]
                array_dests = [sub("^"+path_source_full_4_s3, \
                    path_dest_full_4_s3, x) \
                    for x in array_sources]
                for item_dest in array_dests:
                    assert not self.__exists(item_dest), \
                        "Destination already exists."
                for j, item_source in enumerate(array_sources):
                    self.__connection_bucket.copy_key(
                        array_dests[j], 
                        self.__bucket, 
                        item_source
                    )
                    self.__connection_bucket.delete_key(item_source)
                    assert self.__exists(array_dests[j]), \
                        "Destination check failed."
                    assert not self.__exists(item_source), \
                        "Source check failed."
            logger.debug("mv " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to move. " + str(e)) 
            raise ValueError("mv failed!")


    def cp(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = str(path_source)
            path_source_full = self.__path_expand(path_source, bool_file=True)
            path_source_full_4_s3 = self.__rm_lead_slash(path_source_full)
            path_dest = str(path_dest)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest, bool_file=True)
            path_dest_full_4_s3 = self.__rm_lead_slash(path_dest_full)
            if self.__exists(path_source_full_4_s3) \
                and not self.__exists(path_dest_full_4_s3):
                self.__connection_bucket.copy_key(
                    path_dest_full_4_s3, 
                    self.__bucket, 
                    path_source_full_4_s3
                )
            else:
                assert self.__exists(path_source_full_4_s3+"/") \
                    and not self.__exists(path_dest_full_4_s3+"/"), \
                    "Source not found or destination already exists."
                iterable = self.__connection_bucket\
                    .list(prefix=path_source_full_4_s3+"/")
                array_sources = [x.name for x in iterable]
                array_dests = [sub("^"+path_source_full_4_s3, \
                    path_dest_full_4_s3, x) \
                    for x in array_sources]
                for item_dest in array_dests:
                    assert not self.__exists(item_dest), \
                        "Destination already exists."
                for j, item_source in enumerate(array_sources):
                    self.__connection_bucket.copy_key(
                        array_dests[j], 
                        self.__bucket, 
                        item_source
                    )
                    assert self.__exists(array_dests[j]), \
                        "Destination check failed."
                    assert self.__exists(item_source), \
                        "Source check failed."
            logger.debug("cp " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to copy. " + str(e)) 
            raise ValueError("cp failed!")


    def append(self, path, content):
        try:
            logger.warning("Not the most efficient implementation, improve it!")
            assert self.__initialized, "Storage not initialized."
            assert type(content) == str, \
                "content should be a string"
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path, bool_file=True)
            path_full = self.__rm_lead_slash(path_full)
            k = Key(self.__connection_bucket)
            k.key = path_full
            assert k.exists(), "File not found."
            content_old = self.download_to_memory(path=path)
            assert type(content_old) == str, \
                "It is only possible to append to strings!"
            content_new = content_old + content
            self.rm(path)
            self.upload_from_memory(variable=content_new, path=path)
            logger.debug("append " + str(path) + ": " + str(content))
        except Exception as e:
            logger.error("Failed to append. " + str(e)) 
            raise ValueError("append failed!")