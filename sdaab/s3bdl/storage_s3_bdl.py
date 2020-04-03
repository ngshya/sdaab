from abc import ABC, abstractmethod
import pickle
from pathlib import Path
from os.path import isdir, isfile
from os import stat
from re import sub
from io import BytesIO
from filechunkio import FileChunkIO
from numpy import unique
from math import ceil
from requests import post
from json import loads as jloads
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


class StorageS3BDL(Storage):


    def __init__(
        self, 
        url,
        secret_key,
        root_path="/"
    ):
        try:
            self.__storage_type = "S3BDL"
            root_path = str(root_path)
            assert len(root_path) > 0, "No root path provided."
            assert root_path[0] == "/", "Root path should start with /."
            root_path = str(Path(root_path).resolve())
            if root_path[-1] != "/":
                root_path = root_path + "/"
            self.__root_path_full = root_path
            self.__cd_full = root_path
            self.__cd = "/"
            self.__url = str(url)
            if self.__url[-1] != "/":
                self.__url  = self.__url[-1] + "/"
            self.__secret_key = str(secret_key)
            assert post(self.__url).text == '200'
            self.__initialized = True
            logger.debug("Storage S3BDL initialized.")
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
        return post(
            url=self.__url+"exists/",
            data={
                "secret_key": self.__secret_key,
                "key": key
            }
        ).text == 'True'

    
    def __exists_parent(self, key):
        key_parent = str(Path("/" + key).parent)
        if key_parent[-1] != "/":
            key_parent = key_parent + "/"
        if key_parent == "/":
            return True
        else:
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key
            }
            output = jloads(post(
                url=self.__url+"ls/", 
                data=post_data
            ).text)["ls"]
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
            output = self.__exists(key=path_full)
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key
            }
            output = post(
                url=self.__url+"mkdir/", 
                data=post_data
            ).text
            assert output == "OK!", "Post call failed."
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key,
            }
            post_files = {'file': open(path_source,'rb')}
            output = post(
                url=self.__url+"upload/", 
                data=post_data,
                files=post_files
            ).text
            assert output == "OK!", "Post call failed."
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key,
            }
            content = post(
                url=self.__url+"download/", 
                data=post_data,
            ).content
            with open(path_dest, 'wb') as s:
                s.write(content)
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key
            }
            output = post(
                url=self.__url+"rm/", 
                data=post_data
            ).text
            assert output == "OK!", "Post call failed."
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key
            }
            output = post(
                url=self.__url+"size/", 
                data=post_data
            ).text
            output = int(output)
            assert output >= 0, "Wrong output size."
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key,
            }
            post_files = {'file': content)}
            output = post(
                url=self.__url+"upload/", 
                data=post_data,
                files=post_files
            ).text
            assert output == "OK!", "Post call failed."
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
            post_data = {
                "key": path_full_4_s3, 
                "secret_key": self.__secret_key,
            }
            content = post(
                url=self.__url+"download/", 
                data=post_data,
            ).content
            if bool_bin:
                output = content.read()
            else:
                output = pickle.loads(content.read())
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
            post_data = {
                "key_old": path_source_full_4_s3, 
                "key_new": path_dest_full_4_s3,
                "secret_key": self.__secret_key
            }
            output = post(
                url=self.__url+"rename/", 
                data=post_data
            ).text
            assert output == "OK!", "Post call failed."
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
            post_data = {
                "key_old": path_source_full_4_s3, 
                "key_new": path_dest_full_4_s3,
                "secret_key": self.__secret_key
            }
            output = post(
                url=self.__url+"mv/", 
                data=post_data
            ).text
            assert output == "OK!", "Post call failed."
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
            post_data = {
                "key_old": path_source_full_4_s3, 
                "key_new": path_dest_full_4_s3,
                "secret_key": self.__secret_key
            }
            output = post(
                url=self.__url+"cp/", 
                data=post_data
            ).text
            assert output == "OK!", "Post call failed."
            logger.debug("cp " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to copy. " + str(e)) 
            raise ValueError("cp failed!")


    def append(self, path, content):
        # TODO: implement it!
        raise ValueError("append method not implemented yet!")
        '''
        try:
            logger.warning("Not the most efficient implementation, improve it!")
            assert self.__initialized, "Storage not initialized."
            assert type(content) == str, \
                "content should be a string"
            path = str(path)
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path, bool_file=True)
            path_full = self.__rm_lead_slash(path_full)

            logger.debug("append " + str(path) + ": " + str(content))
        except Exception as e:
            logger.error("Failed to append. " + str(e)) 
            raise ValueError("append failed!")
        '''