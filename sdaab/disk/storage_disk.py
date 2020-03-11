from abc import ABC, abstractmethod
import pickle
from pathlib import Path
from os import makedirs, chmod, remove, walk, rename
from os.path import isdir, isfile, getsize, join, islink
from shutil import copyfile, move, copytree, rmtree
from re import sub
from .logger import logger
from ..storage.storage import Storage


def safe_folder_path_str(path):
    path = str(path)
    path = sub('[^a-zA-Z0-9-_/]+', '', path)
    path = path + "/"
    path = sub('[/]+', '/', path)
    return path


def safe_file_path_str(path):
    path = Path(path)
    path_folder = path.parent
    file_name = path.name
    path_folder = safe_folder_path_str(path_folder)
    file_name = sub('[^a-zA-Z0-9-_.]+', '', file_name)
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


class StorageDisk(Storage):


    def __init__(self, root_path="/"):
        try:
            self.__storage_type = "DISK"
            assert root_path[0] == "/", "Root path should start with /."
            root_path = Path(root_path).resolve()
            assert isdir(root_path), "Root folder not found."
            self.__root_path_full = root_path
            self.__cd_full = root_path
            self.__cd = Path("/")
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
            path_full = self.__path_expand(path)
            assert isdir(path_full), "Current directory not found."
            self.__cd_full = path_full
            self.__cd = Path(str(self.__cd) + "/" + path).resolve()
            logger.debug("cd " + str(path) + ": True")
        except Exception as e:
            logger.error("cd failed. " + str(e))
    

    def pwd(self):
        try:
            assert self.__initialized, "Storage not initialized."
            logger.debug("pwd: " + str(self.__cd))
            return str(self.__cd)
        except Exception as e:
            logger.error("pwd failed. " + str(e))


    def ls(self, path=""):
        try:
            assert self.__initialized, "Storage not initialized."
            path_full = self.__path_expand(path)
            assert isdir(path_full), "Folder not found."
            output = [x.name for x in path_full.iterdir()]
            logger.debug("ls " + str(path) + ": " + " ".join(output))
            return output
        except Exception as e:
            logger.error("Failed to list objects inside the folder. " + str(e))


    def exists(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path_full = self.__path_expand(path)
            output = isdir(path_full) or isfile(path_full)
            logger.debug("exists " + str(path) + ": " + str(output))
            return output
        except Exception as e:
            logger.error("Failed to check the existence. " + str(e))


    def mkdir(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = safe_folder_path_str(path)
            path_full = self.__path_expand(path)
            assert not isdir(path_full), "Directory already exists."
            makedirs(path_full)
            assert isdir(path_full), "Directory check failed."
            chmod(path_full, 0o777)
            logger.debug("mkdir " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to create the directory. " + str(e))  


    def upload(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_dest = safe_file_path_str(path_dest)
            path_full = self.__path_expand(path_dest)
            assert isfile(path_source), "Source file not found."
            assert not isfile(path_full), "Destination file already exists."
            assert not isdir(path_full), "Destination folder already exists."
            copyfile(path_source, path_full)
            assert isfile(path_full), "Destination file check failed."
            chmod(path_full, 0o777)
            logger.debug("upload " + str(path_dest) + ": True")
        except Exception as e:
            logger.error("Failed to upload. " + str(e))  


    def download(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source = safe_file_path_str(path_source)
            path_full = self.__path_expand(path_source)
            assert isfile(path_source), "Source file not found."
            assert not isfile(path_dest), "Destination file already exists."
            assert not isdir(path_dest), "Destination folder already exists."
            copyfile(path_full, path_dest)
            assert isfile(path_dest), "Destination file check failed."
            chmod(path_dest, 0o777)
            logger.debug("download " + str(path_source) + ": True")
        except Exception as e:
            logger.error("Failed to download. " + str(e)) 


    def rm(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = safe_folder_path_str(path)
            path_full = self.__path_expand(path)
            assert (isdir(path_full) or isfile(path_full)), \
                "File/folder not found."
            if isfile(path_full):
                remove(path_full)
            else:
                rmtree(path_full, ignore_errors=True)
            assert ((not isdir(path_full)) and (not isfile(path_full))), \
                "File/folder still exists."
            logger.debug("rm " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to remove the file/folder. " + str(e)) 


    def size(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = safe_folder_path_str(path)
            path_full = self.__path_expand(path)
            assert (isdir(path_full) or isfile(path_full)), \
                "File/folder not found."
            if isfile(path_full):
                output = getsize(path_full)
            else:
                output = get_folder_size(path_full)
            logger.debug("size " + str(path) + ": " + str(output))
        except Exception as e:
            logger.error("Failed to get the size. " + str(e))


    def upload_from_memory(self, variable, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path)
            assert not isfile(path_full), "File already exists."
            assert not isdir(path_full), "Folder already exists."
            pickle.dump(obj=variable, file=open(path_full, "wb"))
            assert isfile(path_full), "File check failed."
            chmod(path_full, 0o777)
            logger.debug("upload_from_memory " + str(path) + ": True")
        except Exception as e:
            logger.error("Failed to upload. " + str(e))  


    def download_to_memory(self, path):
        try:
            assert self.__initialized, "Storage not initialized."
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path)
            assert isfile(path_full), "File not found."
            logger.debug("download_to_memory " + str(path) + ": True")
            return pickle.load(file=open(path_full, "rb"))
        except Exception as e:
            logger.error("Failed to download. " + str(e))  


    def rename(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source_full = self.__path_expand(path_source)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest)
            assert (isfile(path_source_full) or isdir(path_source_full)), \
                "Source file/folder not found."
            assert not (isfile(path_dest_full) or isdir(path_dest_full)), \
                "Destination already exists."
            rename(path_source_full, path_dest_full)
            assert (isfile(path_dest_full) or isdir(path_dest_full)), \
                "Destination check failed."
            assert not (isfile(path_source_full) or isdir(path_source_full)), \
                "Source check failed."
            logger.debug("rename " + str(path_source) + \
                " --> " + str(path_dest))
        except Exception as e:
            logger.error("Failed to rename. " + str(e)) 


    def mv(self, path_source, path_dest):
        try:
            assert self.__initialized, "Storage not initialized."
            path_source_full = self.__path_expand(path_source)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest)
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
            path_source_full = self.__path_expand(path_source)
            path_dest = safe_file_path_str(path_dest)
            path_dest_full = self.__path_expand(path_dest)
            assert (isfile(path_source_full) or isdir(path_source_full)), \
                "Source file/folder not found."
            assert not (isfile(path_dest_full) or isdir(path_dest_full)), \
                "Destination already exists."
            if isfile(path_source_full):
                copyfile(path_source, path_dest)
            else:
                copytree(path_source, path_dest)
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
            path = safe_file_path_str(path)
            path_full = self.__path_expand(path)
            assert isfile(path_full), "File not found."
            with open(path_full, "a") as f:
                f.write(content)
            logger.debug("append " + str(path) + ": " + str(content))
        except Exception as e:
            logger.error("Failed to append. " + str(e)) 