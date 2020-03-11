from abc import ABC, abstractmethod


class Storage(ABC):


    @abstractmethod
    def __init__(self):
        pass


    @abstractmethod
    def get_type(self):
        pass


    @abstractmethod
    def cd(self):
        pass


    @abstractmethod
    def pwd(self):
        pass


    @abstractmethod
    def ls(self):
        pass


    @abstractmethod
    def exists(self):
        pass


    @abstractmethod
    def mkdir(self):
        pass


    @abstractmethod
    def upload(self):
        pass


    @abstractmethod
    def download(self):
        pass


    @abstractmethod
    def rm(self):
        pass


    @abstractmethod
    def size(self):
        pass


    @abstractmethod
    def upload_from_memory(self):
        pass


    @abstractmethod
    def download_to_memory(self):
        pass


    @abstractmethod
    def rename(self):
        pass


    @abstractmethod
    def mv(self):
        pass


    @abstractmethod
    def cp(self):
        pass


    @abstractmethod
    def append(self):
        pass