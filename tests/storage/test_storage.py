import pytest
from sdaab.storage.storage import Storage


def test_storage():

    with pytest.raises(Exception):
        s = Storage()

    class MyStorageClass(Storage):

        def __init__(self):
            super.__init__()
            pass

        def get_type(self):
            super.get_type()
            pass


        def cd(self):
            super.cd()
            pass


        def pwd(self):
            super.pwd()
            pass


        def ls(self):
            super.ls()
            pass


        def exists(self):
            super.exists()
            pass


        def mkdir(self):
            super.mkdir()
            pass


        def upload(self):
            super.upload()
            pass


        def download(self):
            super.download()
            pass


        def rm(self):
            super.rm()
            pass


        def size(self):
            super.size()
            pass


        def upload_from_memory(self):
            super.upload_from_memory()
            pass


        def download_to_memory(self):
            super.download_to_memory()
            pass


        def rename(self):
            super.rename()
            pass


        def mv(self):
            super.mv()
            pass


        def cp(self):
            super.cp()
            pass


        def append(self):
            super.append()
            pass



    with pytest.raises(Exception):
        s = MyStorageClass()