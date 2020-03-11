from os import rmdir
from os.path import isdir, getmtime
from shutil import rmtree
from pathlib import Path 
from pytest import raises
from sdaab.disk.storage_disk import StorageDisk
from sdaab.utils.get_config import dict_config


def test_storage_disk_init():

    assert dict_config["ENV"] == "TESTING"
    
    s = StorageDisk(root_path="/this/folder/does/not/exist")
    assert not s.initialized()

    s = StorageDisk(root_path=dict_config["STORAGE_DISK"]["ROOT_PATH"])
    assert s.initialized()
    assert s.get_type() == "DISK"
    
    s = StorageDisk(root_path="this/folder/does/not/exist")
    assert not s.initialized()


def test_storage_disk_mkdir():

    assert dict_config["ENV"] == "TESTING"

    root_path = dict_config["STORAGE_DISK"]["ROOT_PATH"]
    path_4_test = Path(root_path) / "sdaab_folder_1234"
    s = StorageDisk(root_path=root_path)
    
    if isdir(path_4_test):
        rmtree(path_4_test, ignore_errors=True)
    
    s.mkdir("sdaab_folder_1234")
    assert isdir(path_4_test)
    
    s.mkdir("sdaab_folder_1234/tmp1")
    assert isdir(path_4_test / "tmp1")
    
    s.mkdir("/sdaab_folder_1234/tmp2")
    assert isdir(path_4_test / "tmp2")
    
    tmp_folders = [x.name for x in path_4_test.iterdir()]
    assert sorted(tmp_folders) == ["tmp1", "tmp2"]
    
    tmp_mtime = getmtime(path_4_test / "tmp2")
    s.mkdir("sdaab_folder_1234/tmp2")
    assert tmp_mtime == getmtime(path_4_test / "tmp2")

    s.mkdir("sdaab_folder_1234/tmp/tmp3")
    assert isdir(path_4_test / "tmp/tmp3")

