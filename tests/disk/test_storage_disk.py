from os.path import isdir
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
    s = StorageDisk(root_path=root_path)
    s.mkdir("sdaab_folder_1234")
    assert isdir(Path(root_path) / "sdaab_folder_1234")