from os import rmdir, makedirs, remove
from os.path import isdir, isfile, getmtime, getsize
from shutil import rmtree
from pathlib import Path 
from datetime import datetime
from numpy.random import randint
from pytest import raises
from sdaab.disk.storage_disk import StorageDisk
from sdaab.utils.get_config import dict_config


def generate_folder_path(dict_config=dict_config):
    assert dict_config["ENV"] == "TESTING"
    root_path = Path(dict_config["DISK"]["ROOT_PATH"] + \
        "/sdaab-" + datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f-") + \
        str(randint(0, 1000)))
    makedirs(root_path)
    assert isdir(root_path)
    return root_path


def remove_folder(path):
    assert isdir(path)
    rmtree(path)


def test_storage_disk_init():

    root_path = generate_folder_path()

    try:
        s = StorageDisk(root_path="/this/folder/does/not/exist")
    except Exception as e:
        print(e)
        r = True
    assert r

    s = StorageDisk(root_path=root_path)
    assert s.initialized()
    assert s.get_type() == "DISK"
    
    try:
        s = StorageDisk(root_path="this/folder/does/not/exist")
    except Exception as e:
        print(e)
        r = True
    assert r

    remove_folder(root_path)



def test_storage_disk_mkdir():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()
    
    s.mkdir("/tmp1")
    assert isdir(root_path / "tmp1")
    
    s.mkdir("tmp2")
    assert isdir(root_path / "tmp2")
    
    tmp_folders = [x.name for x in root_path.iterdir()]
    assert sorted(tmp_folders) == ["tmp1", "tmp2"]
    
    tmp_mtime = getmtime(root_path / "tmp2")
    try:
        s.mkdir("tmp2")
    except Exception as e:
        print(e)
    assert tmp_mtime == getmtime(root_path / "tmp2")

    s.mkdir("tmp/tmp3")
    assert isdir(root_path / "tmp/tmp3")
    assert isdir(root_path / "tmp")

    remove_folder(root_path)


def test_storage_disk_get_type():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    assert s.get_type() == "DISK"

    try:
        s = StorageDisk(root_path="this/folder/does/not/exist")
    except Exception as e:
        print(e)
        r = True
    assert r

    remove_folder(root_path)


def test_storage_disk_mkdir_cd_pwd():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    s.mkdir("level1/level2")
    s.mkdir("/level1/level2/level3")
    assert isdir(root_path / "level1/level2/level3")

    s.cd("level1")
    assert s.pwd() == "/level1"
    s.cd("level2")
    assert s.pwd() == "/level1/level2"
    s.cd("/level1/level2/level3")
    assert s.pwd() == "/level1/level2/level3"
    s.mkdir("level4")
    assert isdir(root_path / "level1/level2/level3/level4")
    s.cd("..")
    assert s.pwd() == "/level1/level2"
    s.cd("../..")
    assert s.pwd() == "/"
    try:
        s.cd("../..")
    except Exception as e:
        print(e)
    assert s.pwd() == "/"
    s.mkdir("tmp")
    isdir(root_path / "tmp")

    remove_folder(root_path)


def test_storage_disk_cd_ls_exists():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "level1/level2")
    Path(root_path / "level1/level2/level2.txt").touch()
    Path(root_path / "level1/level1.txt").touch()
    assert s.ls() == ["level1"]
    assert sorted(s.ls("level1")) == ["level1.txt", "level2"]
    s.cd("level1")
    assert sorted(s.ls()) == ["level1.txt", "level2"]
    assert s.ls("/level1/level2") == ["level2.txt"]
    assert s.exists("level1.txt")
    assert s.exists("/level1/level2/level2.txt")
    makedirs(root_path / "level1/level2/level3")
    assert s.exists("/level1/level2/level3")
    assert s.ls("/level1/level2/level3") == []
    try: 
        s.ls("/folder/that/does/not/exist")
    except Exception as e:
        print(e)
        r = None
    assert r is None
    s.cd("/level1/level2/level3")
    assert not s.exists("level4.txt")
    assert not s.exists("level4")
    assert s.exists("/level1/level2/level3")

    remove_folder(root_path)


def test_storage_disk_upload():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "level1/level2")
    Path(root_path / "level1/level2/level2.txt").touch()
    Path(root_path / "level1/level1.txt").touch()
    Path(root_path / "level0.txt").touch()
    makedirs(root_path / "uploaded")

    s.upload(root_path / "level0.txt", "/uploaded/uploaded_level0.txt")
    s.upload(root_path / "level1/level1.txt", "/uploaded/uploaded_level1.txt")
    s.cd("uploaded")
    assert s.pwd() == "/uploaded"
    s.upload(root_path / "level1/level2/level2.txt", "uploaded_level2.txt")

    assert isfile(root_path / "uploaded/uploaded_level0.txt")
    assert isfile(root_path / "uploaded/uploaded_level1.txt")
    assert isfile(root_path / "uploaded/uploaded_level2.txt")
    assert getsize(root_path / "uploaded/uploaded_level0.txt") \
        == getsize(root_path / "level0.txt")
    assert getsize(root_path / "uploaded/uploaded_level1.txt") \
        == getsize(root_path / "level1/level1.txt")
    assert getsize(root_path / "uploaded/uploaded_level2.txt") \
        == getsize(root_path / "level1/level2/level2.txt")

    s.cd("/level1")
    try:
        s.upload("level2", "/uploaded/uploaded_level2")
    except Exception as e:
        print(e)
    assert not isdir(root_path / "uploaded/uploaded_level2")
    assert not isfile(root_path / "uploaded/uploaded_level2")

    remove_folder(root_path)


def test_storage_disk_download():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "level1/level2")
    Path(root_path / "level1/level2/level2.txt").touch()
    Path(root_path / "level1/level1.txt").touch()
    Path(root_path / "level0.txt").touch()
    makedirs(root_path / "downloaded")

    s.download("level0.txt", root_path / "downloaded/downloaded_level0.txt")
    s.download("/level1/level1.txt", \
        root_path / "downloaded/downloaded_level1.txt")
    s.cd("level1")
    s.download("level2/level2.txt", \
        root_path / "downloaded/downloaded_level2.txt")

    assert isfile(root_path / "downloaded/downloaded_level0.txt")
    assert isfile(root_path / "downloaded/downloaded_level1.txt")
    assert isfile(root_path / "downloaded/downloaded_level2.txt")
    assert getsize(root_path / "downloaded/downloaded_level0.txt") \
        == getsize(root_path / "level0.txt")
    assert getsize(root_path / "downloaded/downloaded_level1.txt") \
        == getsize(root_path / "level1/level1.txt")
    assert getsize(root_path / "downloaded/downloaded_level2.txt") \
        == getsize(root_path / "level1/level2/level2.txt")

    s.cd("/level1")
    try:
        s.download("level2", "/downloaded/downloaded_level2")
    except Exception as e:
        print(e)
    assert not isdir(root_path / "downloaded/downloaded_level2")
    assert not isfile(root_path / "downloaded/downloaded_level2")

    remove_folder(root_path)


def test_storage_disk_size_rm():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "folder")
    assert s.size("folder") == 0

    with open(root_path / "folder/text.txt", "a") as f:
        f.write("ciao")
    assert s.size("folder") == getsize(root_path / "folder/text.txt")
    assert s.size("/folder/text.txt") == getsize(root_path / "folder/text.txt")

    with open(root_path / "folder/text_2.txt", "a") as f:
        f.write("buongiorno")
    assert s.size("folder") == \
        getsize(root_path / "folder/text.txt") + \
        getsize(root_path / "folder/text_2.txt")

    s.rm("/folder/text.txt")

    assert not isfile(root_path / "folder/text.txt")
    
    s.rm("folder/")

    assert not isdir(root_path / "folder")

    remove_folder(root_path)


def test_storage_disk_upload_download_memory():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    my_variable = 1102

    s.upload_from_memory(my_variable, "v1")
    s.upload_from_memory(my_variable, "/v2")
    makedirs(root_path / "level1")
    s.upload_from_memory(my_variable, "level1/v3")
    s.upload_from_memory(my_variable, "/level1/v4")
    s.cd("level1")
    s.upload_from_memory(my_variable, "v5")
    s.upload_from_memory(my_variable, "v6")

    v1 = s.download_to_memory("/v1")
    v2 = s.download_to_memory("../v2")
    v3 = s.download_to_memory("/level1/v3")
    v4 = s.download_to_memory("v4")
    s.cd("/")
    v5 = s.download_to_memory("level1/v5")
    v6 = s.download_to_memory("/level1/v6")

    try:
        s.upload_from_memory(my_variable, "/level1/level2/v10")
    except Exception as e:
        print(e)
    assert not isfile(root_path / "level1/level2/v10")
    try:
        v10 = s.download_to_memory("level1/level2/v10")
    except Exception as e:
        v10 = None

    assert my_variable == v1
    assert v1 == v2
    assert v2 == v3
    assert v3 == v4
    assert v4 == v5
    assert v5 == v6
    assert v10 is None

    remove_folder(root_path)


def test_storage_disk_rename():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "level1")
    Path(root_path / "name0").touch()
    Path(root_path / "level1/name1").touch()
    try:
        s.rename("name0", "level1/name0")
    except Exception as e:
        print(e)
    assert isfile(root_path / "name0")
    s.rename("name0", "new_name0")
    assert isfile(root_path / "new_name0")
    assert not isfile(root_path / "name0")
    s.rename("/level1", "/new_level1")
    assert isdir(root_path / "new_level1")
    assert not isdir(root_path / "level1")
    s.rename("/new_level1/name1", "new_level1/new_name1")
    assert isfile(root_path / "new_level1/new_name1")
    assert not isfile(root_path / "new_level1/name1")   

    remove_folder(root_path)


def test_storage_disk_mv():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "folder1")
    makedirs(root_path / "folder2")
    Path(root_path / "file0").touch()
    Path(root_path / "folder1/file0").touch()

    with open(root_path / "file0", "a") as f:
        f.write("ciao")

    s.mv("file0", "folder2/file0")
    assert isfile(root_path / "folder2/file0")
    s.mv("folder2/file0", "/file0")
    assert isfile(root_path / "file0")

    try:
        s.mv("file0", "folder1/file0")
    except Exception as e:
        print(e)
    assert isfile(root_path/ "file0")
    assert getsize(root_path / "folder1/file0") != getsize(root_path / "file0")

    s.mv("file0", "/folder2/file0")
    assert isfile(root_path / "folder2/file0")
    s.mv("/folder2/file0", "file0")
    assert isfile(root_path / "file0")

    s.cd("folder2")
    s.mv("/file0", "file0")
    assert isfile(root_path / "folder2/file0")
    s.mv("file0", "/file0")

    s.cd("/")
    s.mv("file0", "folder2/file0000")
    assert isfile(root_path / "folder2/file0000")
    s.mv("folder2/file0000", "/file0")

    s.mv("folder1", "/folder2/folder1111")
    assert isdir(root_path / "folder2/folder1111")
    assert isfile(root_path / "folder2/folder1111/file0")
    assert not isdir(root_path / "folder1")

    remove_folder(root_path)


def test_storage_disk_cp():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "folder1")
    makedirs(root_path / "folder2")
    Path(root_path / "file0").touch()
    Path(root_path / "folder1/file0").touch()

    with open(root_path / "file0", "a") as f:
        f.write("ciao")

    s.cp("file0", "folder2/file0")
    assert isfile(root_path / "folder2/file0")
    assert isfile(root_path / "file0")
    assert getsize(root_path / "folder2/file0") == getsize(root_path / "file0")
    remove(root_path / "folder2/file0")

    try:
        s.cp("file0", "folder1/file0")
    except Exception as e:
        print(e)
    assert isfile(root_path/ "file0")
    assert getsize(root_path / "folder1/file0") != getsize(root_path / "file0")

    s.cp("file0", "/folder2/file0")
    assert isfile(root_path / "folder2/file0")
    assert isfile(root_path / "file0")
    remove(root_path / "folder2/file0")

    s.cd("folder2")
    s.cp("/file0", "file0")
    assert isfile(root_path / "folder2/file0")
    remove(root_path / "folder2/file0")

    s.cd("/")
    s.cp("file0", "folder2/file0000")
    assert isfile(root_path / "folder2/file0000")
    remove(root_path / "folder2/file0000")

    try:
        s.cp("file0", "/folder1/file0")
    except Exception as e:
        print(e)
    assert getsize(root_path / "file0") != \
        getsize(root_path / "folder1/file0")
    remove(root_path / "folder1/file0")
    s.cp("file0", "/folder1/file0")
    assert getsize(root_path / "file0") == \
        getsize(root_path / "folder1/file0")
    s.cp("folder1", "/folder2/folder1111")
    assert isdir(root_path / "folder2/folder1111")
    assert isfile(root_path / "folder2/folder1111/file0")
    assert isdir(root_path / "folder1")
    assert getsize(root_path / "folder2/folder1111/file0") == \
        getsize(root_path / "folder1/file0")

    remove_folder(root_path)


def test_storage_disk_append():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "folder")
    Path(root_path / "folder/file.txt").touch()

    s.append("/folder/file.txt", "ciao")
    s.append("folder/file.txt", "ciao")
    s.cd("folder")
    s.append("file.txt", "ciao")

    with open(root_path / "folder/file.txt", "r") as f:
        assert f.read() == "ciaociaociao"
    
    try:
        s.append("/folder/file_not_found", "ciao")
    except Exception as e:
        print(e)
    assert not isfile(root_path / "folder/file_not_found")

    remove_folder(root_path)


def test_storage_disk_tmp():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    # Do your stuff

    remove_folder(root_path)