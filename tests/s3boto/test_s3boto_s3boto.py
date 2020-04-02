from os import rmdir, makedirs, remove
from os.path import isdir, isfile, getmtime, getsize
from shutil import rmtree
from pathlib import Path 
from datetime import datetime
from numpy.random import randint
from pytest import raises
from sdaab.s3boto.storage_s3_boto import StorageS3boto
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


def get_s3_obj(dict_config=dict_config):
    assert dict_config["ENV"] == "TESTING"
    root_path = "sdaab-" \
        + datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f-") \
        + str(randint(0, 1000)) \
        + "/"
    s3boto_parent = StorageS3boto(
        host=dict_config["S3"]["HOST"],
        port=dict_config["S3"]["PORT"],
        access_key=dict_config["S3"]["ACCESS_KEY"],
        secret_key=dict_config["S3"]["SECRET_KEY"], 
        bucket=dict_config["S3"]["BUCKET"],
        calling_format=dict_config["S3"]["CALLING_FORMAT"],
        secure=dict_config["S3"]["SECURE"],
        root_path=dict_config["S3"]["ROOT_PATH"]
    )
    s3boto_parent.mkdir(root_path)
    s3boto = StorageS3boto(
        host=dict_config["S3"]["HOST"],
        port=dict_config["S3"]["PORT"],
        access_key=dict_config["S3"]["ACCESS_KEY"],
        secret_key=dict_config["S3"]["SECRET_KEY"], 
        bucket=dict_config["S3"]["BUCKET"],
        calling_format=dict_config["S3"]["CALLING_FORMAT"],
        secure=dict_config["S3"]["SECURE"],
        root_path=dict_config["S3"]["ROOT_PATH"] + root_path
    )
    return s3boto, root_path, s3boto_parent


def remove_s3_folder(s3boto, folder):
    assert s3boto.exists(folder)
    s3boto.rm(folder)
    assert not s3boto.exists(folder)


def test_s3boto_init():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    assert s3boto.initialized()
    assert s3boto.get_type() == "S3boto"
    assert s3boto_parent.initialized()
    assert s3boto_parent.get_type() == "S3boto"
    assert type(root_path) == str
    remove_s3_folder(s3boto_parent, root_path)




def test_s3boto_mkdir_ls_exists():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    s3boto.mkdir("/tmp1")
    assert s3boto.exists("tmp1/")
    assert s3boto.exists("tmp1")
    assert s3boto.exists("/tmp1")
    s3boto.mkdir("tmp2")
    assert s3boto.exists("tmp2/")
    assert s3boto.exists("tmp2")
    assert s3boto.exists("/tmp2")
    s3boto.mkdir("tmp2/tmp3")
    tmp_folders = s3boto.ls()
    assert sorted(tmp_folders) == ["tmp1", "tmp2"]
    s3boto.mkdir("/tmp1/tmp3")
    assert s3boto.exists("tmp1/tmp3/")
    assert s3boto.exists("tmp1/tmp3")
    assert s3boto.exists("/tmp1/tmp3")
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_get_type():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    assert s3boto.get_type() == "S3boto"
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_mkdir_cd_pwd():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    s3boto.mkdir("/level1")
    s3boto.mkdir("level1/level2")
    s3boto.mkdir("/level1/level2/level3")
    assert s3boto.exists("/level1/level2/level3/")
    s3boto.cd("level1")
    assert s3boto.pwd() == "/level1"
    s3boto.cd("level2")
    assert s3boto.pwd() == "/level1/level2"
    s3boto.cd("/level1/level2/level3")
    assert s3boto.pwd() == "/level1/level2/level3"
    s3boto.mkdir("level4")
    assert s3boto.exists("/level1/level2/level3/level4")
    s3boto.cd("..")
    assert s3boto.pwd() == "/level1/level2"
    s3boto.cd("../..")
    assert s3boto.pwd() == "/"
    s3boto.cd("../..")
    assert s3boto.pwd() == "/"
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_cd_ls_exists():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    s3boto.mkdir("level1")
    s3boto.cd("level1")
    s3boto.mkdir("level2")
    s3boto.cd("level2")
    s3boto.mkdir("level3")
    s3boto.cd("level3")   
    assert s3boto.ls("/") == ["level1"]
    assert s3boto.ls("/level1/level2") == ["level3"]
    assert s3boto.exists("/level1/level2/level3")
    s3boto.cd("/")
    assert s3boto.exists("level1/level2/level3")
    assert s3boto.exists("/level1/level2/")
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_upload():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    root_path_local = generate_folder_path()
    assert isdir(root_path_local)
    makedirs(root_path_local / "level1/level2")
    Path(root_path_local / "level1/level2/level2.txt").touch()
    Path(root_path_local / "level1/level1.txt").touch()
    Path(root_path_local / "level0.txt").touch()
    makedirs(root_path_local / "uploaded")
    s3boto.mkdir("uploaded")
    s3boto.upload(root_path_local / "level0.txt", "/uploaded/uploaded_level0.txt")
    s3boto.upload(root_path_local / "level1/level1.txt", "uploaded/uploaded_level1.txt")
    s3boto.cd("uploaded")
    assert s3boto.pwd() == "/uploaded"
    s3boto.upload(root_path_local / "level1/level2/level2.txt", "uploaded_level2.txt")
    assert sorted(s3boto.ls()) \
        == ["uploaded_level0.txt", "uploaded_level1.txt", "uploaded_level2.txt"]
    s3boto.cd("..")
    assert sorted(s3boto.ls("uploaded")) \
        == ["uploaded_level0.txt", "uploaded_level1.txt", "uploaded_level2.txt"]
    remove_folder(root_path_local)
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_upload_download():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    root_path_local = generate_folder_path()
    assert isdir(root_path_local)
    Path(root_path_local / "level0.txt").touch()
    Path(root_path_local / "level1.txt").touch()
    Path(root_path_local / "level2.txt").touch()
    s3boto.upload(root_path_local / "level0.txt", "uploaded_level0.txt")
    s3boto.mkdir("level1")
    s3boto.upload(root_path_local / "level1.txt", "/level1/uploaded_level1.txt")
    s3boto.mkdir("/level1/level2/")
    s3boto.upload(root_path_local / "level2.txt", "/level1/level2/uploaded_level2.txt")
    print(root_path_local)
    s3boto.download("uploaded_level0.txt", root_path_local / "downloaded_level0.txt")
    s3boto.download("/level1/uploaded_level1.txt", \
        root_path_local / "downloaded_level1.txt")
    s3boto.cd("level1")
    s3boto.download("level2/uploaded_level2.txt", \
        root_path_local / "downloaded_level2.txt")
    assert isfile(root_path_local / "downloaded_level0.txt")
    assert isfile(root_path_local / "downloaded_level1.txt")
    assert isfile(root_path_local / "downloaded_level2.txt")
    remove_folder(root_path_local)
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_size_rm():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    root_path_local = generate_folder_path()
    s3boto.mkdir("folder")
    assert s3boto.size("folder") == 0
    with open(root_path_local / "text.txt", "a") as f:
        f.write("ciao")
    s3boto.upload(root_path_local / "text.txt", "folder/text.txt")
    assert s3boto.size("/folder/text.txt") \
        == getsize(root_path_local / "text.txt")
    remove_folder(root_path_local)
    remove_s3_folder(s3boto_parent, root_path)


'''
def test_s3boto_upload_download_memory():

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

    s.upload_from_memory(my_variable, "/level1/level2/v10")
    assert not isfile(root_path / "level1/level2/v10")
    v10 = s.download_to_memory("level1/level2/v10")

    assert my_variable == v1
    assert v1 == v2
    assert v2 == v3
    assert v3 == v4
    assert v4 == v5
    assert v5 == v6
    assert v10 is None

    remove_folder(root_path)


def test_s3boto_rename():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    makedirs(root_path / "level1")
    Path(root_path / "name0").touch()
    Path(root_path / "level1/name1").touch()
    s.rename("name0", "level1/name0")
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


def test_s3boto_mv():

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

    s.mv("file0", "folder1/file0")
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


def test_s3boto_cp():

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

    s.cp("file0", "folder1/file0")
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

    s.cp("file0", "/folder1/file0")
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


def test_s3boto_append():

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
    
    s.append("/folder/file_not_found", "ciao")
    assert not isfile(root_path / "folder/file_not_found")

    remove_folder(root_path)


def test_s3boto_tmp():

    root_path = generate_folder_path()
    assert isdir(root_path)
    s = StorageDisk(root_path=root_path)
    assert s.initialized()

    # Do your stuff

    remove_folder(root_path)

'''