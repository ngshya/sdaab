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
    try:
        s3boto.cd("../..")
    except Exception as e:
        print(e)
        r = True
    assert r
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
    with open(root_path_local / "level0.txt", "w") as f:
        f.write('hello')
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
    with open(root_path_local / "downloaded_level0.txt", "r") as f:
        assert f.read() == "hello"
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
    s3boto.rm("folder/text.txt")
    assert not s3boto.exists("folder/text.txt")
    with open(root_path_local / "text_new.txt", "a") as f:
        f.write("buongiorno")
    s3boto.upload(root_path_local / "text_new.txt", "folder/text_new.txt")
    assert s3boto.size("/folder/text_new.txt") \
        == getsize(root_path_local / "text_new.txt")
    s3boto.cd("folder")
    assert s3boto.exists("text_new.txt")
    s3boto.rm("/folder/text_new.txt")    
    assert not s3boto.exists("text_new.txt")
    s3boto.cd("..")
    s3boto.rm("folder")
    assert not s3boto.exists("folder")
    remove_folder(root_path_local)
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_upload_download_memory():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    my_variable = 1102
    s3boto.upload_from_memory(my_variable, "v1")
    s3boto.upload_from_memory(my_variable, "/v2")
    s3boto.mkdir("level1")
    s3boto.upload_from_memory(my_variable, "level1/v3")
    s3boto.upload_from_memory(my_variable, "/level1/v4")
    s3boto.cd("level1")
    s3boto.upload_from_memory(my_variable, "v5")
    s3boto.upload_from_memory(my_variable, "v6")
    v1 = s3boto.download_to_memory("/v1")
    v2 = s3boto.download_to_memory("../v2")
    v3 = s3boto.download_to_memory("/level1/v3")
    v4 = s3boto.download_to_memory("v4")
    s3boto.cd("/")
    v5 = s3boto.download_to_memory("level1/v5")
    v6 = s3boto.download_to_memory("/level1/v6")
    assert my_variable == v1
    assert v1 == v2
    assert v2 == v3
    assert v3 == v4
    assert v4 == v5
    assert v5 == v6
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_rename():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    s3boto.mkdir("folder1")
    s3boto.mkdir("/folder1/folder2")
    s3boto.upload_from_memory("ciao", "ciao")
    s3boto.upload_from_memory("ciao", "folder1/ciao1")
    s3boto.upload_from_memory("ciao", "folder1/folder2/ciao2")
    s3boto.rename("ciao", "ciao_renamed")
    assert s3boto.exists("ciao_renamed")
    assert not s3boto.exists("ciao")
    s3boto.rename("folder1", "folder1_renamed")
    assert s3boto.exists("folder1_renamed")
    assert not s3boto.exists("folder1")
    s3boto.rename("/folder1_renamed/folder2", "/folder1_renamed/folder2_renamed")
    assert s3boto.exists("/folder1_renamed/folder2_renamed/ciao2")
    assert not s3boto.exists("/folder1_renamed/folder2")
    s3boto.rename("/folder1_renamed/folder2_renamed/ciao2", "/folder1_renamed/folder2_renamed/ciao2_renamed")
    assert s3boto.exists("/folder1_renamed/folder2_renamed/ciao2_renamed")
    assert not s3boto.exists("/folder1_renamed/folder2_renamed/ciao2")
    s3boto.cd("folder1_renamed")
    s3boto.rename("folder2_renamed", "folder2_renamed_again")
    assert s3boto.exists("folder2_renamed_again/ciao2_renamed")
    remove_s3_folder(s3boto_parent, root_path)



def test_s3boto_mv():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    s3boto.mkdir("folder1")
    s3boto.mkdir("/folder1/folder2")
    s3boto.upload_from_memory("ciao", "ciao")
    s3boto.upload_from_memory("ciao", "folder1/ciao1")
    s3boto.upload_from_memory("ciao", "folder1/folder2/ciao2")
    s3boto.mv("ciao", "ciao_moved")
    assert s3boto.exists("ciao_moved")
    assert not s3boto.exists("ciao")
    s3boto.mv("folder1", "folder1_moved")
    assert s3boto.exists("folder1_moved")
    assert not s3boto.exists("folder1")
    s3boto.mv("/folder1_moved/folder2", "/folder1_moved/folder2_moved")
    assert s3boto.exists("/folder1_moved/folder2_moved/ciao2")
    assert not s3boto.exists("/folder1_moved/folder2")
    s3boto.mv("/folder1_moved/folder2_moved/ciao2", "/folder1_moved/folder2_moved/ciao2_moved")
    assert s3boto.exists("/folder1_moved/folder2_moved/ciao2_moved")
    assert not s3boto.exists("/folder1_moved/folder2_moved/ciao2")
    s3boto.cd("folder1_moved")
    s3boto.mv("folder2_moved", "folder2_moved_again")
    assert s3boto.exists("folder2_moved_again/ciao2_moved")
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_cp():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    s3boto.mkdir("folder1")
    s3boto.mkdir("/folder1/folder2")
    s3boto.upload_from_memory("ciao", "ciao")
    s3boto.upload_from_memory("ciao", "folder1/ciao1")
    s3boto.upload_from_memory("ciao", "folder1/folder2/ciao2")
    s3boto.cp("ciao", "ciao_copied")
    assert s3boto.exists("ciao_copied")
    assert s3boto.exists("ciao")
    s3boto.cp("folder1", "folder1_copied")
    assert s3boto.exists("folder1_copied")
    assert s3boto.exists("folder1")
    s3boto.cp("/folder1_copied/folder2", "/folder1_copied/folder2_copied")
    assert s3boto.exists("/folder1_copied/folder2_copied/ciao2")
    assert s3boto.exists("/folder1_copied/folder2")
    assert s3boto.exists("folder1/folder2")
    s3boto.cp("/folder1_copied/folder2_copied/ciao2", "/folder1_copied/folder2_copied/ciao2_copied")
    assert s3boto.exists("/folder1_copied/folder2_copied/ciao2_copied")
    assert s3boto.exists("folder1_copied/folder2_copied/ciao2")
    s3boto.cd("folder1_copied")
    s3boto.cp("folder2_copied", "folder2_copied_again")
    assert s3boto.exists("folder2_copied_again/ciao2_copied")
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_append():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    s3boto.upload_from_memory("ciao", "c")
    s3boto.append("c", "ciao")
    s3boto.append("c", "ciao")
    assert s3boto.download_to_memory("c") == "ciaociaociao"
    s3boto.mkdir("folder")
    s3boto.cd("folder")
    s3boto.upload_from_memory("ciao", "c")
    s3boto.append("c", "ciao")
    s3boto.append("/folder/c", "come")
    s3boto.append("c", "ciao")
    s3boto.cd("/")
    assert s3boto.download_to_memory("folder/c") == "ciaociaocomeciao"
    remove_s3_folder(s3boto_parent, root_path)


def test_s3boto_tmp():
    s3boto, root_path, s3boto_parent = get_s3_obj()
    # Do your stuff
    remove_s3_folder(s3boto_parent, root_path)