from os import rmdir, makedirs, remove
from os.path import isdir, isfile, getmtime, getsize
from shutil import rmtree
from pathlib import Path 
from datetime import datetime
from numpy.random import randint
from pytest import raises
from sdaab.s3boto.storage_s3_boto import StorageS3boto
from sdaab.s3bdl.storage_s3_bdl import StorageS3BDL
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
    root_path = "/sdaab-" \
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
        root_path="/testing/"
    )
    s3boto_parent.mkdir(root_path)
    s3bdl = StorageS3BDL(
        url=dict_config["S3BDL"]["URL"], 
        secret_key="testing", 
        root_path=root_path)
    return s3bdl, root_path, s3boto_parent


def remove_s3_folder(s3boto, folder):
    assert s3boto.exists(folder)
    s3boto.rm(folder)
    assert not s3boto.exists(folder)


def test_s3bdl_init():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    assert s3bdl.initialized()
    assert s3bdl.get_type() == "S3BDL"
    assert s3boto_parent.initialized()
    assert s3boto_parent.get_type() == "S3boto"
    assert type(root_path) == str
    remove_s3_folder(s3boto_parent, root_path)




def test_s3bdl_mkdir_ls_exists():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    s3bdl.mkdir("/tmp1")
    assert s3bdl.exists("tmp1/")
    assert s3bdl.exists("tmp1")
    assert s3bdl.exists("/tmp1")
    s3bdl.mkdir("tmp2")
    assert s3bdl.exists("tmp2/")
    assert s3bdl.exists("tmp2")
    assert s3bdl.exists("/tmp2")
    s3bdl.mkdir("tmp2/tmp3")
    tmp_folders = s3bdl.ls()
    assert sorted(tmp_folders) == ["tmp1", "tmp2"]
    s3bdl.mkdir("/tmp1/tmp3")
    assert s3bdl.exists("tmp1/tmp3/")
    assert s3bdl.exists("tmp1/tmp3")
    assert s3bdl.exists("/tmp1/tmp3")
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_get_type():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    assert s3bdl.get_type() == "S3BDL"
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_mkdir_cd_pwd():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    s3bdl.mkdir("/level1")
    s3bdl.mkdir("level1/level2")
    s3bdl.mkdir("/level1/level2/level3")
    assert s3bdl.exists("/level1/level2/level3/")
    s3bdl.cd("level1")
    assert s3bdl.pwd() == "/level1"
    s3bdl.cd("level2")
    assert s3bdl.pwd() == "/level1/level2"
    s3bdl.cd("/level1/level2/level3")
    assert s3bdl.pwd() == "/level1/level2/level3"
    s3bdl.mkdir("level4")
    assert s3bdl.exists("/level1/level2/level3/level4")
    s3bdl.cd("..")
    assert s3bdl.pwd() == "/level1/level2"
    s3bdl.cd("../..")
    assert s3bdl.pwd() == "/"
    try:
        s3bdl.cd("../..")
    except Exception as e:
        print(e)
        r = True
    assert r
    assert s3bdl.pwd() == "/"
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_cd_ls_exists():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    s3bdl.mkdir("level1")
    s3bdl.cd("level1")
    s3bdl.mkdir("level2")
    s3bdl.cd("level2")
    s3bdl.mkdir("level3")
    s3bdl.cd("level3")   
    assert s3bdl.ls("/") == ["level1"]
    assert s3bdl.ls("/level1/level2") == ["level3"]
    assert s3bdl.exists("/level1/level2/level3")
    s3bdl.cd("/")
    assert s3bdl.exists("level1/level2/level3")
    assert s3bdl.exists("/level1/level2/")
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_upload():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    root_path_local = generate_folder_path()
    assert isdir(root_path_local)
    makedirs(root_path_local / "level1/level2")
    Path(root_path_local / "level1/level2/level2.txt").touch()
    Path(root_path_local / "level1/level1.txt").touch()
    Path(root_path_local / "level0.txt").touch()
    makedirs(root_path_local / "uploaded")
    s3bdl.mkdir("uploaded")
    s3bdl.upload(root_path_local / "level0.txt", "/uploaded/uploaded_level0.txt")
    s3bdl.upload(root_path_local / "level1/level1.txt", "uploaded/uploaded_level1.txt")
    s3bdl.cd("uploaded")
    assert s3bdl.pwd() == "/uploaded"
    s3bdl.upload(root_path_local / "level1/level2/level2.txt", "uploaded_level2.txt")
    assert sorted(s3bdl.ls()) \
        == ["uploaded_level0.txt", "uploaded_level1.txt", "uploaded_level2.txt"]
    s3bdl.cd("..")
    assert sorted(s3bdl.ls("uploaded")) \
        == ["uploaded_level0.txt", "uploaded_level1.txt", "uploaded_level2.txt"]
    remove_folder(root_path_local)
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_upload_download():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    root_path_local = generate_folder_path()
    assert isdir(root_path_local)
    Path(root_path_local / "level0.txt").touch()
    Path(root_path_local / "level1.txt").touch()
    Path(root_path_local / "level2.txt").touch()
    with open(root_path_local / "level0.txt", "w") as f:
        f.write('hello')
    s3bdl.upload(root_path_local / "level0.txt", "uploaded_level0.txt")
    s3bdl.mkdir("level1")
    s3bdl.upload(root_path_local / "level1.txt", "/level1/uploaded_level1.txt")
    s3bdl.mkdir("/level1/level2/")
    s3bdl.upload(root_path_local / "level2.txt", "/level1/level2/uploaded_level2.txt")
    s3bdl.download("uploaded_level0.txt", root_path_local / "downloaded_level0.txt")
    s3bdl.download("/level1/uploaded_level1.txt", \
        root_path_local / "downloaded_level1.txt")
    s3bdl.cd("level1")
    s3bdl.download("level2/uploaded_level2.txt", \
        root_path_local / "downloaded_level2.txt")
    assert isfile(root_path_local / "downloaded_level0.txt")
    assert isfile(root_path_local / "downloaded_level1.txt")
    assert isfile(root_path_local / "downloaded_level2.txt")
    with open(root_path_local / "downloaded_level0.txt", "r") as f:
        assert f.read() == "hello"
    remove_folder(root_path_local)
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_size_rm():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    root_path_local = generate_folder_path()
    s3bdl.mkdir("folder")
    assert s3bdl.size("folder") == 0
    with open(root_path_local / "text.txt", "a") as f:
        f.write("ciao")
    s3bdl.upload(root_path_local / "text.txt", "folder/text.txt")
    assert s3bdl.size("/folder/text.txt") \
        == getsize(root_path_local / "text.txt")
    s3bdl.rm("folder/text.txt")
    assert not s3bdl.exists("folder/text.txt")
    with open(root_path_local / "text_new.txt", "a") as f:
        f.write("buongiorno")
    s3bdl.upload(root_path_local / "text_new.txt", "folder/text_new.txt")
    assert s3bdl.size("/folder/text_new.txt") \
        == getsize(root_path_local / "text_new.txt")
    s3bdl.cd("folder")
    assert s3bdl.exists("text_new.txt")
    s3bdl.rm("/folder/text_new.txt")    
    assert not s3bdl.exists("text_new.txt")
    s3bdl.cd("..")
    s3bdl.rm("folder")
    assert not s3bdl.exists("folder")
    remove_folder(root_path_local)
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_upload_download_memory():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    my_variable = 1102
    s3bdl.upload_from_memory(my_variable, "v1")
    s3bdl.upload_from_memory(my_variable, "/v2")
    s3bdl.mkdir("level1")
    s3bdl.upload_from_memory(my_variable, "level1/v3")
    s3bdl.upload_from_memory(my_variable, "/level1/v4")
    s3bdl.cd("level1")
    s3bdl.upload_from_memory(my_variable, "v5")
    s3bdl.upload_from_memory(my_variable, "v6")
    v1 = s3bdl.download_to_memory("/v1")
    v2 = s3bdl.download_to_memory("../v2")
    v3 = s3bdl.download_to_memory("/level1/v3")
    v4 = s3bdl.download_to_memory("v4")
    s3bdl.cd("/")
    v5 = s3bdl.download_to_memory("level1/v5")
    v6 = s3bdl.download_to_memory("/level1/v6")
    assert my_variable == v1
    assert v1 == v2
    assert v2 == v3
    assert v3 == v4
    assert v4 == v5
    assert v5 == v6
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_rename():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    s3bdl.mkdir("folder1")
    s3bdl.mkdir("/folder1/folder2")
    s3bdl.upload_from_memory("ciao", "ciao")
    s3bdl.upload_from_memory("ciao", "folder1/ciao1")
    s3bdl.upload_from_memory("ciao", "folder1/folder2/ciao2")
    s3bdl.rename("ciao", "ciao_renamed")
    assert s3bdl.exists("ciao_renamed")
    assert not s3bdl.exists("ciao")
    s3bdl.rename("folder1", "folder1_renamed")
    assert s3bdl.exists("folder1_renamed")
    assert not s3bdl.exists("folder1")
    s3bdl.rename("/folder1_renamed/folder2", "/folder1_renamed/folder2_renamed")
    assert s3bdl.exists("/folder1_renamed/folder2_renamed/ciao2")
    assert not s3bdl.exists("/folder1_renamed/folder2")
    s3bdl.rename("/folder1_renamed/folder2_renamed/ciao2", "/folder1_renamed/folder2_renamed/ciao2_renamed")
    assert s3bdl.exists("/folder1_renamed/folder2_renamed/ciao2_renamed")
    assert not s3bdl.exists("/folder1_renamed/folder2_renamed/ciao2")
    s3bdl.cd("folder1_renamed")
    s3bdl.rename("folder2_renamed", "folder2_renamed_again")
    assert s3bdl.exists("folder2_renamed_again/ciao2_renamed")
    remove_s3_folder(s3boto_parent, root_path)



def test_s3bdl_mv():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    s3bdl.mkdir("folder1")
    s3bdl.mkdir("/folder1/folder2")
    s3bdl.upload_from_memory("ciao", "ciao")
    s3bdl.upload_from_memory("ciao", "folder1/ciao1")
    s3bdl.upload_from_memory("ciao", "folder1/folder2/ciao2")
    s3bdl.mv("ciao", "ciao_moved")
    assert s3bdl.exists("ciao_moved")
    assert not s3bdl.exists("ciao")
    s3bdl.mv("folder1", "folder1_moved")
    assert s3bdl.exists("folder1_moved")
    assert not s3bdl.exists("folder1")
    s3bdl.mv("/folder1_moved/folder2", "/folder1_moved/folder2_moved")
    assert s3bdl.exists("/folder1_moved/folder2_moved/ciao2")
    assert not s3bdl.exists("/folder1_moved/folder2")
    s3bdl.mv("/folder1_moved/folder2_moved/ciao2", "/folder1_moved/folder2_moved/ciao2_moved")
    assert s3bdl.exists("/folder1_moved/folder2_moved/ciao2_moved")
    assert not s3bdl.exists("/folder1_moved/folder2_moved/ciao2")
    s3bdl.cd("folder1_moved")
    s3bdl.mv("folder2_moved", "folder2_moved_again")
    assert s3bdl.exists("folder2_moved_again/ciao2_moved")
    remove_s3_folder(s3boto_parent, root_path)


def test_s3bdl_cp():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    s3bdl.mkdir("folder1")
    s3bdl.mkdir("/folder1/folder2")
    s3bdl.upload_from_memory("ciao", "ciao")
    s3bdl.upload_from_memory("ciao", "folder1/ciao1")
    s3bdl.upload_from_memory("ciao", "folder1/folder2/ciao2")
    s3bdl.cp("ciao", "ciao_copied")
    assert s3bdl.exists("ciao_copied")
    assert s3bdl.exists("ciao")
    s3bdl.cp("folder1", "folder1_copied")
    assert s3bdl.exists("folder1_copied")
    assert s3bdl.exists("folder1")
    s3bdl.cp("/folder1_copied/folder2", "/folder1_copied/folder2_copied")
    assert s3bdl.exists("/folder1_copied/folder2_copied/ciao2")
    assert s3bdl.exists("/folder1_copied/folder2")
    assert s3bdl.exists("folder1/folder2")
    s3bdl.cp("/folder1_copied/folder2_copied/ciao2", "/folder1_copied/folder2_copied/ciao2_copied")
    assert s3bdl.exists("/folder1_copied/folder2_copied/ciao2_copied")
    assert s3bdl.exists("folder1_copied/folder2_copied/ciao2")
    s3bdl.cd("folder1_copied")
    s3bdl.cp("folder2_copied", "folder2_copied_again")
    assert s3bdl.exists("folder2_copied_again/ciao2_copied")
    remove_s3_folder(s3boto_parent, root_path)


'''
def test_s3bdl_append():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    s3bdl.upload_from_memory("ciao", "c")
    s3bdl.append("c", "ciao")
    s3bdl.append("c", "ciao")
    assert s3bdl.download_to_memory("c") == "ciaociaociao"
    s3bdl.mkdir("folder")
    s3bdl.cd("folder")
    s3bdl.upload_from_memory("ciao", "c")
    s3bdl.append("c", "ciao")
    s3bdl.append("/folder/c", "come")
    s3bdl.append("c", "ciao")
    s3bdl.cd("/")
    assert s3bdl.download_to_memory("folder/c") == "ciaociaocomeciao"
    remove_s3_folder(s3boto_parent, root_path)
'''

def test_s3bdl_tmp():
    s3bdl, root_path, s3boto_parent = get_s3_obj()
    # Do your stuff
    remove_s3_folder(s3boto_parent, root_path)