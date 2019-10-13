import os


def dir_create_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
